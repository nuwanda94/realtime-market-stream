"""Lakehouse data-quality rules: freshness, nulls, volume anomalies.

Local-first and broker-free. Rules operate on in-memory row dicts so they
can run in CI, Airflow, or ``make dq`` without Redpanda / Snowflake.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

REQUIRED_TRADE_FIELDS: tuple[str, ...] = (
    "tick_id",
    "symbol",
    "ts",
    "price",
    "size",
)
REQUIRED_OHLCV_FIELDS: tuple[str, ...] = (
    "symbol",
    "window_start",
    "open",
    "high",
    "low",
    "close",
    "volume",
)


@dataclass(frozen=True)
class RuleResult:
    """Outcome of a single DQ rule."""

    name: str
    ok: bool
    rows_scanned: int
    violations: int
    detail: str
    samples: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class QualityReport:
    """Aggregate report for a view."""

    view: str
    rows: int
    ok: bool
    rules: list[RuleResult]

    def as_dict(self) -> dict[str, Any]:
        return {
            "view": self.view,
            "rows": self.rows,
            "ok": self.ok,
            "rules": [rule.as_dict() for rule in self.rules],
        }


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _row_ts(row: dict[str, Any]) -> datetime | None:
    for key in ("ts", "window_start", "window_end", "ingested_at"):
        parsed = _parse_ts(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _volume_of(row: dict[str, Any]) -> float | None:
    for key in ("size", "volume"):
        parsed = _to_float(row.get(key))
        if parsed is not None:
            return parsed
    return None


def check_freshness(
    rows: Sequence[dict[str, Any]],
    *,
    max_age_seconds: int = 86_400,
    now: datetime | None = None,
) -> RuleResult:
    """Fail when the view is empty or the newest timestamp is too old."""

    clock = now or datetime.now(tz=UTC)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=UTC)
    latest: datetime | None = None
    for row in rows:
        ts = _row_ts(row)
        if ts is not None and (latest is None or ts > latest):
            latest = ts
    if not rows or latest is None:
        return RuleResult(
            name="freshness",
            ok=False,
            rows_scanned=len(rows),
            violations=1,
            detail="no timestamped rows",
        )
    age = (clock.astimezone(UTC) - latest).total_seconds()
    stale = age > max_age_seconds
    return RuleResult(
        name="freshness",
        ok=not stale,
        rows_scanned=len(rows),
        violations=1 if stale else 0,
        detail=f"latest={latest.isoformat()} age_seconds={age:.0f} max={max_age_seconds}",
    )


def check_nulls(
    rows: Sequence[dict[str, Any]],
    *,
    required_fields: Sequence[str] | None = None,
    max_samples: int = 5,
) -> RuleResult:
    """Fail when required fields are missing, empty, or NaN."""

    fields = tuple(required_fields) if required_fields else _infer_required(rows)
    violations = 0
    samples: list[dict[str, Any]] = []
    for row in rows:
        missing = [name for name in fields if _is_missing(row.get(name))]
        if missing:
            violations += 1
            if len(samples) < max_samples:
                samples.append({"missing": missing, "symbol": row.get("symbol")})
    return RuleResult(
        name="nulls",
        ok=violations == 0,
        rows_scanned=len(rows),
        violations=violations,
        detail=f"required={list(fields)} missing_rows={violations}",
        samples=samples,
    )


def _infer_required(rows: Sequence[dict[str, Any]]) -> tuple[str, ...]:
    for row in rows:
        event = str(row.get("event_type") or "").lower()
        if event == "ohlcv" or "volume" in row:
            return REQUIRED_OHLCV_FIELDS
        if event == "trade" or "size" in row:
            return REQUIRED_TRADE_FIELDS
    return REQUIRED_TRADE_FIELDS


def check_volume_anomalies(
    rows: Sequence[dict[str, Any]],
    *,
    iqr_multiplier: float = 3.0,
    max_samples: int = 5,
) -> RuleResult:
    """Flag non-positive size/volume and Tukey IQR outliers per symbol."""

    by_symbol: dict[str, list[tuple[dict[str, Any], float]]] = {}
    non_positive = 0
    samples: list[dict[str, Any]] = []
    scanned = 0
    for row in rows:
        volume = _volume_of(row)
        if volume is None:
            continue
        scanned += 1
        if volume <= 0:
            non_positive += 1
            if len(samples) < max_samples:
                samples.append(
                    {
                        "reason": "non_positive",
                        "symbol": row.get("symbol"),
                        "volume": volume,
                    }
                )
            continue
        symbol = str(row.get("symbol") or "_")
        by_symbol.setdefault(symbol, []).append((row, volume))

    outliers = 0
    for symbol, pairs in by_symbol.items():
        values = [vol for _, vol in pairs]
        if len(values) < 4:
            continue
        ordered = sorted(values)
        q1 = _percentile(ordered, 0.25)
        q3 = _percentile(ordered, 0.75)
        iqr = q3 - q1
        if iqr <= 0:
            continue
        high = q3 + iqr_multiplier * iqr
        low = max(0.0, q1 - iqr_multiplier * iqr)
        for _row, vol in pairs:
            if vol < low or vol > high:
                outliers += 1
                if len(samples) < max_samples:
                    samples.append(
                        {
                            "reason": "iqr",
                            "symbol": symbol,
                            "volume": vol,
                            "low": low,
                            "high": high,
                        }
                    )

    violations = non_positive + outliers
    return RuleResult(
        name="volume_anomalies",
        ok=violations == 0,
        rows_scanned=scanned,
        violations=violations,
        detail=(
            f"non_positive={non_positive} iqr_outliers={outliers} "
            f"multiplier={iqr_multiplier}"
        ),
        samples=samples,
    )


def _percentile(ordered: Sequence[float], q: float) -> float:
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    pos = q * (len(ordered) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return ordered[lo]
    weight = pos - lo
    return ordered[lo] * (1.0 - weight) + ordered[hi] * weight


def evaluate_rows(
    rows: Sequence[dict[str, Any]],
    *,
    view: str = "bronze_ticks",
    max_age_seconds: int = 86_400,
    iqr_multiplier: float = 3.0,
    required_fields: Sequence[str] | None = None,
    now: datetime | None = None,
) -> QualityReport:
    """Run freshness, nulls, and volume rules over ``rows``."""

    rules = [
        check_freshness(rows, max_age_seconds=max_age_seconds, now=now),
        check_nulls(rows, required_fields=required_fields),
        check_volume_anomalies(rows, iqr_multiplier=iqr_multiplier),
    ]
    ok = all(rule.ok for rule in rules)
    return QualityReport(view=view, rows=len(rows), ok=ok, rules=rules)


def mean_volume(rows: Iterable[dict[str, Any]]) -> float:
    """Helper used by tests / dashboards."""

    values = [vol for vol in (_volume_of(row) for row in rows) if vol is not None]
    if not values:
        return 0.0
    return float(statistics.fmean(values))
