"""Unit tests for lakehouse data-quality rules."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from realtime_market_stream.orchestration.jobs import run_data_quality_check
from realtime_market_stream.processing.quality import (
    check_freshness,
    check_nulls,
    check_volume_anomalies,
    evaluate_rows,
)


def _tick(
    *,
    symbol: str = "AAPL",
    age_seconds: int = 10,
    price: float = 100.0,
    size: int = 10,
    tick_id: str = "t1",
    drop: str | None = None,
) -> dict[str, object]:
    ts = datetime.now(tz=UTC) - timedelta(seconds=age_seconds)
    row: dict[str, object] = {
        "tick_id": tick_id,
        "symbol": symbol,
        "ts": ts.isoformat(),
        "price": price,
        "size": size,
        "event_type": "trade",
    }
    if drop:
        row.pop(drop, None)
    return row


def test_freshness_stale_when_old() -> None:
    rows = [_tick(age_seconds=10_000)]
    result = check_freshness(rows, max_age_seconds=60)
    assert result.ok is False
    assert result.violations == 1


def test_freshness_ok_when_recent() -> None:
    rows = [_tick(age_seconds=5)]
    result = check_freshness(rows, max_age_seconds=60)
    assert result.ok is True


def test_nulls_detects_missing_price() -> None:
    rows = [_tick(), _tick(tick_id="t2", drop="price")]
    result = check_nulls(rows)
    assert result.ok is False
    assert result.violations == 1
    assert result.samples[0]["missing"] == ["price"]


def test_volume_flags_non_positive_and_iqr() -> None:
    rows = [
        _tick(tick_id="a", size=10),
        _tick(tick_id="b", size=11),
        _tick(tick_id="c", size=12),
        _tick(tick_id="d", size=13),
        _tick(tick_id="e", size=10_000),
        _tick(tick_id="f", size=0),
    ]
    result = check_volume_anomalies(rows, iqr_multiplier=3.0)
    assert result.ok is False
    assert result.violations >= 2


def test_evaluate_rows_and_job_pass_on_clean_sample() -> None:
    rows = [_tick(tick_id=f"t{i}", size=10 + i) for i in range(6)]
    report = evaluate_rows(rows, view="bronze_ticks", max_age_seconds=86_400)
    assert report.ok
    assert {rule.name for rule in report.rules} == {
        "freshness",
        "nulls",
        "volume_anomalies",
    }
    job = run_data_quality_check(rows=rows, view="bronze_ticks")
    assert job.ok
