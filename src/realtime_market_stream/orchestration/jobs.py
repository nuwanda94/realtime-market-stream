"""Airflow-independent orchestration jobs.

These callables are the bodies of the Airflow DAGs under ``airflow/dags/``.
They stay importable in CI with zero Airflow / Redpanda / MinIO cost.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from realtime_market_stream.config.settings import Settings, get_settings
from realtime_market_stream.ingestion.generator import SyntheticTickGenerator
from realtime_market_stream.processing.bronze import BronzeProcessor
from realtime_market_stream.processing.checkpoint import FileCheckpointStore
from realtime_market_stream.processing.quality import QualityReport, evaluate_rows
from realtime_market_stream.schemas.registry import (
    SCHEMA_VERSION,
    SchemaSubject,
    build_json_schema,
    load_bundled_schema,
)
from realtime_market_stream.schemas.ticks import OhlcvBar, TradeTick
from realtime_market_stream.sinks.delta import build_bronze_sink

logger = logging.getLogger(__name__)


class _NoopPublisher:
    """Swallow DLQ publishes so jobs work without a broker."""

    def send(self, topic: str, key: bytes, value: bytes) -> None:  # noqa: ARG002
        return None

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


@dataclass
class BackfillResult:
    generated: int
    written: int
    dlq: int
    data_root: str


@dataclass
class SchemaCheckResult:
    subjects: list[str]
    drifted: list[str]
    ok: bool


@dataclass
class FreshnessResult:
    view: str
    rows: int
    latest_ts: str
    stale: bool
    max_age_seconds: int


def _event_value(event: TradeTick | OhlcvBar) -> bytes:
    return json.dumps(event.to_kafka_value(), default=str).encode("utf-8")


def run_backfill(
    *,
    count: int = 20,
    data_root: str | Path | None = None,
    settings: Settings | None = None,
    seed: int | None = 42,
) -> BackfillResult:
    """Generate synthetic ticks and land them in Bronze without Kafka."""

    cfg = settings or get_settings()
    root = Path(data_root) if data_root else Path("data") / cfg.minio.bucket
    sink = build_bronze_sink(cfg, local_root=str(root))
    processor = BronzeProcessor(
        settings=cfg,
        sink=sink,
        publisher=_NoopPublisher(),
        consumer=None,
        checkpoint_store=FileCheckpointStore(root / ".checkpoints"),
    )
    generator = SyntheticTickGenerator.from_settings(cfg.generator, seed=seed)
    payloads: list[bytes] = []
    produced = 0
    now = datetime.now(tz=UTC)
    while produced < count:
        for event in generator.next_events(now=now):
            payloads.append(_event_value(event))
            if isinstance(event, TradeTick):
                produced += 1
                if produced >= count:
                    break
    processor.process_batch(payloads)
    processor.close()
    return BackfillResult(
        generated=produced,
        written=processor.stats.written,
        dlq=processor.stats.dlq,
        data_root=str(root),
    )


def run_schema_evolution_check() -> SchemaCheckResult:
    """Compare bundled JSON Schema files to live Pydantic models."""

    drifted: list[str] = []
    subjects = [item.value for item in SchemaSubject]
    for subject in SchemaSubject:
        live = build_json_schema(subject, version=SCHEMA_VERSION)
        bundled = load_bundled_schema(subject)
        live_props = set((live.get("properties") or {}).keys())
        bundled_props = set((bundled.get("properties") or {}).keys())
        live_required = set(live.get("required") or [])
        bundled_required = set(bundled.get("required") or [])
        if live_props != bundled_props or live_required != bundled_required:
            drifted.append(subject.value)
            logger.warning("schema drift on subject %s", subject.value)
    return SchemaCheckResult(subjects=subjects, drifted=drifted, ok=not drifted)


def run_freshness_check(
    *,
    view: str = "bronze_ticks",
    max_age_seconds: int = 86_400,
    data_root: str | Path | None = None,
    settings: Settings | None = None,
    limit: int = 50,
) -> FreshnessResult:
    """Lightweight lakehouse freshness probe used by the DQ DAG."""

    from realtime_market_stream.serving.query import QueryEngine

    cfg = settings or get_settings()
    engine = QueryEngine.from_settings(cfg, local_root=data_root, backend="jsonl")
    rows = engine.scan(view, limit=limit)
    latest = ""
    for row in rows:
        for key in ("ts", "window_start", "ingested_at"):
            value = row.get(key)
            if isinstance(value, str) and value > latest:
                latest = value
    stale = True
    if latest:
        try:
            parsed = datetime.fromisoformat(latest.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            age = (datetime.now(tz=UTC) - parsed.astimezone(UTC)).total_seconds()
            stale = age > max_age_seconds
        except ValueError:
            stale = True
    if not rows:
        stale = True
    return FreshnessResult(
        view=view,
        rows=len(rows),
        latest_ts=latest,
        stale=stale,
        max_age_seconds=max_age_seconds,
    )


def run_data_quality_check(
    *,
    view: str = "bronze_ticks",
    max_age_seconds: int = 86_400,
    iqr_multiplier: float = 3.0,
    data_root: str | Path | None = None,
    settings: Settings | None = None,
    limit: int = 500,
    rows: list[dict[str, Any]] | None = None,
) -> QualityReport:
    """Run freshness + nulls + volume-anomaly rules over a lakehouse view."""

    if rows is None:
        from realtime_market_stream.serving.query import QueryEngine

        cfg = settings or get_settings()
        engine = QueryEngine.from_settings(cfg, local_root=data_root, backend="jsonl")
        rows = engine.scan(view, limit=limit)
    report = evaluate_rows(
        rows,
        view=view,
        max_age_seconds=max_age_seconds,
        iqr_multiplier=iqr_multiplier,
    )
    if not report.ok:
        logger.warning("data quality failed view=%s report=%s", view, report.as_dict())
    return report


def result_as_dict(
    result: BackfillResult | SchemaCheckResult | FreshnessResult | QualityReport,
) -> dict[str, Any]:
    if isinstance(result, QualityReport):
        return result.as_dict()
    return asdict(result)
