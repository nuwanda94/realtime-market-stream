"""In-process pipeline: generator → ingest → bronze → silver.

Uses injectable publishers/sinks so CI needs neither Redpanda nor MinIO.
A live-broker test is skipped unless ``KAFKA_BOOTSTRAP_SERVERS`` is reachable.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from realtime_market_stream.config.settings import Settings
from realtime_market_stream.ingestion.generator import SyntheticTickGenerator
from realtime_market_stream.ingestion.service import IngestionService, encode_event
from realtime_market_stream.processing.bronze import BronzeProcessor
from realtime_market_stream.processing.checkpoint import ConsumedRecord, FileCheckpointStore
from realtime_market_stream.processing.dlq import is_replayable, parse_dlq_value
from realtime_market_stream.processing.silver import SilverProcessor
from realtime_market_stream.schemas.ticks import TradeTick
from realtime_market_stream.sinks.bronze import FilesystemBronzeSink, InMemoryBronzeSink
from realtime_market_stream.sinks.silver import FilesystemSilverSink, InMemorySilverSink
from tests.conftest import FakePublisher, make_trade

pytestmark = pytest.mark.integration


def test_synthetic_ingest_bronze_silver_in_memory(tmp_path: Path) -> None:
    settings = Settings()
    settings.checkpoint.dir = str(tmp_path / "ck")
    publisher = FakePublisher()
    start = datetime(2026, 9, 6, 10, 0, 0, tzinfo=UTC)
    generator = SyntheticTickGenerator(
        symbols=["AAPL", "MSFT"],
        ticks_per_sec=20,
        seed=42,
        start_time=start,
    )
    ingest = IngestionService(
        settings=settings,
        publisher=publisher,
        generator=generator,
        source="synthetic",
    )
    stats = ingest.run_synthetic(count=8)
    assert stats.published >= 8
    assert stats.dlq == 0

    raw_values = publisher.values_for(settings.kafka.topic_raw_ticks)
    assert raw_values

    bronze_sink = InMemoryBronzeSink()
    bronze = BronzeProcessor(
        settings=settings,
        sink=bronze_sink,
        publisher=publisher,
        checkpoint_store=FileCheckpointStore(tmp_path / "ck"),
        consumer_group="it-bronze",
    )
    bronze.process_batch(raw_values)
    assert bronze.stats.written == len(raw_values)
    assert bronze.stats.dlq == 0
    assert bronze_sink.records

    silver_sink = InMemorySilverSink()
    silver = SilverProcessor(
        settings=settings,
        sink=silver_sink,
        publisher=publisher,
        checkpoint_store=FileCheckpointStore(tmp_path / "ck"),
        consumer_group="it-silver",
        window_seconds=60,
    )
    emitted = silver.process_batch(raw_values, flush_open=True)
    assert silver.stats.trades_accepted >= 1
    assert silver.stats.dlq == 0
    assert emitted
    assert any(row.event_type == "trade" for row in silver_sink.records)


def test_filesystem_partitions_written(tmp_path: Path) -> None:
    settings = Settings()
    settings.checkpoint.dir = str(tmp_path / "ck")
    lake = tmp_path / "lake"
    publisher = FakePublisher()
    tick = make_trade()
    _key, value = encode_event(tick)

    bronze = BronzeProcessor(
        settings=settings,
        sink=FilesystemBronzeSink(lake),
        publisher=publisher,
        checkpoint_store=FileCheckpointStore(tmp_path / "ck"),
        consumer_group="fs-bronze",
    )
    bronze.process_batch([value])
    bronze_parts = list(lake.rglob("*.jsonl"))
    assert bronze_parts
    assert any("bronze/ticks" in str(path) for path in bronze_parts)
    assert any("symbol=AAPL" in str(path) for path in bronze_parts)
    assert any("date=2026-09-06" in str(path) for path in bronze_parts)

    silver = SilverProcessor(
        settings=settings,
        sink=FilesystemSilverSink(lake),
        publisher=publisher,
        checkpoint_store=FileCheckpointStore(tmp_path / "ck"),
        consumer_group="fs-silver",
        window_seconds=60,
    )
    silver.process_batch([value], flush_open=True)
    silver_parts = list((lake / "silver").rglob("*.jsonl"))
    assert silver_parts
    line = silver_parts[0].read_text(encoding="utf-8").splitlines()[0]
    payload = json.loads(line)
    assert payload["symbol"] == "AAPL"


def test_replayable_dlq_envelope_roundtrip() -> None:
    tick = make_trade()
    envelope = {
        "error": "temporary schema mismatch",
        "received_at": "2026-09-06T00:00:00+00:00",
        "payload": tick.to_kafka_value(),
    }
    raw = json.dumps(envelope).encode("utf-8")
    record = parse_dlq_value(raw)
    assert is_replayable(record)
    event = TradeTick.model_validate(record.payload)
    assert event.symbol == "AAPL"


def test_offset_skip_on_second_pass(tmp_path: Path) -> None:
    settings = Settings()
    store = FileCheckpointStore(tmp_path / "ck")
    sink = InMemoryBronzeSink()
    publisher = FakePublisher()
    tick = make_trade()
    _key, value = encode_event(tick)
    record = ConsumedRecord(
        key=b"AAPL",
        value=value,
        topic="raw-ticks",
        partition=0,
        offset=7,
    )
    first = BronzeProcessor(
        settings=settings,
        sink=sink,
        publisher=publisher,
        checkpoint_store=store,
        consumer_group="idem",
    )
    first.process_consumed([record])
    assert first.stats.written == 1

    second = BronzeProcessor(
        settings=settings,
        sink=sink,
        publisher=publisher,
        checkpoint_store=store,
        consumer_group="idem",
    )
    second.process_consumed([record])
    assert second.stats.skipped == 1
    assert second.stats.written == 0
    assert len(sink.records) == 1


@pytest.mark.broker
def test_optional_redpanda_bootstrap_reachable() -> None:
    """Skip unless a local broker is actually listening.

    Kept so `pytest -m broker` can exercise a live laptop compose stack
    without failing default CI.
    """
    host = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    try:
        from kafka import KafkaAdminClient
    except ImportError:
        pytest.skip("kafka-python not importable")
    try:
        client = KafkaAdminClient(bootstrap_servers=host, request_timeout_ms=1500)
        client.close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Redpanda not reachable at {host}: {exc}")
