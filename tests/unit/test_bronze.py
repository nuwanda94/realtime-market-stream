"""Unit tests for the Bronze stream processor (no broker / MinIO required)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from realtime_market_stream.config.settings import Settings
from realtime_market_stream.ingestion.service import encode_event
from realtime_market_stream.processing.bronze import BronzeProcessor, event_to_bronze_record
from realtime_market_stream.schemas.ticks import OhlcvBar, Side, TradeTick
from realtime_market_stream.sinks.bronze import FilesystemBronzeSink, InMemoryBronzeSink


class FakePublisher:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes | None, bytes]] = []

    def send(self, topic: str, key: bytes | None, value: bytes) -> None:
        self.sent.append((topic, key, value))

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


class FakeConsumer:
    def __init__(self, values: list[bytes]) -> None:
        self._values = values

    def poll(self):
        for value in self._values:
            yield b"AAPL", value

    def close(self) -> None:
        return None


def _trade(symbol: str = "AAPL", price: float = 190.0) -> TradeTick:
    return TradeTick(
        tick_id=uuid4(),
        symbol=symbol,
        ts=datetime(2026, 9, 5, 12, 0, tzinfo=UTC),
        price=price,
        size=10,
        side=Side.BUY,
        venue="TEST",
        sequence=1,
    )


def test_event_to_bronze_record_partitions_by_symbol_and_date() -> None:
    record = event_to_bronze_record(_trade("msft"))
    assert record.symbol == "MSFT"
    assert record.event_type == "trade"
    assert record.event_date == "2026-09-05"
    assert "MSFT" in record.partition_path()
    assert "date=2026-09-05" in record.partition_path()


def test_process_valid_trade_writes_to_sink() -> None:
    sink = InMemoryBronzeSink()
    publisher = FakePublisher()
    processor = BronzeProcessor(settings=Settings(), sink=sink, publisher=publisher)
    _, value = encode_event(_trade())
    record = processor.process_value(value)
    assert record is not None
    processor.process_batch([value])
    assert processor.stats.written == 1
    assert processor.stats.dlq == 0
    assert sink.records[0].symbol == "AAPL"


def test_invalid_json_goes_to_dlq() -> None:
    sink = InMemoryBronzeSink()
    publisher = FakePublisher()
    processor = BronzeProcessor(settings=Settings(), sink=sink, publisher=publisher)
    result = processor.process_value(b"not-json")
    assert result is None
    assert processor.stats.dlq == 1
    assert publisher.sent[0][0] == "dlq"
    assert sink.records == []


def test_schema_violation_goes_to_dlq() -> None:
    sink = InMemoryBronzeSink()
    publisher = FakePublisher()
    processor = BronzeProcessor(settings=Settings(), sink=sink, publisher=publisher)
    payload = json.dumps({"event_type": "trade", "symbol": "AAPL", "price": -1, "size": 1}).encode()
    assert processor.process_value(payload) is None
    assert processor.stats.dlq == 1


def test_ohlcv_lands_under_ohlcv_partition() -> None:
    bar = OhlcvBar(
        symbol="NVDA",
        window_start=datetime(2026, 9, 5, 14, 0, tzinfo=UTC),
        window_end=datetime(2026, 9, 5, 14, 1, tzinfo=UTC),
        open=100.0,
        high=101.0,
        low=99.5,
        close=100.5,
        volume=1_000,
        trade_count=12,
        vwap=100.2,
    )
    record = event_to_bronze_record(bar)
    assert record.event_type == "ohlcv"
    assert "event_type=ohlcv" in record.partition_path()


def test_filesystem_sink_writes_partitioned_jsonl(tmp_path: Path) -> None:
    sink = FilesystemBronzeSink(tmp_path)
    processor = BronzeProcessor(
        settings=Settings(),
        sink=sink,
        publisher=FakePublisher(),
    )
    _, value = encode_event(_trade("GOOG"))
    written = processor.process_batch([value])
    assert len(written) == 1
    files = list(tmp_path.rglob("*.jsonl"))
    assert len(files) == 1
    assert "symbol=GOOG" in str(files[0])
    assert "date=2026-09-05" in str(files[0])
    line = files[0].read_text(encoding="utf-8").strip()
    row = json.loads(line)
    assert row["symbol"] == "GOOG"
    assert row["event_type"] == "trade"


def test_run_consumes_from_injected_consumer() -> None:
    sink = InMemoryBronzeSink()
    _, good = encode_event(_trade())
    consumer = FakeConsumer([good, b"{not json}"])
    processor = BronzeProcessor(
        settings=Settings(),
        sink=sink,
        publisher=FakePublisher(),
        consumer=consumer,
        batch_size=10,
    )
    stats = processor.run()
    assert stats.consumed == 2
    assert stats.written == 1
    assert stats.dlq == 1
    assert len(sink.records) == 1
