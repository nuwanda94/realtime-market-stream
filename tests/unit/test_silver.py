"""Unit tests for the Silver stream processor (no broker / MinIO required)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from realtime_market_stream.config.settings import Settings
from realtime_market_stream.ingestion.service import encode_event
from realtime_market_stream.processing.silver import (
    SilverProcessor,
    enrich_trade,
    floor_window,
)
from realtime_market_stream.schemas.ticks import Side, TradeTick
from realtime_market_stream.sinks.silver import FilesystemSilverSink, InMemorySilverSink


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


def _trade(
    symbol: str = "AAPL",
    price: float = 190.0,
    size: int = 10,
    ts: datetime | None = None,
    tick_id=None,
    side: Side = Side.BUY,
) -> TradeTick:
    return TradeTick(
        tick_id=tick_id or uuid4(),
        symbol=symbol,
        ts=ts or datetime(2026, 9, 5, 12, 0, 5, tzinfo=UTC),
        price=price,
        size=size,
        side=side,
        venue="TEST",
        sequence=1,
    )


def test_floor_window_aligns_to_minute() -> None:
    ts = datetime(2026, 9, 5, 12, 0, 37, tzinfo=UTC)
    start = floor_window(ts, timedelta(seconds=60))
    assert start == datetime(2026, 9, 5, 12, 0, 0, tzinfo=UTC)


def test_enrich_trade_computes_return_bps() -> None:
    payload = enrich_trade(_trade(price=101.0), prev_close=100.0)
    assert payload["notional"] == 1010.0
    assert payload["return_bps"] == 100.0
    assert payload["prev_close"] == 100.0


def test_duplicate_tick_is_dropped() -> None:
    tick = _trade()
    sink = InMemorySilverSink()
    publisher = FakePublisher()
    processor = SilverProcessor(
        settings=Settings(),
        sink=sink,
        publisher=publisher,
        window_seconds=60,
    )
    _, value = encode_event(tick)
    first = processor.process_value(value)
    second = processor.process_value(value)
    assert first
    assert second == []
    assert processor.stats.duplicates == 1
    assert processor.stats.trades_accepted == 1


def test_window_roll_closes_prior_bar() -> None:
    sink = InMemorySilverSink()
    processor = SilverProcessor(
        settings=Settings(),
        sink=sink,
        publisher=FakePublisher(),
        window_seconds=60,
    )
    t1 = _trade(price=100.0, ts=datetime(2026, 9, 5, 12, 0, 10, tzinfo=UTC))
    t2 = _trade(price=102.0, size=5, ts=datetime(2026, 9, 5, 12, 1, 5, tzinfo=UTC), side=Side.SELL)
    _, v1 = encode_event(t1)
    _, v2 = encode_event(t2)
    records = processor.process_batch([v1, v2])
    bars = [r for r in records if r.event_type == "ohlcv"]
    assert len(bars) == 1
    assert bars[0].payload["open"] == 100.0
    assert bars[0].payload["close"] == 100.0
    assert bars[0].payload["volume"] == 10
    assert processor.stats.bars_closed == 1


def test_flush_open_windows_emits_in_flight_bar() -> None:
    sink = InMemorySilverSink()
    processor = SilverProcessor(
        settings=Settings(),
        sink=sink,
        publisher=FakePublisher(),
        window_seconds=60,
    )
    t1 = _trade(price=100.0, size=4, side=Side.BUY)
    t2 = _trade(price=110.0, size=6, side=Side.SELL)
    _, v1 = encode_event(t1)
    _, v2 = encode_event(t2)
    processor.process_batch([v1, v2])
    closed = processor.flush_open_windows()
    processor._persist(closed)
    bars = [r for r in sink.records if r.event_type == "ohlcv"]
    assert len(bars) == 1
    payload = bars[0].payload
    assert payload["open"] == 100.0
    assert payload["high"] == 110.0
    assert payload["low"] == 100.0
    assert payload["close"] == 110.0
    assert payload["volume"] == 10
    assert payload["buy_volume"] == 4
    assert payload["sell_volume"] == 6
    assert payload["imbalance"] == -0.2


def test_invalid_json_goes_to_dlq() -> None:
    publisher = FakePublisher()
    processor = SilverProcessor(
        settings=Settings(),
        sink=InMemorySilverSink(),
        publisher=publisher,
    )
    assert processor.process_value(b"not-json") == []
    assert processor.stats.dlq == 1
    assert publisher.sent[0][0] == "dlq"


def test_filesystem_sink_writes_silver_partitions(tmp_path: Path) -> None:
    sink = FilesystemSilverSink(tmp_path)
    processor = SilverProcessor(
        settings=Settings(),
        sink=sink,
        publisher=FakePublisher(),
        window_seconds=60,
    )
    _, value = encode_event(_trade("GOOG", price=175.0))
    processor.process_batch([value], flush_open=True)
    files = list(tmp_path.rglob("*.jsonl"))
    assert files
    assert any("silver/ticks" in str(p) for p in files)
    assert any("symbol=GOOG" in str(p) for p in files)


def test_run_consumes_from_injected_consumer() -> None:
    sink = InMemorySilverSink()
    tick = _trade()
    _, good = encode_event(tick)
    consumer = FakeConsumer([good, good, b"{not json}"])
    processor = SilverProcessor(
        settings=Settings(),
        sink=sink,
        publisher=FakePublisher(),
        consumer=consumer,
        window_seconds=60,
    )
    stats = processor.run()
    assert stats.consumed == 3
    assert stats.trades_accepted == 1
    assert stats.duplicates == 1
    assert stats.dlq == 1
    assert stats.bars_closed == 1
    topics = json.dumps([r.event_type for r in sink.records])
    assert "trade" in topics
    assert "ohlcv" in topics
