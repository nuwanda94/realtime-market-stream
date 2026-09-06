"""Unit tests for Gold aggregations and anomaly scores (no broker)."""

from __future__ import annotations

import json
from pathlib import Path

from realtime_market_stream.config.settings import Settings
from realtime_market_stream.processing.gold import GoldProcessor, iqr_score, zscore
from realtime_market_stream.sinks.gold import FilesystemGoldSink, InMemoryGoldSink


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


def _bar(
    *,
    symbol: str = "AAPL",
    close: float = 100.0,
    volume: int = 100,
    window_start: str = "2026-09-05T12:00:00+00:00",
) -> bytes:
    payload = {
        "event_type": "ohlcv",
        "symbol": symbol,
        "window_start": window_start,
        "window_end": "2026-09-05T12:01:00+00:00",
        "open": close,
        "high": close + 0.5,
        "low": close - 0.5,
        "close": close,
        "volume": volume,
        "trade_count": 4,
        "vwap": close,
    }
    return json.dumps(payload).encode("utf-8")


def test_zscore_is_zero_with_short_history() -> None:
    assert zscore(1.0, [1.0]) == 0.0


def test_iqr_flags_extreme_return() -> None:
    history = [0.01, 0.0, -0.01, 0.005, 0.002, -0.003, 0.001, 0.004]
    score, flag = iqr_score(0.5, history, k=1.5)
    assert flag is True
    assert score > 0


def test_trades_are_skipped() -> None:
    processor = GoldProcessor(
        settings=Settings(),
        sink=InMemoryGoldSink(),
        publisher=FakePublisher(),
        lookback=5,
    )
    trade = json.dumps({"event_type": "trade", "symbol": "AAPL", "close": 100}).encode()
    assert processor.process_value(trade) == []
    assert processor.stats.skipped_trades == 1
    assert processor.stats.bars_scored == 0


def test_spike_emits_anomaly_and_alert() -> None:
    sink = InMemoryGoldSink()
    publisher = FakePublisher()
    processor = GoldProcessor(
        settings=Settings(),
        sink=sink,
        publisher=publisher,
        lookback=8,
        z_threshold=2.5,
    )
    closes = [100.0 + i * 0.1 for i in range(10)]
    values = [_bar(close=price, volume=100) for price in closes]
    values.append(_bar(close=160.0, volume=100))
    records = processor.process_batch(values)
    assert records
    last = records[-1]
    assert last.payload["is_anomaly"] is True
    assert processor.stats.anomalies >= 1
    assert any(topic == "alerts" for topic, _, _ in publisher.sent)


def test_invalid_json_goes_to_dlq() -> None:
    publisher = FakePublisher()
    processor = GoldProcessor(
        settings=Settings(),
        sink=InMemoryGoldSink(),
        publisher=publisher,
    )
    assert processor.process_value(b"not-json") == []
    assert processor.stats.dlq == 1
    assert publisher.sent[0][0] == "dlq"


def test_filesystem_sink_writes_gold_partitions(tmp_path: Path) -> None:
    sink = FilesystemGoldSink(tmp_path)
    processor = GoldProcessor(
        settings=Settings(),
        sink=sink,
        publisher=FakePublisher(),
        lookback=4,
    )
    processor.process_batch([_bar(symbol="NVDA", close=120.0)])
    files = list(tmp_path.rglob("*.jsonl"))
    assert files
    assert any("gold/bars" in str(path) for path in files)
    assert any("symbol=NVDA" in str(path) for path in files)


def test_run_consumes_from_injected_consumer() -> None:
    sink = InMemoryGoldSink()
    consumer = FakeConsumer([_bar(), _bar(close=101.0), b"{not json}"])
    processor = GoldProcessor(
        settings=Settings(),
        sink=sink,
        publisher=FakePublisher(),
        consumer=consumer,
        lookback=4,
    )
    stats = processor.run()
    assert stats.consumed == 3
    assert stats.bars_scored == 2
    assert stats.dlq == 1
    assert stats.written == 2
    assert all(row.event_type == "gold_bar" for row in sink.records)


def test_checkpoint_restores_history(tmp_path: Path) -> None:
    from realtime_market_stream.processing.checkpoint import FileCheckpointStore

    store = FileCheckpointStore(tmp_path)
    first = GoldProcessor(
        settings=Settings(),
        sink=InMemoryGoldSink(),
        publisher=FakePublisher(),
        checkpoint_store=store,
        lookback=8,
    )
    first.process_batch([_bar(close=100.0 + i) for i in range(6)])
    second = GoldProcessor(
        settings=Settings(),
        sink=InMemoryGoldSink(),
        publisher=FakePublisher(),
        checkpoint_store=store,
        lookback=8,
    )
    assert "AAPL" in second._windows
    assert len(second._windows["AAPL"].closes) == 6
