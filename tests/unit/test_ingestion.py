"""Unit tests for the ingestion service (no broker required)."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from realtime_market_stream.config.settings import Settings
from realtime_market_stream.ingestion.generator import SyntheticTickGenerator
from realtime_market_stream.ingestion.service import (
    IngestionService,
    parse_market_event,
)
from realtime_market_stream.schemas.ticks import Side, TradeTick


class FakePublisher:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes | None, bytes]] = []

    def send(self, topic: str, key: bytes | None, value: bytes) -> None:
        self.sent.append((topic, key, value))

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


def test_parse_canonical_trade() -> None:
    tick = TradeTick(
        tick_id=uuid4(),
        symbol="aapl",
        ts=datetime(2026, 1, 1, tzinfo=UTC),
        price=190.5,
        size=10,
        side=Side.BUY,
        venue="TEST",
        sequence=1,
    )
    parsed = parse_market_event(tick.to_kafka_value())
    assert isinstance(parsed, TradeTick)
    assert parsed.symbol == "AAPL"


def test_parse_compact_live_payload() -> None:
    event = parse_market_event({"s": "msft", "p": 420.1, "sz": 3, "side": "SELL"})
    assert isinstance(event, TradeTick)
    assert event.symbol == "MSFT"
    assert event.price == 420.1
    assert event.size == 3
    assert event.side is Side.SELL
    assert event.venue == "LIVE"


def test_invalid_payload_goes_to_dlq() -> None:
    publisher = FakePublisher()
    service = IngestionService(
        settings=Settings(),
        publisher=publisher,
        source="synthetic",
    )
    result = service.publish_raw({"symbol": "AAPL", "price": -1, "size": 1})
    assert result is None
    assert service.stats.dlq == 1
    assert publisher.sent[0][0] == "dlq"


def test_synthetic_run_publishes_to_raw_topic() -> None:
    publisher = FakePublisher()
    gen = SyntheticTickGenerator(symbols=["NVDA"], ticks_per_sec=100, seed=1)
    service = IngestionService(
        settings=Settings(),
        publisher=publisher,
        generator=gen,
        source="synthetic",
    )
    stats = service.run_synthetic(count=5)
    raw_sends = [item for item in publisher.sent if item[0] == "raw-ticks"]
    assert stats.published == len(raw_sends)
    assert stats.published >= 5
    assert stats.source_synthetic == stats.published
    assert all(item[1] == b"NVDA" for item in raw_sends)


def test_auto_without_url_uses_synthetic() -> None:
    publisher = FakePublisher()
    service = IngestionService(
        settings=Settings(),
        publisher=publisher,
        generator=SyntheticTickGenerator(symbols=["GOOG"], ticks_per_sec=50, seed=2),
        source="auto",
        websocket_url="",
    )
    service.run(count=2)
    assert service.stats.source_synthetic >= 2
    assert service.stats.source_live == 0
