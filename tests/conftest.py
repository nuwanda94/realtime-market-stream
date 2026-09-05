"""Shared pytest fixtures. No cloud services; Redpanda is optional."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from realtime_market_stream.schemas.ticks import Side, TradeTick


class FakePublisher:
    """In-process EventPublisher used by unit and integration tests."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes | None, bytes]] = []

    def send(self, topic: str, key: bytes | None, value: bytes) -> None:
        self.sent.append((topic, key, value))

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None

    def values_for(self, topic: str) -> list[bytes]:
        return [value for name, _key, value in self.sent if name == topic]


class FakeConsumer:
    """Yields either raw values or ConsumedRecord-shaped tuples."""

    def __init__(self, values: list[bytes], *, topic: str = "raw-ticks") -> None:
        self._values = values
        self.topic = topic
        self.closed = False

    def poll(self) -> Iterator[tuple[bytes | None, bytes]]:
        for value in self._values:
            yield self.topic.encode("utf-8"), value

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_publisher() -> FakePublisher:
    return FakePublisher()


def make_trade(
    symbol: str = "AAPL",
    price: float = 190.0,
    size: int = 10,
    sequence: int = 1,
    ts: datetime | None = None,
) -> TradeTick:
    return TradeTick(
        tick_id=uuid4(),
        symbol=symbol,
        ts=ts or datetime(2026, 9, 6, 9, 30, tzinfo=UTC),
        price=price,
        size=size,
        side=Side.BUY,
        venue="TEST",
        sequence=sequence,
    )
