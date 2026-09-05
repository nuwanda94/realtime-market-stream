"""Unit tests for DLQ parsing and replay (no broker required)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

from realtime_market_stream.config.settings import Settings
from realtime_market_stream.ingestion.service import encode_dlq
from realtime_market_stream.processing.dlq import (
    DlqReplayTool,
    is_replayable,
    parse_dlq_value,
    run_dlq_replay,
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


class FakeConsumer:
    def __init__(self, values: list[bytes]) -> None:
        self._values = values

    def poll(self) -> Iterator[tuple[bytes | None, bytes]]:
        for value in self._values:
            yield b"dlq", value

    def close(self) -> None:
        return None


def _valid_tick_payload() -> dict[str, object]:
    tick = TradeTick(
        tick_id=uuid4(),
        symbol="AAPL",
        ts=datetime(2026, 1, 1, tzinfo=UTC),
        price=190.5,
        size=10,
        side=Side.BUY,
        venue="TEST",
        sequence=1,
    )
    return tick.to_kafka_value()


def test_parse_canonical_envelope() -> None:
    value = encode_dlq({"symbol": "AAPL"}, "price must be > 0")
    record = parse_dlq_value(value)
    assert record.error == "price must be > 0"
    assert record.payload == {"symbol": "AAPL"}
    assert not is_replayable(record)


def test_parse_bare_valid_payload() -> None:
    payload = _valid_tick_payload()
    value = json.dumps(payload).encode("utf-8")
    record = parse_dlq_value(value)
    assert record.error == ""
    assert is_replayable(record)


def test_replay_publishes_valid_payloads_to_raw_ticks() -> None:
    good = encode_dlq(_valid_tick_payload(), "transient schema mismatch")
    bad = encode_dlq({"symbol": "AAPL", "price": -1}, "price must be > 0")
    garbage = b"not-json"
    publisher = FakePublisher()
    stats = run_dlq_replay(
        consumer=FakeConsumer([good, bad, garbage]),
        publisher=publisher,
        settings=Settings(),
    )
    assert stats.consumed == 3
    assert stats.replayed == 1
    assert stats.skipped == 2
    assert publisher.sent[0][0] == "raw-ticks"
    assert publisher.sent[0][1] == b"AAPL"


def test_dry_run_does_not_publish() -> None:
    good = encode_dlq(_valid_tick_payload(), "ok now")
    publisher = FakePublisher()
    stats = run_dlq_replay(
        consumer=FakeConsumer([good]),
        publisher=publisher,
        settings=Settings(),
        dry_run=True,
    )
    assert stats.replayed == 0
    assert stats.inspect_only == 1
    assert publisher.sent == []


def test_inspect_lists_without_publishing() -> None:
    value = encode_dlq({"foo": 1}, "invalid json object")
    publisher = FakePublisher()
    tool = DlqReplayTool(
        settings=Settings(),
        consumer=FakeConsumer([value]),
        publisher=publisher,
    )
    records = tool.inspect()
    assert len(records) == 1
    assert records[0].error == "invalid json object"
    assert publisher.sent == []


def test_error_contains_filter() -> None:
    match = encode_dlq(_valid_tick_payload(), "schema version drift")
    other = encode_dlq(_valid_tick_payload(), "invalid json")
    publisher = FakePublisher()
    stats = run_dlq_replay(
        consumer=FakeConsumer([match, other]),
        publisher=publisher,
        settings=Settings(),
        error_contains="schema",
    )
    assert stats.replayed == 1
    assert stats.skipped == 1
