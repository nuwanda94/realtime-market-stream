"""Unit tests for the local JSON Schema registry."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from realtime_market_stream.schemas.registry import (
    SchemaRegistry,
    SchemaRegistryError,
    SchemaSubject,
    build_json_schema,
    infer_subject,
    load_bundled_schema,
)
from realtime_market_stream.schemas.ticks import Side, TradeTick


def _trade_payload() -> dict:
    tick = TradeTick(
        tick_id=uuid4(),
        symbol="aapl",
        ts=datetime(2026, 9, 6, 1, 0, tzinfo=UTC),
        price=190.25,
        size=10,
        side=Side.BUY,
        venue="TEST",
        sequence=1,
    )
    return tick.to_kafka_value()


def test_bundled_schemas_load() -> None:
    trade = load_bundled_schema(SchemaSubject.TRADE_TICK)
    bar = load_bundled_schema(SchemaSubject.OHLCV_BAR)
    assert trade["x-subject"] == "trade-tick"
    assert bar["x-subject"] == "ohlcv-bar"
    assert "tick_id" in trade["required"]
    assert "window_start" in bar["required"]


def test_validate_accepts_canonical_trade() -> None:
    registry = SchemaRegistry()
    model = registry.validate(_trade_payload(), SchemaSubject.TRADE_TICK)
    assert model.symbol == "AAPL"


def test_validate_rejects_missing_fields() -> None:
    registry = SchemaRegistry()
    with pytest.raises(SchemaRegistryError, match="missing required"):
        registry.validate({"event_type": "trade", "symbol": "AAPL"})


def test_validate_rejects_wrong_types() -> None:
    registry = SchemaRegistry()
    payload = _trade_payload()
    payload["price"] = "not-a-number"
    with pytest.raises(SchemaRegistryError, match="expected type"):
        registry.validate(payload, SchemaSubject.TRADE_TICK)


def test_infer_subject_from_event_type() -> None:
    assert infer_subject({"event_type": "ohlcv"}) is SchemaSubject.OHLCV_BAR
    assert infer_subject({"event_type": "trade"}) is SchemaSubject.TRADE_TICK


def test_build_json_schema_has_stable_id() -> None:
    schema = build_json_schema(SchemaSubject.TRADE_TICK)
    assert schema["$id"].endswith("/trade-tick/v1")
    assert schema["x-version"] == 1


def test_list_subjects() -> None:
    names = SchemaRegistry().list_subjects()
    assert names == ["trade-tick", "ohlcv-bar"]
