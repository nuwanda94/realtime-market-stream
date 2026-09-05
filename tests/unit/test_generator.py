"""Unit tests for the synthetic tick generator."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from realtime_market_stream.ingestion.generator import SyntheticTickGenerator
from realtime_market_stream.schemas.ticks import OhlcvBar, TradeTick


def test_reproducible_with_seed() -> None:
    a = SyntheticTickGenerator(symbols=["AAPL"], ticks_per_sec=10, seed=42)
    b = SyntheticTickGenerator(symbols=["AAPL"], ticks_per_sec=10, seed=42)
    now = datetime(2026, 1, 1, 15, 0, 0, tzinfo=UTC)
    ticks_a = [e for e in a.next_events(now) if isinstance(e, TradeTick)]
    ticks_b = [e for e in b.next_events(now) if isinstance(e, TradeTick)]
    assert ticks_a[0].price == ticks_b[0].price
    assert ticks_a[0].size == ticks_b[0].size
    assert ticks_a[0].symbol == "AAPL"


def test_round_robin_across_symbols() -> None:
    gen = SyntheticTickGenerator(symbols=["AAPL", "MSFT"], ticks_per_sec=20, seed=1)
    now = datetime(2026, 1, 1, 15, 0, 0, tzinfo=UTC)
    seen: list[str] = []
    for _ in range(4):
        for event in gen.next_events(now):
            if isinstance(event, TradeTick):
                seen.append(event.symbol)
    assert seen.count("AAPL") == 2
    assert seen.count("MSFT") == 2


def test_ohlcv_bar_rolls_after_window() -> None:
    gen = SyntheticTickGenerator(symbols=["NVDA"], ticks_per_sec=10, bar_seconds=1, seed=7)
    start = datetime(2026, 1, 1, 15, 0, 0, tzinfo=UTC)
    bars: list[OhlcvBar] = []
    for offset_ms in range(0, 1500, 100):
        now = start + timedelta(milliseconds=offset_ms)
        for event in gen.next_events(now):
            if isinstance(event, OhlcvBar):
                bars.append(event)
    assert bars, "expected at least one closed 1s bar"
    bar = bars[0]
    assert bar.high >= bar.low
    assert bar.high >= bar.open
    assert bar.high >= bar.close
    assert bar.low <= bar.open
    assert bar.volume >= 0
    assert bar.trade_count >= 1
    assert bar.symbol == "NVDA"


def test_rejects_empty_universe() -> None:
    with pytest.raises(ValueError, match="at least one symbol"):
        SyntheticTickGenerator(symbols=["  "], ticks_per_sec=5)


def test_trade_tick_payload_shape() -> None:
    gen = SyntheticTickGenerator(symbols=["GOOG"], ticks_per_sec=5, seed=3)
    now = datetime(2026, 1, 1, 15, 0, 0, tzinfo=UTC)
    tick = next(e for e in gen.next_events(now) if isinstance(e, TradeTick))
    payload = tick.to_kafka_value()
    assert payload["event_type"] == "trade"
    assert payload["symbol"] == "GOOG"
    assert payload["price"] > 0
    assert payload["size"] >= 1
    assert payload["side"] in {"buy", "sell"}
    assert payload["venue"] == "SYNTH"
