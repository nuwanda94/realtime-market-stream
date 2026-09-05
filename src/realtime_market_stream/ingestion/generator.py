"""Geometric-Brownian-motion synthetic market tick generator.

Produces last-sale prints and rolling 1-second OHLCV bars without any
external market data feed. Intended as the Phase 0 fallback source and
as a deterministic fixture for later pipeline tests (pass ``seed``).

This module does **not** publish to Kafka; topic wiring is a later task.
"""

from __future__ import annotations

import math
import random
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from realtime_market_stream.config.settings import GeneratorSettings
from realtime_market_stream.schemas.ticks import OhlcvBar, Side, TradeTick

# Rough mid prices so generated levels look finance-shaped rather than $1.00.
_DEFAULT_SPOTS: dict[str, float] = {
    "AAPL": 190.0,
    "MSFT": 420.0,
    "GOOG": 165.0,
    "AMZN": 180.0,
    "NVDA": 120.0,
    "TSLA": 250.0,
    "META": 500.0,
}


@dataclass
class _SymbolState:
    symbol: str
    price: float
    window_start: datetime
    window_open: float
    high: float
    low: float
    volume: int = 0
    notional: float = 0.0
    trade_count: int = 0
    sequence: int = 0


@dataclass
class SyntheticTickGenerator:
    """Stateful multi-symbol GBM tick factory.

    Parameters
    ----------
    symbols:
        Universe to simulate. Defaults to ``GeneratorSettings.tick_symbols``.
    ticks_per_sec:
        Aggregate print rate across the whole universe (round-robin).
    volatility:
        Annualized GBM volatility (e.g. 0.25 = 25%).
    bar_seconds:
        OHLCV window length. A bar is emitted when the window rolls.
    seed:
        Optional RNG seed for reproducible tests.
    start_time:
        Optional clock origin for bar windows (defaults to wall clock).
        Tests should pass a fixed datetime so OHLCV roll is deterministic.
    """

    symbols: list[str] = field(default_factory=list)
    ticks_per_sec: int = 50
    volatility: float = 0.25
    bar_seconds: int = 1
    seed: int | None = None
    venue: str = "SYNTH"
    start_time: datetime | None = None

    _rng: random.Random = field(init=False, repr=False)
    _states: dict[str, _SymbolState] = field(init=False, repr=False)
    _dt_years: float = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.symbols:
            self.symbols = list(GeneratorSettings().tick_symbols)
        if self.ticks_per_sec < 1:
            raise ValueError("ticks_per_sec must be >= 1")
        if self.bar_seconds < 1:
            raise ValueError("bar_seconds must be >= 1")
        self.symbols = [s.strip().upper() for s in self.symbols if s.strip()]
        if not self.symbols:
            raise ValueError("at least one symbol is required")

        self._rng = random.Random(self.seed)
        base = self.start_time or datetime.now(tz=UTC)
        if base.tzinfo is None:
            base = base.replace(tzinfo=UTC)
        window_start = base.replace(microsecond=0)
        self._states = {}
        for symbol in self.symbols:
            spot = _DEFAULT_SPOTS.get(symbol, 100.0)
            self._states[symbol] = _SymbolState(
                symbol=symbol,
                price=spot,
                window_start=window_start,
                window_open=spot,
                high=spot,
                low=spot,
            )
        # Time step implied by the aggregate rate, shared across symbols.
        self._dt_years = (1.0 / float(self.ticks_per_sec)) / (252.0 * 6.5 * 3600.0)

    @classmethod
    def from_settings(
        cls,
        settings: GeneratorSettings | None = None,
        *,
        seed: int | None = None,
        start_time: datetime | None = None,
    ) -> SyntheticTickGenerator:
        cfg = settings or GeneratorSettings()
        return cls(
            symbols=list(cfg.tick_symbols),
            ticks_per_sec=cfg.tick_rate_per_sec,
            seed=seed,
            start_time=start_time,
        )

    def _step_price(self, state: _SymbolState) -> float:
        # Driftless GBM: S * exp(-0.5 sigma^2 dt + sigma * sqrt(dt) * Z)
        z = self._rng.gauss(0.0, 1.0)
        sigma = self.volatility
        dt = self._dt_years
        shock = math.exp((-0.5 * sigma * sigma * dt) + (sigma * math.sqrt(dt) * z))
        state.price = max(0.01, state.price * shock)
        return state.price

    def _roll_bar_if_needed(
        self,
        state: _SymbolState,
        now: datetime,
    ) -> OhlcvBar | None:
        window_end = state.window_start + timedelta(seconds=self.bar_seconds)
        if now < window_end:
            return None
        volume = state.volume
        vwap = (state.notional / volume) if volume else state.window_open
        bar = OhlcvBar(
            symbol=state.symbol,
            window_start=state.window_start,
            window_end=window_end,
            open=state.window_open,
            high=state.high,
            low=state.low,
            close=state.price,
            volume=volume,
            trade_count=state.trade_count,
            vwap=round(vwap, 6),
        )
        state.window_start = window_end
        state.window_open = state.price
        state.high = state.price
        state.low = state.price
        state.volume = 0
        state.notional = 0.0
        state.trade_count = 0
        return bar

    def next_events(self, now: datetime | None = None) -> list[TradeTick | OhlcvBar]:
        """Advance one print on the next symbol; may also close an OHLCV bar."""
        now = now or datetime.now(tz=UTC)
        # Round-robin by lowest sequence so symbols stay balanced.
        state = min(self._states.values(), key=lambda s: s.sequence)
        price = self._step_price(state)
        size = self._rng.randint(1, 500)
        side = Side.BUY if self._rng.random() < 0.5 else Side.SELL
        state.sequence += 1
        state.high = max(state.high, price)
        state.low = min(state.low, price)
        state.volume += size
        state.notional += price * size
        state.trade_count += 1

        tick = TradeTick(
            tick_id=uuid4(),
            symbol=state.symbol,
            ts=now,
            price=round(price, 4),
            size=size,
            side=side,
            venue=self.venue,
            sequence=state.sequence,
        )
        events: list[TradeTick | OhlcvBar] = [tick]
        bar = self._roll_bar_if_needed(state, now)
        if bar is not None:
            events.append(bar)
        return events

    def stream(self, count: int | None = None) -> Iterator[TradeTick | OhlcvBar]:
        """Yield events, sleeping to honor ``ticks_per_sec``.

        ``count`` limits the number of *trade* ticks (bars do not count).
        ``count=None`` runs until interrupted.
        """
        interval = 1.0 / float(self.ticks_per_sec)
        produced = 0
        while count is None or produced < count:
            started = time.perf_counter()
            for event in self.next_events():
                yield event
                if isinstance(event, TradeTick):
                    produced += 1
            elapsed = time.perf_counter() - started
            remaining = interval - elapsed
            if remaining > 0:
                time.sleep(remaining)
