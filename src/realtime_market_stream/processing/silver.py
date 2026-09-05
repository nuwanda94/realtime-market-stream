"""Silver stream processor: dedup, enrich, and window trades into OHLC.

Consumes ``raw-ticks``, drops duplicate trade prints (by ``tick_id``),
enriches accepted trades, and rolls them into tumbling OHLCV bars. Closed
bars plus enriched trades are published to ``enriched-ticks`` and landed
under a partitioned Silver layout (symbol + date).

The lakehouse write is abstracted behind :class:`SilverSink`. ``build_silver_sink``
selects JSONL, Delta, or Iceberg from ``LAKEHOUSE_FORMAT``.

Dedup cache, prev-close, and open windows are checkpointed with Kafka
offsets so a restart does not emit duplicate Silver rows.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from realtime_market_stream.config.settings import Settings, get_settings
from realtime_market_stream.ingestion.service import (
    EventPublisher,
    KafkaEventPublisher,
    encode_dlq,
    parse_market_event,
)
from realtime_market_stream.processing.bronze import EventConsumer, KafkaEventConsumer, decode_payload
from realtime_market_stream.processing.checkpoint import (
    ConsumedRecord,
    FileCheckpointStore,
    OffsetMap,
    ProcessorCheckpoint,
    iter_unprocessed,
    records_from_values,
)
from realtime_market_stream.schemas.ticks import OhlcvBar, Side, TradeTick
from realtime_market_stream.sinks.delta import build_silver_sink
from realtime_market_stream.sinks.silver import SilverRecord, SilverSink

logger = logging.getLogger(__name__)

MarketEvent = TradeTick | OhlcvBar

DEFAULT_WINDOW_SECONDS = 60
DEFAULT_SEEN_CAP = 50_000


def floor_window(ts: datetime, window: timedelta) -> datetime:
    """Align ``ts`` down to the tumbling-window boundary."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    else:
        ts = ts.astimezone(UTC)
    epoch = int(ts.timestamp())
    size = max(int(window.total_seconds()), 1)
    start = epoch - (epoch % size)
    return datetime.fromtimestamp(start, tz=UTC)


@dataclass
class WindowAccumulator:
    """In-memory tumbling window for one symbol."""

    symbol: str
    window_start: datetime
    window_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    trade_count: int
    notional: float
    buy_volume: int
    sell_volume: int

    @classmethod
    def from_trade(cls, trade: TradeTick, window: timedelta) -> WindowAccumulator:
        start = floor_window(trade.ts, window)
        return cls(
            symbol=trade.symbol,
            window_start=start,
            window_end=start + window,
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
            volume=trade.size,
            trade_count=1,
            notional=trade.price * trade.size,
            buy_volume=trade.size if trade.side is Side.BUY else 0,
            sell_volume=trade.size if trade.side is Side.SELL else 0,
        )

    def add(self, trade: TradeTick) -> None:
        self.high = max(self.high, trade.price)
        self.low = min(self.low, trade.price)
        self.close = trade.price
        self.volume += trade.size
        self.trade_count += 1
        self.notional += trade.price * trade.size
        if trade.side is Side.BUY:
            self.buy_volume += trade.size
        else:
            self.sell_volume += trade.size

    def to_bar(self) -> OhlcvBar:
        vwap = self.notional / self.volume if self.volume else self.close
        return OhlcvBar(
            symbol=self.symbol,
            window_start=self.window_start,
            window_end=self.window_end,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            trade_count=self.trade_count,
            vwap=round(vwap, 6),
        )

    def to_enriched_payload(self) -> dict[str, Any]:
        bar = self.to_bar()
        payload = bar.to_kafka_value()
        payload["notional"] = round(self.notional, 6)
        payload["buy_volume"] = self.buy_volume
        payload["sell_volume"] = self.sell_volume
        payload["imbalance"] = _imbalance(self.buy_volume, self.sell_volume)
        return payload

    def to_state(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "trade_count": self.trade_count,
            "notional": self.notional,
            "buy_volume": self.buy_volume,
            "sell_volume": self.sell_volume,
        }

    @classmethod
    def from_state(cls, raw: dict[str, Any]) -> WindowAccumulator:
        return cls(
            symbol=str(raw["symbol"]),
            window_start=datetime.fromisoformat(str(raw["window_start"])),
            window_end=datetime.fromisoformat(str(raw["window_end"])),
            open=float(raw["open"]),
            high=float(raw["high"]),
            low=float(raw["low"]),
            close=float(raw["close"]),
            volume=int(raw["volume"]),
            trade_count=int(raw["trade_count"]),
            notional=float(raw["notional"]),
            buy_volume=int(raw["buy_volume"]),
            sell_volume=int(raw["sell_volume"]),
        )


def _imbalance(buy: int, sell: int) -> float:
    total = buy + sell
    if total == 0:
        return 0.0
    return round((buy - sell) / total, 6)


def enrich_trade(trade: TradeTick, prev_close: float | None) -> dict[str, Any]:
    """Add notional and simple return-vs-previous-close fields."""
    payload = trade.to_kafka_value()
    payload["notional"] = round(trade.price * trade.size, 6)
    if prev_close and prev_close > 0:
        payload["return_bps"] = round((trade.price / prev_close - 1.0) * 10_000, 4)
        payload["prev_close"] = prev_close
    else:
        payload["return_bps"] = 0.0
        payload["prev_close"] = trade.price
    return payload


def silver_record_from_payload(
    event_type: str,
    symbol: str,
    event_ts: datetime,
    payload: dict[str, Any],
    ingested_at: datetime | None = None,
) -> SilverRecord:
    ingested = ingested_at or datetime.now(tz=UTC)
    if event_ts.tzinfo is None:
        event_ts = event_ts.replace(tzinfo=UTC)
    return SilverRecord(
        event_type=event_type,
        symbol=symbol,
        event_date=event_ts.astimezone(UTC).date().isoformat(),
        ingested_at=ingested.astimezone(UTC).isoformat(),
        payload=payload,
    )


@dataclass
class SilverStats:
    consumed: int = 0
    trades_accepted: int = 0
    duplicates: int = 0
    bars_closed: int = 0
    published: int = 0
    written: int = 0
    dlq: int = 0
    skipped: int = 0
    checkpoints: int = 0


class DedupCache:
    """Bounded FIFO set of seen trade ids."""

    def __init__(self, max_size: int = DEFAULT_SEEN_CAP) -> None:
        self.max_size = max(max_size, 1)
        self._order: list[str] = []
        self._seen: set[str] = set()

    def seen(self, tick_id: UUID | str) -> bool:
        key = str(tick_id)
        if key in self._seen:
            return True
        self._seen.add(key)
        self._order.append(key)
        if len(self._order) > self.max_size:
            evicted = self._order.pop(0)
            self._seen.discard(evicted)
        return False

    def snapshot(self) -> list[str]:
        return list(self._order)

    def restore(self, tick_ids: Iterable[str]) -> None:
        for tick_id in tick_ids:
            self.seen(tick_id)


@dataclass
class SilverProcessor:
    """Dedup + enrich raw ticks and emit tumbling Silver OHLC bars."""

    settings: Settings | None = None
    sink: SilverSink | None = None
    publisher: EventPublisher | None = None
    consumer: EventConsumer | None = None
    window_seconds: int = DEFAULT_WINDOW_SECONDS
    seen_cap: int = DEFAULT_SEEN_CAP
    consumer_group: str = "silver-processor"
    checkpoint_store: FileCheckpointStore | None = None
    stats: SilverStats = field(default_factory=SilverStats)
    _owns_publisher: bool = field(default=False, init=False, repr=False)
    _owns_consumer: bool = field(default=False, init=False, repr=False)
    _windows: dict[str, WindowAccumulator] = field(default_factory=dict, init=False, repr=False)
    _prev_close: dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _dedup: DedupCache = field(init=False, repr=False)
    _offsets: OffsetMap = field(default_factory=OffsetMap, init=False, repr=False)

    def __post_init__(self) -> None:
        self.settings = self.settings or get_settings()
        if self.window_seconds < 1:
            raise ValueError("window_seconds must be >= 1")
        self._dedup = DedupCache(max_size=self.seen_cap)
        if self.sink is None:
            self.sink = build_silver_sink(self.settings)
        if self.checkpoint_store is None:
            self.checkpoint_store = FileCheckpointStore(self.settings.checkpoint.dir)
        self._restore_checkpoint()

    def _window(self) -> timedelta:
        return timedelta(seconds=self.window_seconds)

    def _get_publisher(self) -> EventPublisher:
        if self.publisher is None:
            assert self.settings is not None
            self.publisher = KafkaEventPublisher(self.settings.kafka.bootstrap_servers)
            self._owns_publisher = True
        return self.publisher

    def _get_consumer(self) -> EventConsumer:
        if self.consumer is None:
            assert self.settings is not None
            self.consumer = KafkaEventConsumer(
                bootstrap_servers=self.settings.kafka.bootstrap_servers,
                topic=self.settings.kafka.topic_raw_ticks,
                group_id=self.consumer_group,
            )
            self._owns_consumer = True
        return self.consumer

    def process_value(self, value: bytes) -> list[SilverRecord]:
        """Handle one Kafka value. Returns zero or more Silver records."""
        self.stats.consumed += 1
        try:
            raw = decode_payload(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._to_dlq(value.decode("utf-8", errors="replace"), f"invalid json: {exc}")
            return []
        if not isinstance(raw, dict):
            self._to_dlq(raw, "payload is not a JSON object")
            return []
        try:
            event = parse_market_event(raw)
        except ValidationError as exc:
            self._to_dlq(raw, str(exc))
            return []
        if isinstance(event, TradeTick):
            return self._handle_trade(event)
        return self._handle_bar(event)

    def process_batch(
        self, values: Iterable[bytes], *, flush_open: bool = False
    ) -> list[SilverRecord]:
        return self.process_consumed(records_from_values(values), flush_open=flush_open)

    def process_consumed(
        self, records: Iterable[ConsumedRecord], *, flush_open: bool = False
    ) -> list[SilverRecord]:
        pending = list(records)
        fresh = list(iter_unprocessed(pending, self._offsets))
        self.stats.skipped += len(pending) - len(fresh)
        emitted: list[SilverRecord] = []
        for record in fresh:
            emitted.extend(self.process_value(record.value))
        if flush_open:
            emitted.extend(self.flush_open_windows())
        if emitted:
            self._persist(emitted)
        if fresh or (flush_open and emitted):
            self._commit_batch(fresh)
        return emitted

    def flush_open_windows(self) -> list[SilverRecord]:
        """Close every in-flight window (end of batch / shutdown)."""
        records: list[SilverRecord] = []
        for acc in list(self._windows.values()):
            records.append(self._close_window(acc))
        self._windows.clear()
        return records

    def run(self, max_records: int | None = None) -> SilverStats:
        pending: list[ConsumedRecord] = []
        seen = 0
        try:
            for record in self._get_consumer().poll():
                pending.append(record)
                seen += 1
                if max_records is not None and seen >= max_records:
                    break
            if pending:
                self.process_consumed(pending, flush_open=True)
        finally:
            self.close()
        return self.stats

    def _handle_trade(self, trade: TradeTick) -> list[SilverRecord]:
        if self._dedup.seen(trade.tick_id):
            self.stats.duplicates += 1
            logger.debug("dropping duplicate tick %s", trade.tick_id)
            return []
        self.stats.trades_accepted += 1
        prev = self._prev_close.get(trade.symbol)
        payload = enrich_trade(trade, prev)
        self._prev_close[trade.symbol] = trade.price
        record = silver_record_from_payload("trade", trade.symbol, trade.ts, payload)
        out = [record]
        self._publish(trade.symbol, payload)
        out.extend(self._roll_window(trade))
        return out

    def _handle_bar(self, bar: OhlcvBar) -> list[SilverRecord]:
        """Pass through upstream OHLCV after adding imbalance placeholders."""
        payload = bar.to_kafka_value()
        payload.setdefault("notional", round(bar.vwap * bar.volume, 6))
        payload.setdefault("buy_volume", 0)
        payload.setdefault("sell_volume", 0)
        payload.setdefault("imbalance", 0.0)
        self._prev_close[bar.symbol] = bar.close
        record = silver_record_from_payload("ohlcv", bar.symbol, bar.window_start, payload)
        self._publish(bar.symbol, payload)
        return [record]

    def _roll_window(self, trade: TradeTick) -> list[SilverRecord]:
        window = self._window()
        start = floor_window(trade.ts, window)
        key = trade.symbol
        acc = self._windows.get(key)
        closed: list[SilverRecord] = []
        if acc is not None and acc.window_start != start:
            closed.append(self._close_window(acc))
            acc = None
        if acc is None:
            self._windows[key] = WindowAccumulator.from_trade(trade, window)
        else:
            acc.add(trade)
        return closed

    def _close_window(self, acc: WindowAccumulator) -> SilverRecord:
        payload = acc.to_enriched_payload()
        self._prev_close[acc.symbol] = acc.close
        self.stats.bars_closed += 1
        self._publish(acc.symbol, payload)
        return silver_record_from_payload("ohlcv", acc.symbol, acc.window_start, payload)

    def _persist(self, records: list[SilverRecord]) -> None:
        assert self.sink is not None
        self.sink.write(records)
        self.stats.written += len(records)

    def _publish(self, symbol: str, payload: dict[str, Any]) -> None:
        assert self.settings is not None
        try:
            self._get_publisher().send(
                self.settings.kafka.topic_enriched_ticks,
                key=symbol.encode("utf-8"),
                value=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            )
            self.stats.published += 1
        except Exception:  # noqa: BLE001
            logger.debug("enriched-ticks publish failed", exc_info=True)

    def _to_dlq(self, raw: Any, error: str) -> None:
        assert self.settings is not None
        logger.warning("silver routing invalid tick to DLQ: %s", error)
        try:
            self._get_publisher().send(
                self.settings.kafka.topic_dlq,
                key=b"silver-invalid",
                value=encode_dlq(raw, error),
            )
        except Exception:  # noqa: BLE001
            logger.debug("DLQ publish failed", exc_info=True)
        self.stats.dlq += 1

    def _snapshot_state(self) -> dict[str, Any]:
        return {
            "seen_tick_ids": self._dedup.snapshot(),
            "prev_close": self._prev_close,
            "windows": [acc.to_state() for acc in self._windows.values()],
            "window_seconds": self.window_seconds,
        }

    def _restore_checkpoint(self) -> None:
        assert self.checkpoint_store is not None
        loaded = self.checkpoint_store.load(self.consumer_group)
        self._offsets = OffsetMap(committed=dict(loaded.offsets))
        state = loaded.state
        seen = state.get("seen_tick_ids") if isinstance(state.get("seen_tick_ids"), list) else []
        self._dedup.restore(str(item) for item in seen)
        prev = state.get("prev_close") if isinstance(state.get("prev_close"), dict) else {}
        self._prev_close = {str(k): float(v) for k, v in prev.items()}
        windows = state.get("windows") if isinstance(state.get("windows"), list) else []
        restored: dict[str, WindowAccumulator] = {}
        for item in windows:
            if not isinstance(item, dict):
                continue
            acc = WindowAccumulator.from_state(item)
            restored[acc.symbol] = acc
        self._windows = restored

    def _commit_batch(self, records: list[ConsumedRecord]) -> None:
        self._offsets.advance(records)
        assert self.checkpoint_store is not None
        self.checkpoint_store.save(
            ProcessorCheckpoint(
                consumer_group=self.consumer_group,
                offsets=dict(self._offsets.committed),
                state=self._snapshot_state(),
            )
        )
        self.stats.checkpoints += 1
        consumer = self.consumer
        if consumer is not None:
            try:
                consumer.commit_offsets(records)
            except Exception:  # noqa: BLE001
                logger.debug("broker offset commit failed", exc_info=True)

    def close(self) -> None:
        leftover = self.flush_open_windows()
        if leftover:
            try:
                self._persist(leftover)
                self._commit_batch([])
            except Exception:  # noqa: BLE001
                logger.debug("final silver flush failed", exc_info=True)
        if self.publisher is not None and self._owns_publisher:
            try:
                self.publisher.flush()
                self.publisher.close()
            except Exception:  # noqa: BLE001
                logger.debug("publisher close failed", exc_info=True)
        if self.consumer is not None and self._owns_consumer:
            try:
                self.consumer.close()
            except Exception:  # noqa: BLE001
                logger.debug("consumer close failed", exc_info=True)


def run_silver(
    *,
    max_records: int | None = None,
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
    sink: SilverSink | None = None,
    consumer: EventConsumer | None = None,
    publisher: EventPublisher | None = None,
    settings: Settings | None = None,
) -> SilverStats:
    """Module-level entry used by the CLI script."""
    processor = SilverProcessor(
        settings=settings,
        sink=sink,
        consumer=consumer,
        publisher=publisher,
        window_seconds=window_seconds,
    )
    return processor.run(max_records=max_records)
