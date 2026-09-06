"""Gold stream processor: rolling features + z-score / IQR anomaly scores.

Consumes ``enriched-ticks``, keeps a per-symbol rolling window of closed
OHLCV bars, and emits Gold rows with:

* bar-level features (return, range, volume z-score)
* robust IQR outlier scores (Tukey fences on close returns)
* an ``is_anomaly`` flag when either detector fires

Anomalous rows are also published to the ``alerts`` topic. Offsets and
rolling windows are checkpointed after a successful sink write.
"""

from __future__ import annotations

import json
import logging
import math
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from realtime_market_stream.config.settings import Settings, get_settings
from realtime_market_stream.ingestion.service import EventPublisher, KafkaEventPublisher, encode_dlq
from realtime_market_stream.processing.bronze import (
    EventConsumer,
    KafkaEventConsumer,
    decode_payload,
)
from realtime_market_stream.processing.checkpoint import (
    ConsumedRecord,
    FileCheckpointStore,
    OffsetMap,
    ProcessorCheckpoint,
    coerce_consumed,
    iter_unprocessed,
    records_from_values,
)
from realtime_market_stream.sinks.delta import build_gold_sink
from realtime_market_stream.sinks.gold import GoldRecord, GoldSink

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK = 20
DEFAULT_Z_THRESHOLD = 3.0
DEFAULT_IQR_K = 1.5


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mu = _mean(values)
    var = sum((item - mu) ** 2 for item in values) / (len(values) - 1)
    return math.sqrt(var)


def _quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = q * (len(sorted_values) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_values[lo]
    weight = pos - lo
    return sorted_values[lo] * (1.0 - weight) + sorted_values[hi] * weight


def zscore(value: float, history: list[float]) -> float:
    """Sample z-score of ``value`` vs ``history`` (history excludes value)."""
    if len(history) < 2:
        return 0.0
    sigma = _stdev(history)
    if sigma == 0.0:
        return 0.0
    return (value - _mean(history)) / sigma


def iqr_score(value: float, history: list[float], k: float = DEFAULT_IQR_K) -> tuple[float, bool]:
    """Return (signed distance past Tukey fence / IQR, is_outlier).

    Score is 0 when the value sits inside ``[Q1 - k*IQR, Q3 + k*IQR]``.
    Outside, the score is how many IQRs the point sits beyond the fence
    (positive = high outlier, negative = low outlier).
    """
    if len(history) < 4:
        return 0.0, False
    ordered = sorted(history)
    q1 = _quantile(ordered, 0.25)
    q3 = _quantile(ordered, 0.75)
    iqr = q3 - q1
    if iqr == 0.0:
        return 0.0, False
    lower = q1 - k * iqr
    upper = q3 + k * iqr
    if value > upper:
        return (value - upper) / iqr, True
    if value < lower:
        return (value - lower) / iqr, True
    return 0.0, False


def gold_record_from_payload(
    symbol: str,
    event_ts: datetime,
    payload: dict[str, Any],
    ingested_at: datetime | None = None,
) -> GoldRecord:
    ingested = ingested_at or datetime.now(tz=UTC)
    if event_ts.tzinfo is None:
        event_ts = event_ts.replace(tzinfo=UTC)
    return GoldRecord(
        event_type="gold_bar",
        symbol=symbol,
        event_date=event_ts.astimezone(UTC).date().isoformat(),
        ingested_at=ingested.astimezone(UTC).isoformat(),
        payload=payload,
    )


@dataclass
class GoldStats:
    consumed: int = 0
    bars_scored: int = 0
    anomalies: int = 0
    published: int = 0
    written: int = 0
    skipped_trades: int = 0
    dlq: int = 0
    skipped: int = 0
    checkpoints: int = 0


@dataclass
class SymbolWindow:
    """Rolling close / volume history for one symbol."""

    closes: deque[float]
    volumes: deque[int]
    last_close: float | None = None

    def to_state(self) -> dict[str, Any]:
        return {
            "closes": list(self.closes),
            "volumes": list(self.volumes),
            "last_close": self.last_close,
        }

    @classmethod
    def from_state(cls, raw: dict[str, Any], lookback: int) -> SymbolWindow:
        closes = deque((float(v) for v in raw.get("closes", [])), maxlen=lookback)
        volumes = deque((int(v) for v in raw.get("volumes", [])), maxlen=lookback)
        last = raw.get("last_close")
        return cls(
            closes=closes,
            volumes=volumes,
            last_close=float(last) if last is not None else None,
        )


@dataclass
class GoldProcessor:
    """Score Silver OHLCV bars and land Gold feature rows."""

    settings: Settings | None = None
    sink: GoldSink | None = None
    publisher: EventPublisher | None = None
    consumer: EventConsumer | None = None
    lookback: int = DEFAULT_LOOKBACK
    z_threshold: float = DEFAULT_Z_THRESHOLD
    iqr_k: float = DEFAULT_IQR_K
    consumer_group: str = "gold-processor"
    checkpoint_store: FileCheckpointStore | None = None
    stats: GoldStats = field(default_factory=GoldStats)
    _owns_publisher: bool = field(default=False, init=False, repr=False)
    _owns_consumer: bool = field(default=False, init=False, repr=False)
    _windows: dict[str, SymbolWindow] = field(default_factory=dict, init=False, repr=False)
    _offsets: OffsetMap = field(default_factory=OffsetMap, init=False, repr=False)

    def __post_init__(self) -> None:
        self.settings = self.settings or get_settings()
        if self.lookback < 2:
            raise ValueError("lookback must be >= 2")
        if self.sink is None:
            self.sink = build_gold_sink(self.settings)
        if self.checkpoint_store is None:
            self.checkpoint_store = FileCheckpointStore(self.settings.checkpoint.dir)
        self._restore_checkpoint()

    def _window_for(self, symbol: str) -> SymbolWindow:
        window = self._windows.get(symbol)
        if window is None:
            window = SymbolWindow(
                closes=deque(maxlen=self.lookback),
                volumes=deque(maxlen=self.lookback),
            )
            self._windows[symbol] = window
        return window

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
                topic=self.settings.kafka.topic_enriched_ticks,
                group_id=self.consumer_group,
            )
            self._owns_consumer = True
        return self.consumer

    def score_bar(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Attach anomaly scores to a Silver OHLCV payload."""
        symbol = str(payload.get("symbol", "")).upper()
        try:
            close = float(payload["close"])
            volume = int(payload.get("volume", 0))
            high = float(payload.get("high", close))
            low = float(payload.get("low", close))
            open_px = float(payload.get("open", close))
        except (KeyError, TypeError, ValueError):
            return None
        if not symbol:
            return None

        window = self._window_for(symbol)
        ret = 0.0
        if window.last_close and window.last_close > 0:
            ret = (close / window.last_close) - 1.0

        hist_returns = _returns_from_closes(list(window.closes))
        hist_volumes = [float(v) for v in window.volumes]
        z_ret = zscore(ret, hist_returns)
        z_vol = zscore(float(volume), hist_volumes)
        iqr_ret, iqr_flag = iqr_score(ret, hist_returns, k=self.iqr_k)

        is_anomaly = abs(z_ret) >= self.z_threshold or abs(z_vol) >= self.z_threshold or iqr_flag
        range_pct = ((high - low) / close) if close else 0.0

        scored = dict(payload)
        scored.update(
            {
                "event_type": "gold_bar",
                "symbol": symbol,
                "return": round(ret, 8),
                "range_pct": round(range_pct, 8),
                "bar_open": open_px,
                "zscore_return": round(z_ret, 6),
                "zscore_volume": round(z_vol, 6),
                "iqr_score_return": round(iqr_ret, 6),
                "is_anomaly": is_anomaly,
                "lookback": self.lookback,
                "sample_size": len(window.closes),
            }
        )

        window.closes.append(close)
        window.volumes.append(volume)
        window.last_close = close
        return scored

    def process_value(self, value: bytes) -> list[GoldRecord]:
        self.stats.consumed += 1
        try:
            raw = decode_payload(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._to_dlq(value.decode("utf-8", errors="replace"), f"invalid json: {exc}")
            return []
        if not isinstance(raw, dict):
            self._to_dlq(raw, "payload is not a JSON object")
            return []

        event_type = str(raw.get("event_type", "")).lower()
        if event_type in {"trade", "trade-tick", "tick"}:
            self.stats.skipped_trades += 1
            return []
        if event_type not in {"ohlcv", "ohlcv-bar", "bar", "gold_bar", ""}:
            if "close" not in raw:
                self.stats.skipped_trades += 1
                return []

        scored = self.score_bar(raw)
        if scored is None:
            self._to_dlq(raw, "missing close/symbol for gold scoring")
            return []

        self.stats.bars_scored += 1
        ts = _event_ts(raw)
        record = gold_record_from_payload(scored["symbol"], ts, scored)
        if scored["is_anomaly"]:
            self.stats.anomalies += 1
            self._publish_alert(scored)
        return [record]

    def process_batch(self, values: Iterable[bytes]) -> list[GoldRecord]:
        return self.process_consumed(records_from_values(values))

    def process_consumed(self, records: Iterable[ConsumedRecord]) -> list[GoldRecord]:
        pending = [coerce_consumed(record) for record in records]
        fresh = list(iter_unprocessed(pending, self._offsets))
        self.stats.skipped += len(pending) - len(fresh)
        emitted: list[GoldRecord] = []
        for record in fresh:
            emitted.extend(self.process_value(record.value))
        if emitted:
            self._persist(emitted)
        if fresh:
            self._commit_batch(fresh)
        return emitted

    def run(self, max_records: int | None = None) -> GoldStats:
        pending: list[ConsumedRecord] = []
        seen = 0
        try:
            for item in self._get_consumer().poll():
                pending.append(coerce_consumed(item))
                seen += 1
                if max_records is not None and seen >= max_records:
                    break
            if pending:
                self.process_consumed(pending)
        finally:
            self.close()
        return self.stats

    def _persist(self, records: list[GoldRecord]) -> None:
        assert self.sink is not None
        self.sink.write(records)
        self.stats.written += len(records)

    def _publish_alert(self, payload: dict[str, Any]) -> None:
        assert self.settings is not None
        alert = {
            "event_type": "anomaly",
            "symbol": payload["symbol"],
            "zscore_return": payload["zscore_return"],
            "zscore_volume": payload["zscore_volume"],
            "iqr_score_return": payload["iqr_score_return"],
            "close": payload.get("close"),
            "volume": payload.get("volume"),
            "window_start": payload.get("window_start"),
        }
        try:
            self._get_publisher().send(
                self.settings.kafka.topic_alerts,
                key=str(payload["symbol"]).encode("utf-8"),
                value=json.dumps(alert, separators=(",", ":"), default=str).encode("utf-8"),
            )
            self.stats.published += 1
        except Exception:  # noqa: BLE001
            logger.debug("alerts publish failed", exc_info=True)

    def _to_dlq(self, raw: Any, error: str) -> None:
        assert self.settings is not None
        logger.warning("gold routing invalid payload to DLQ: %s", error)
        try:
            self._get_publisher().send(
                self.settings.kafka.topic_dlq,
                key=b"gold-invalid",
                value=encode_dlq(raw, error),
            )
        except Exception:  # noqa: BLE001
            logger.debug("DLQ publish failed", exc_info=True)
        self.stats.dlq += 1

    def _snapshot_state(self) -> dict[str, Any]:
        return {
            "lookback": self.lookback,
            "windows": {symbol: window.to_state() for symbol, window in self._windows.items()},
        }

    def _restore_checkpoint(self) -> None:
        assert self.checkpoint_store is not None
        loaded = self.checkpoint_store.load(self.consumer_group)
        self._offsets = OffsetMap(committed=dict(loaded.offsets))
        state = loaded.state
        windows = state.get("windows") if isinstance(state.get("windows"), dict) else {}
        restored: dict[str, SymbolWindow] = {}
        for symbol, raw in windows.items():
            if isinstance(raw, dict):
                restored[str(symbol)] = SymbolWindow.from_state(raw, self.lookback)
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
        commit = getattr(consumer, "commit_offsets", None) if consumer is not None else None
        if callable(commit):
            try:
                commit(records)
            except Exception:  # noqa: BLE001
                logger.debug("broker offset commit failed", exc_info=True)

    def close(self) -> None:
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


def _returns_from_closes(closes: list[float]) -> list[float]:
    returns: list[float] = []
    for prev, cur in zip(closes, closes[1:], strict=False):
        if prev > 0:
            returns.append((cur / prev) - 1.0)
    return returns


def _event_ts(payload: dict[str, Any]) -> datetime:
    raw = payload.get("window_start") or payload.get("ts")
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=UTC)
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    return datetime.now(tz=UTC)


def run_gold(
    *,
    max_records: int | None = None,
    lookback: int = DEFAULT_LOOKBACK,
    z_threshold: float = DEFAULT_Z_THRESHOLD,
    sink: GoldSink | None = None,
    consumer: EventConsumer | None = None,
    publisher: EventPublisher | None = None,
    settings: Settings | None = None,
) -> GoldStats:
    """Module-level entry used by the CLI script."""
    processor = GoldProcessor(
        settings=settings,
        sink=sink,
        consumer=consumer,
        publisher=publisher,
        lookback=lookback,
        z_threshold=z_threshold,
    )
    return processor.run(max_records=max_records)
