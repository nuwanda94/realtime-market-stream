"""Bronze stream processor: deserialize, schema-enforce, land raw events.

Consumes ``raw-ticks``, validates each payload against the canonical tick
schemas, and writes accepted records into a partitioned Bronze layout
(``bronze/ticks`` partitioned by symbol + date). Invalid records are
routed to the DLQ topic.

The lakehouse write is abstracted behind :class:`BronzeSink` so unit tests
need neither Redpanda nor MinIO. ``build_bronze_sink`` selects JSONL, Delta,
or Iceberg from ``LAKEHOUSE_FORMAT``.

Offsets are committed only after a successful sink write (see
:mod:`realtime_market_stream.processing.checkpoint`).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

from pydantic import ValidationError

from realtime_market_stream.config.settings import Settings, get_settings
from realtime_market_stream.ingestion.service import (
    EventPublisher,
    KafkaEventPublisher,
    encode_dlq,
    parse_market_event,
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
from realtime_market_stream.schemas.ticks import OhlcvBar, TradeTick
from realtime_market_stream.sinks.bronze import BronzeRecord, BronzeSink
from realtime_market_stream.sinks.delta import build_bronze_sink

logger = logging.getLogger(__name__)

MarketEvent = TradeTick | OhlcvBar


class EventConsumer(Protocol):
    """Minimal consumer so tests can inject a fake."""

    def poll(self) -> Iterator[ConsumedRecord | tuple[bytes | None, bytes]]: ...

    def close(self) -> None: ...


class KafkaEventConsumer:
    """Thin kafka-python consumer wrapper with manual offset commits."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "bronze-processor",
        auto_offset_reset: str = "earliest",
    ) -> None:
        from kafka import KafkaConsumer

        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
        )

    def poll(self) -> Iterator[ConsumedRecord]:
        for message in self._consumer:
            key = message.key if isinstance(message.key, (bytes, type(None))) else None
            value = message.value if isinstance(message.value, bytes) else b""
            yield ConsumedRecord(
                key=key,
                value=value,
                topic=str(message.topic),
                partition=int(message.partition),
                offset=int(message.offset),
            )

    def commit_offsets(self, records: list[ConsumedRecord]) -> None:
        if not records:
            return
        from kafka.structs import OffsetAndMetadata, TopicPartition

        latest: dict[tuple[str, int], int] = {}
        for record in records:
            if record.offset < 0 or not record.topic:
                continue
            key = (record.topic, record.partition)
            current = latest.get(key, -1)
            if record.offset > current:
                latest[key] = record.offset
        if not latest:
            return
        payload = {
            TopicPartition(topic, partition): OffsetAndMetadata(offset + 1, "", -1)
            for (topic, partition), offset in latest.items()
        }
        self._consumer.commit(payload)

    def close(self) -> None:
        self._consumer.close()


def decode_payload(value: bytes) -> Any:
    """Parse a Kafka value as JSON. Raises ``json.JSONDecodeError``."""
    return json.loads(value.decode("utf-8"))


def event_to_bronze_record(event: MarketEvent, ingested_at: datetime | None = None) -> BronzeRecord:
    """Project a validated market event into a Bronze row."""
    ingested = ingested_at or datetime.now(tz=UTC)
    payload = event.to_kafka_value()
    if isinstance(event, TradeTick):
        event_ts = event.ts
        event_type = "trade"
    else:
        event_ts = event.window_start
        event_type = "ohlcv"
    if event_ts.tzinfo is None:
        event_ts = event_ts.replace(tzinfo=UTC)
    return BronzeRecord(
        event_type=event_type,
        symbol=event.symbol,
        event_date=event_ts.astimezone(UTC).date().isoformat(),
        ingested_at=ingested.astimezone(UTC).isoformat(),
        payload=payload,
    )


@dataclass
class BronzeStats:
    consumed: int = 0
    written: int = 0
    dlq: int = 0
    batches: int = 0
    skipped: int = 0
    checkpoints: int = 0


@dataclass
class BronzeProcessor:
    """Validate raw-ticks and persist them to the Bronze lakehouse layer."""

    settings: Settings | None = None
    sink: BronzeSink | None = None
    publisher: EventPublisher | None = None
    consumer: EventConsumer | None = None
    batch_size: int = 50
    consumer_group: str = "bronze-processor"
    checkpoint_store: FileCheckpointStore | None = None
    stats: BronzeStats = field(default_factory=BronzeStats)
    _owns_publisher: bool = field(default=False, init=False, repr=False)
    _owns_consumer: bool = field(default=False, init=False, repr=False)
    _offsets: OffsetMap = field(default_factory=OffsetMap, init=False, repr=False)

    def __post_init__(self) -> None:
        self.settings = self.settings or get_settings()
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.sink is None:
            self.sink = build_bronze_sink(self.settings)
        if self.checkpoint_store is None:
            self.checkpoint_store = FileCheckpointStore(self.settings.checkpoint.dir)
        loaded = self.checkpoint_store.load(self.consumer_group)
        self._offsets = OffsetMap(committed=dict(loaded.offsets))

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

    def process_value(self, value: bytes) -> BronzeRecord | None:
        """Deserialize + schema-enforce one Kafka value.

        Returns a Bronze record on success; otherwise the payload is sent to
        the DLQ and ``None`` is returned.
        """
        self.stats.consumed += 1
        try:
            raw = decode_payload(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._to_dlq(value.decode("utf-8", errors="replace"), f"invalid json: {exc}")
            return None
        if not isinstance(raw, dict):
            self._to_dlq(raw, "payload is not a JSON object")
            return None
        try:
            event = parse_market_event(raw)
        except ValidationError as exc:
            self._to_dlq(raw, str(exc))
            return None
        return event_to_bronze_record(event)

    def process_batch(self, values: Iterable[bytes]) -> list[BronzeRecord]:
        """Validate a batch and flush accepted rows to the sink."""
        records = records_from_values(values)
        return self.process_consumed(records)

    def process_consumed(self, records: Iterable[ConsumedRecord]) -> list[BronzeRecord]:
        """Process consumed records, skip committed offsets, then checkpoint."""
        pending = [coerce_consumed(record) for record in records]
        fresh = list(iter_unprocessed(pending, self._offsets))
        self.stats.skipped += len(pending) - len(fresh)
        accepted: list[BronzeRecord] = []
        for record in fresh:
            row = self.process_value(record.value)
            if row is not None:
                accepted.append(row)
        if accepted:
            assert self.sink is not None
            self.sink.write(accepted)
            self.stats.written += len(accepted)
            self.stats.batches += 1
        if fresh:
            self._commit_batch(fresh)
        return accepted

    def run(self, max_records: int | None = None) -> BronzeStats:
        """Consume from raw-ticks until ``max_records`` or the consumer idles."""
        pending: list[ConsumedRecord] = []
        seen = 0
        try:
            for item in self._get_consumer().poll():
                pending.append(coerce_consumed(item))
                seen += 1
                if len(pending) >= self.batch_size:
                    self.process_consumed(pending)
                    pending.clear()
                if max_records is not None and seen >= max_records:
                    break
            if pending:
                self.process_consumed(pending)
        finally:
            self.close()
        return self.stats

    def _commit_batch(self, records: list[ConsumedRecord]) -> None:
        self._offsets.advance(records)
        assert self.checkpoint_store is not None
        self.checkpoint_store.save(
            ProcessorCheckpoint(
                consumer_group=self.consumer_group,
                offsets=dict(self._offsets.committed),
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

    def _to_dlq(self, raw: Any, error: str) -> None:
        assert self.settings is not None
        logger.warning("bronze routing invalid tick to DLQ: %s", error)
        try:
            self._get_publisher().send(
                self.settings.kafka.topic_dlq,
                key=b"bronze-invalid",
                value=encode_dlq(raw, error),
            )
        except Exception:  # noqa: BLE001
            logger.debug("DLQ publish failed", exc_info=True)
        self.stats.dlq += 1

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


def new_batch_id() -> str:
    """Stable-enough identifier for a micro-batch file name."""
    return uuid4().hex[:16]


def run_bronze(
    *,
    max_records: int | None = None,
    batch_size: int = 50,
    sink: BronzeSink | None = None,
    consumer: EventConsumer | None = None,
    publisher: EventPublisher | None = None,
    settings: Settings | None = None,
) -> BronzeStats:
    """Module-level entry used by the CLI script."""
    processor = BronzeProcessor(
        settings=settings,
        sink=sink,
        consumer=consumer,
        publisher=publisher,
        batch_size=batch_size,
    )
    return processor.run(max_records=max_records)
