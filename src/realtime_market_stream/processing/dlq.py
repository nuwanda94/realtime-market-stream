"""Dead-letter queue inspection and replay.

Invalid ticks from ingestion and Bronze land on ``KAFKA_TOPIC_DLQ`` as a
JSON envelope::

    {"error": "...", "received_at": "...", "payload": <original>}

This module parses those envelopes, classifies records as replayable
(payload now validates as a market event) or still-poison, and republishes
recoverable payloads onto ``raw-ticks`` so Bronze/Silver can ingest them
again. Dry-run mode inspects without producing.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from realtime_market_stream.config.settings import Settings, get_settings
from realtime_market_stream.ingestion.service import (
    EventPublisher,
    KafkaEventPublisher,
    encode_event,
    parse_market_event,
)
from realtime_market_stream.processing.bronze import EventConsumer, KafkaEventConsumer

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DlqRecord:
    """One decoded DLQ envelope."""

    error: str
    received_at: str
    payload: Any
    raw_value: bytes
    key: bytes | None = None

    @property
    def is_object_payload(self) -> bool:
        return isinstance(self.payload, dict)


def encode_dlq_envelope(raw: Any, error: str, *, source: str = "") -> bytes:
    """Serialize a DLQ envelope. Compatible with ingestion ``encode_dlq``."""
    envelope: dict[str, Any] = {
        "error": error,
        "received_at": datetime.now(tz=UTC).isoformat(),
        "payload": raw,
    }
    if source:
        envelope["source"] = source
    return json.dumps(envelope, default=str, separators=(",", ":")).encode("utf-8")


def parse_dlq_value(value: bytes, key: bytes | None = None) -> DlqRecord:
    """Decode a Kafka DLQ value into a :class:`DlqRecord`.

    Accepts the canonical envelope and also a bare JSON object (treated as
    the original payload with an empty error string).
    """
    text = value.decode("utf-8", errors="replace")
    try:
        decoded: Any = json.loads(text)
    except json.JSONDecodeError:
        return DlqRecord(
            error="dlq value is not valid JSON",
            received_at="",
            payload=text,
            raw_value=value,
            key=key,
        )
    if isinstance(decoded, dict) and "payload" in decoded:
        return DlqRecord(
            error=str(decoded.get("error", "")),
            received_at=str(decoded.get("received_at", "")),
            payload=decoded.get("payload"),
            raw_value=value,
            key=key,
        )
    return DlqRecord(
        error="",
        received_at="",
        payload=decoded,
        raw_value=value,
        key=key,
    )


def is_replayable(record: DlqRecord) -> bool:
    """True when the inner payload currently validates as a market event."""
    if not record.is_object_payload:
        return False
    try:
        parse_market_event(record.payload)
    except (ValidationError, TypeError, ValueError):
        return False
    return True


@dataclass
class ReplayStats:
    consumed: int = 0
    replayed: int = 0
    skipped: int = 0
    inspect_only: int = 0


@dataclass
class DlqReplayTool:
    """Consume the DLQ topic and optionally republish valid payloads."""

    settings: Settings | None = None
    publisher: EventPublisher | None = None
    consumer: EventConsumer | None = None
    consumer_group: str = "dlq-replay"
    stats: ReplayStats = field(default_factory=ReplayStats)
    _owns_publisher: bool = field(default=False, init=False, repr=False)
    _owns_consumer: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.settings = self.settings or get_settings()

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
                topic=self.settings.kafka.topic_dlq,
                group_id=self.consumer_group,
                auto_offset_reset="earliest",
            )
            self._owns_consumer = True
        return self.consumer

    def iter_records(self, max_records: int | None = None) -> Iterator[DlqRecord]:
        """Yield parsed DLQ records from the consumer."""
        seen = 0
        for key, value in self._get_consumer().poll():
            record = parse_dlq_value(value, key=key)
            self.stats.consumed += 1
            yield record
            seen += 1
            if max_records is not None and seen >= max_records:
                break

    def inspect(
        self,
        max_records: int | None = None,
        *,
        error_contains: str = "",
    ) -> list[DlqRecord]:
        """Read DLQ records without producing. Does not require a publisher."""
        needle = error_contains.lower()
        records: list[DlqRecord] = []
        try:
            for record in self.iter_records(max_records=max_records):
                if needle and needle not in record.error.lower():
                    self.stats.skipped += 1
                    continue
                self.stats.inspect_only += 1
                records.append(record)
        finally:
            self.close()
        return records

    def replay(
        self,
        max_records: int | None = None,
        *,
        dry_run: bool = False,
        error_contains: str = "",
    ) -> ReplayStats:
        """Republish replayable payloads to ``raw-ticks``.

        Non-replayable records are counted as skipped and left on the DLQ
        (the consumer still advances its group offset).
        """
        assert self.settings is not None
        needle = error_contains.lower()
        try:
            for record in self.iter_records(max_records=max_records):
                if needle and needle not in record.error.lower():
                    self.stats.skipped += 1
                    continue
                if not is_replayable(record):
                    logger.info("skipping non-replayable DLQ record: %s", record.error[:120])
                    self.stats.skipped += 1
                    continue
                event = parse_market_event(record.payload)
                if dry_run:
                    self.stats.inspect_only += 1
                    continue
                key, value = encode_event(event)
                self._get_publisher().send(self.settings.kafka.topic_raw_ticks, key, value)
                self.stats.replayed += 1
            if self.publisher is not None:
                self._get_publisher().flush()
        finally:
            self.close()
        return self.stats

    def close(self) -> None:
        if self.publisher is not None and self._owns_publisher:
            try:
                self.publisher.flush()
                self.publisher.close()
            except Exception:  # noqa: BLE001
                logger.debug("DLQ publisher close failed", exc_info=True)
        if self.consumer is not None and self._owns_consumer:
            try:
                self.consumer.close()
            except Exception:  # noqa: BLE001
                logger.debug("DLQ consumer close failed", exc_info=True)


def run_dlq_replay(
    *,
    max_records: int | None = None,
    dry_run: bool = False,
    inspect: bool = False,
    error_contains: str = "",
    consumer: EventConsumer | None = None,
    publisher: EventPublisher | None = None,
    settings: Settings | None = None,
) -> ReplayStats:
    """Module-level entry used by the CLI script."""
    tool = DlqReplayTool(settings=settings, consumer=consumer, publisher=publisher)
    if inspect:
        tool.inspect(max_records=max_records, error_contains=error_contains)
        return tool.stats
    return tool.replay(
        max_records=max_records,
        dry_run=dry_run,
        error_contains=error_contains,
    )
