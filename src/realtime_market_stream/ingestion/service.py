"""Ingestion service: live websocket client with synthetic fallback.

Publishes schema-validated ticks to the raw-ticks topic. Payloads that fail
validation are routed to the DLQ. The default source is synthetic so the
pipeline runs with zero external market-data cost.

Publish path is gated by :class:`FlowController` so a fast source cannot
flood Redpanda (token bucket + bounded inflight).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

from pydantic import ValidationError

from realtime_market_stream.config.settings import Settings, get_settings
from realtime_market_stream.ingestion.generator import SyntheticTickGenerator
from realtime_market_stream.processing.backpressure import (
    BackpressurePublisher,
    FlowController,
    controller_from_settings,
)
from realtime_market_stream.schemas.ticks import OhlcvBar, TradeTick

logger = logging.getLogger(__name__)

MarketEvent = TradeTick | OhlcvBar


class EventPublisher(Protocol):
    """Minimal producer interface so tests can inject a fake."""

    def send(self, topic: str, key: bytes | None, value: bytes) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


class KafkaEventPublisher:
    """Thin kafka-python wrapper."""

    def __init__(self, bootstrap_servers: str) -> None:
        from kafka import KafkaProducer

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            linger_ms=5,
            retries=3,
            key_serializer=None,
            value_serializer=None,
        )

    def send(self, topic: str, key: bytes | None, value: bytes) -> None:
        self._producer.send(topic, key=key, value=value)

    def flush(self) -> None:
        self._producer.flush(timeout=10)

    def close(self) -> None:
        self._producer.flush(timeout=10)
        self._producer.close()


def normalize_live_payload(raw: dict[str, Any]) -> dict[str, Any]:
    """Map a loosely-shaped live tick into TradeTick field names."""
    payload = dict(raw)
    if "symbol" not in payload and "s" in payload:
        payload["symbol"] = payload["s"]
    if "price" not in payload and "p" in payload:
        payload["price"] = payload["p"]
    if "size" not in payload and "sz" in payload:
        payload["size"] = payload["sz"]
    if "ts" not in payload:
        payload["ts"] = datetime.now(tz=UTC)
    if "tick_id" not in payload:
        payload["tick_id"] = uuid4()
    if "sequence" not in payload:
        payload["sequence"] = 0
    if "venue" not in payload:
        payload["venue"] = "LIVE"
    if "side" in payload and isinstance(payload["side"], str):
        payload["side"] = payload["side"].lower()
    return payload


def parse_market_event(raw: dict[str, Any]) -> MarketEvent:
    event_type = str(raw.get("event_type", "trade")).lower()
    if event_type == "ohlcv":
        return OhlcvBar.model_validate(raw)
    try:
        return TradeTick.model_validate(normalize_live_payload(raw))
    except ValidationError:
        if {"open", "high", "low", "close", "window_start"} <= set(raw):
            return OhlcvBar.model_validate(raw)
        raise


def encode_event(event: MarketEvent) -> tuple[bytes, bytes]:
    payload = event.to_kafka_value()
    key = event.symbol.encode("utf-8")
    value = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return key, value


def encode_dlq(raw: Any, error: str) -> bytes:
    envelope = {
        "error": error,
        "received_at": datetime.now(tz=UTC).isoformat(),
        "payload": raw,
    }
    return json.dumps(envelope, default=str, separators=(",", ":")).encode("utf-8")


@dataclass
class IngestStats:
    published: int = 0
    dlq: int = 0
    source_live: int = 0
    source_synthetic: int = 0
    dropped: int = 0


@dataclass
class IngestionService:
    settings: Settings | None = None
    publisher: EventPublisher | None = None
    generator: SyntheticTickGenerator | None = None
    source: str = "auto"
    websocket_url: str = ""
    stats: IngestStats = field(default_factory=IngestStats)
    flow: FlowController | None = None
    _owns_publisher: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.settings = self.settings or get_settings()
        self.source = (self.source or "auto").lower()
        if self.source not in {"auto", "synthetic", "websocket"}:
            raise ValueError("source must be auto, synthetic, or websocket")
        if self.generator is None:
            self.generator = SyntheticTickGenerator.from_settings(self.settings.generator)
        if self.flow is None:
            self.flow = controller_from_settings(self.settings.flow)
        if self.publisher is not None and not isinstance(self.publisher, BackpressurePublisher):
            self.publisher = BackpressurePublisher(self.publisher, self.flow)

    def _get_publisher(self) -> EventPublisher:
        if self.publisher is None:
            assert self.settings is not None
            assert self.flow is not None
            inner = KafkaEventPublisher(self.settings.kafka.bootstrap_servers)
            self.publisher = BackpressurePublisher(inner, self.flow)
            self._owns_publisher = True
        return self.publisher

    def publish_event(self, event: MarketEvent, *, from_live: bool = False) -> None:
        assert self.settings is not None
        before = self.flow.stats.dropped if self.flow is not None else 0
        key, value = encode_event(event)
        self._get_publisher().send(self.settings.kafka.topic_raw_ticks, key, value)
        after = self.flow.stats.dropped if self.flow is not None else 0
        if after > before:
            self.stats.dropped += after - before
            return
        self.stats.published += 1
        if from_live:
            self.stats.source_live += 1
        else:
            self.stats.source_synthetic += 1

    def publish_raw(self, raw: Any, *, from_live: bool = False) -> MarketEvent | None:
        assert self.settings is not None
        if not isinstance(raw, dict):
            self._to_dlq(raw, "payload is not a JSON object")
            return None
        try:
            event = parse_market_event(raw)
        except ValidationError as exc:
            self._to_dlq(raw, str(exc))
            return None
        self.publish_event(event, from_live=from_live)
        return event

    def _to_dlq(self, raw: Any, error: str) -> None:
        assert self.settings is not None
        logger.warning("routing invalid tick to DLQ: %s", error)
        self._get_publisher().send(
            self.settings.kafka.topic_dlq,
            key=b"invalid",
            value=encode_dlq(raw, error),
        )
        self.stats.dlq += 1

    def iter_synthetic(self, count: int | None = None) -> Iterator[MarketEvent]:
        assert self.generator is not None
        yield from self.generator.stream(count=count)

    def run_synthetic(self, count: int | None = None) -> IngestStats:
        for event in self.iter_synthetic(count=count):
            self.publish_event(event, from_live=False)
        self._get_publisher().flush()
        return self.stats

    def run(self, count: int | None = None) -> IngestStats:
        try:
            if self.source == "synthetic":
                return self.run_synthetic(count=count)
            if self.source == "websocket":
                return self._run_websocket(count=count, must_connect=True)
            if self.websocket_url:
                try:
                    return self._run_websocket(count=count, must_connect=True)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("live websocket failed (%s); falling back to synthetic", exc)
            return self.run_synthetic(count=count)
        finally:
            self.close()

    def _run_websocket(self, count: int | None, must_connect: bool) -> IngestStats:
        if not self.websocket_url:
            if must_connect:
                raise RuntimeError("websocket_url is empty")
            return self.run_synthetic(count=count)

        try:
            import websockets  # type: ignore[import-untyped]
        except ImportError as exc:
            if must_connect and self.source == "websocket":
                raise RuntimeError(
                    "the 'websockets' package is required for live ingestion"
                ) from exc
            logger.warning("websockets not installed; using synthetic source")
            return self.run_synthetic(count=count)

        import asyncio

        async def _consume() -> None:
            produced = 0
            async with websockets.connect(self.websocket_url, open_timeout=5) as ws:
                logger.info("connected to live feed %s", self.websocket_url)
                async for message in ws:
                    raw: Any
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")
                    try:
                        raw = json.loads(message)
                    except json.JSONDecodeError as err:
                        self._to_dlq(message, f"invalid json: {err}")
                        continue
                    frames = raw if isinstance(raw, list) else [raw]
                    for frame in frames:
                        event = self.publish_raw(frame, from_live=True)
                        if event is not None and isinstance(event, TradeTick):
                            produced += 1
                        if count is not None and produced >= count:
                            return

        asyncio.run(_consume())
        self._get_publisher().flush()
        return self.stats

    def close(self) -> None:
        if self.publisher is not None:
            try:
                self.publisher.flush()
            except Exception:  # noqa: BLE001
                logger.debug("publisher flush failed during close", exc_info=True)
            if self._owns_publisher:
                self.publisher.close()


def run_ingestion(
    *,
    source: str = "auto",
    websocket_url: str = "",
    count: int | None = None,
    publisher: EventPublisher | None = None,
    settings: Settings | None = None,
) -> IngestStats:
    service = IngestionService(
        settings=settings,
        publisher=publisher,
        source=source,
        websocket_url=websocket_url,
    )
    return service.run(count=count)
