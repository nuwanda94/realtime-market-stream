"""Ingestion service: live websocket client with synthetic fallback.

Publishes schema-validated ticks to the raw-ticks topic. Payloads that fail
validation are routed to the DLQ. The default source is synthetic so the stack
runs with zero external cost.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from pydantic import ValidationError

from realtime_market_stream.config.settings import Settings, get_settings
from realtime_market_stream.ingestion.generator import SyntheticTickGenerator
from realtime_market_stream.schemas.ticks import OhlcvBar, TradeTick

logger = logging.getLogger(__name__)

MarketEvent = TradeTick | OhlcvBar


class EventPublisher(Protocol):
    """Minimal publisher so tests can inject a fake."""

    def send(self, topic: str, key: bytes | None, value: bytes) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...
