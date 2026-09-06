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
from realtime_market_stream.schemas.ticks import OhlcvBar, Side, TradeTick
from realtime_market_stream.sinks.delta import build_silver_sink
from realtime_market_stream.sinks.silver import SilverRecord, SilverSink

logger = logging.getLogger(__name__)
