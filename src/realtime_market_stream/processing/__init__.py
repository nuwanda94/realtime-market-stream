"""Stream processors for Bronze, Silver, and Gold layers.

Heavy processors are loaded lazily so ``ingestion.service`` can import
``processing.backpressure`` without a circular import through bronze/silver.
"""

from __future__ import annotations

from typing import Any

from realtime_market_stream.processing.backpressure import (
    BackpressurePublisher,
    FlowController,
    FlowStats,
    InflightGate,
    TokenBucket,
    controller_from_settings,
)
from realtime_market_stream.processing.checkpoint import (
    ConsumedRecord,
    FileCheckpointStore,
    OffsetMap,
    ProcessorCheckpoint,
)

__all__ = [
    "BackpressurePublisher",
    "BronzeProcessor",
    "BronzeStats",
    "ConsumedRecord",
    "DlqReplayTool",
    "FileCheckpointStore",
    "FlowController",
    "FlowStats",
    "GoldProcessor",
    "GoldStats",
    "InflightGate",
    "OffsetMap",
    "ProcessorCheckpoint",
    "ReplayStats",
    "SilverProcessor",
    "SilverStats",
    "TokenBucket",
    "controller_from_settings",
    "run_bronze",
    "run_dlq_replay",
    "run_gold",
    "run_silver",
]

_LAZY: dict[str, tuple[str, str]] = {
    "BronzeProcessor": ("realtime_market_stream.processing.bronze", "BronzeProcessor"),
    "BronzeStats": ("realtime_market_stream.processing.bronze", "BronzeStats"),
    "run_bronze": ("realtime_market_stream.processing.bronze", "run_bronze"),
    "SilverProcessor": ("realtime_market_stream.processing.silver", "SilverProcessor"),
    "SilverStats": ("realtime_market_stream.processing.silver", "SilverStats"),
    "run_silver": ("realtime_market_stream.processing.silver", "run_silver"),
    "GoldProcessor": ("realtime_market_stream.processing.gold", "GoldProcessor"),
    "GoldStats": ("realtime_market_stream.processing.gold", "GoldStats"),
    "run_gold": ("realtime_market_stream.processing.gold", "run_gold"),
    "DlqReplayTool": ("realtime_market_stream.processing.dlq", "DlqReplayTool"),
    "ReplayStats": ("realtime_market_stream.processing.dlq", "ReplayStats"),
    "run_dlq_replay": ("realtime_market_stream.processing.dlq", "run_dlq_replay"),
}


def __getattr__(name: str) -> Any:
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = _LAZY[name]
    from importlib import import_module

    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value
