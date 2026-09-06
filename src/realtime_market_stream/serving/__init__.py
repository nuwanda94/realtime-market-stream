"""Serving APIs and query helpers over the lakehouse."""

from realtime_market_stream.serving.dashboard import (
    latency_ms,
    load_dashboard_payload,
    summarize_ticks,
)
from realtime_market_stream.serving.query import QueryEngine, detect_backends
from realtime_market_stream.serving.store import LakehouseStore, lakehouse_root

__all__ = [
    "LakehouseStore",
    "QueryEngine",
    "detect_backends",
    "lakehouse_root",
    "latency_ms",
    "load_dashboard_payload",
    "summarize_ticks",
]
