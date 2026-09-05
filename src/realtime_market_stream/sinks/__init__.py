"""Lakehouse and warehouse sinks (Delta on MinIO, optional Snowflake)."""

from realtime_market_stream.sinks.bronze import (
    BronzeRecord,
    BronzeSink,
    FilesystemBronzeSink,
    InMemoryBronzeSink,
)
from realtime_market_stream.sinks.silver import (
    FilesystemSilverSink,
    InMemorySilverSink,
    SilverRecord,
    SilverSink,
)

__all__ = [
    "BronzeRecord",
    "BronzeSink",
    "FilesystemBronzeSink",
    "FilesystemSilverSink",
    "InMemoryBronzeSink",
    "InMemorySilverSink",
    "SilverRecord",
    "SilverSink",
]
