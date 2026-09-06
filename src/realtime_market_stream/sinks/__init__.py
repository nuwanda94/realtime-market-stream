"""Lakehouse and warehouse sinks (Delta on MinIO, optional Snowflake)."""

from realtime_market_stream.sinks.bronze import (
    BronzeRecord,
    BronzeSink,
    FilesystemBronzeSink,
    InMemoryBronzeSink,
)
from realtime_market_stream.sinks.delta import (
    BronzeDeltaSink,
    BronzeIcebergSink,
    DeltaLakeSink,
    GoldDeltaSink,
    GoldIcebergSink,
    IcebergPartitionSink,
    SilverDeltaSink,
    SilverIcebergSink,
    build_bronze_sink,
    build_gold_sink,
    build_silver_sink,
    records_to_rows,
)
from realtime_market_stream.sinks.gold import (
    FilesystemGoldSink,
    GoldRecord,
    GoldSink,
    InMemoryGoldSink,
)
from realtime_market_stream.sinks.silver import (
    FilesystemSilverSink,
    InMemorySilverSink,
    SilverRecord,
    SilverSink,
)

__all__ = [
    "BronzeDeltaSink",
    "BronzeIcebergSink",
    "BronzeRecord",
    "BronzeSink",
    "DeltaLakeSink",
    "FilesystemBronzeSink",
    "FilesystemGoldSink",
    "FilesystemSilverSink",
    "GoldDeltaSink",
    "GoldIcebergSink",
    "GoldRecord",
    "GoldSink",
    "IcebergPartitionSink",
    "InMemoryBronzeSink",
    "InMemoryGoldSink",
    "InMemorySilverSink",
    "SilverDeltaSink",
    "SilverIcebergSink",
    "SilverRecord",
    "SilverSink",
    "build_bronze_sink",
    "build_gold_sink",
    "build_silver_sink",
    "records_to_rows",
]
