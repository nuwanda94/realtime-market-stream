"""Lakehouse and warehouse sinks (Delta on MinIO, optional Snowflake)."""

from realtime_market_stream.sinks.bronze import (
    BronzeRecord,
    BronzeSink,
    FilesystemBronzeSink,
    InMemoryBronzeSink,
)
from realtime_market_stream.sinks.dual import DualWriteSink, DualWriteStats, maybe_wrap_dual_write
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
from realtime_market_stream.sinks.snowflake import (
    ConnectorSnowflakeChannel,
    InMemorySnowflakeChannel,
    LocalJsonlSnowflakeChannel,
    SnowflakeStreamingSink,
    build_snowflake_sink,
    records_to_snowflake_rows,
)

__all__ = [
    "BronzeDeltaSink",
    "BronzeIcebergSink",
    "BronzeRecord",
    "BronzeSink",
    "ConnectorSnowflakeChannel",
    "DeltaLakeSink",
    "DualWriteSink",
    "DualWriteStats",
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
    "InMemorySnowflakeChannel",
    "LocalJsonlSnowflakeChannel",
    "SilverDeltaSink",
    "SilverIcebergSink",
    "SilverRecord",
    "SilverSink",
    "SnowflakeStreamingSink",
    "build_bronze_sink",
    "build_gold_sink",
    "build_silver_sink",
    "build_snowflake_sink",
    "maybe_wrap_dual_write",
    "records_to_rows",
    "records_to_snowflake_rows",
]
