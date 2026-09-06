"""Unit tests for lakehouse + Snowflake dual-write."""

from __future__ import annotations

from pathlib import Path

import pytest

from realtime_market_stream.config.settings import AppEnv, Settings
from realtime_market_stream.sinks.bronze import BronzeRecord, InMemoryBronzeSink
from realtime_market_stream.sinks.delta import build_bronze_sink
from realtime_market_stream.sinks.dual import DualWriteSink, DualWriteStats, maybe_wrap_dual_write
from realtime_market_stream.sinks.snowflake import InMemorySnowflakeChannel, SnowflakeStreamingSink


def _bronze(symbol: str = "AAPL") -> BronzeRecord:
    return BronzeRecord(
        event_type="trade",
        symbol=symbol,
        event_date="2026-09-06",
        ingested_at="2026-09-06T12:00:00+00:00",
        payload={"price": 190.5, "size": 10, "tick_id": f"{symbol}-1"},
    )


def test_dual_write_fans_out_to_both_sinks() -> None:
    lake = InMemoryBronzeSink()
    channel = InMemorySnowflakeChannel()
    sink: DualWriteSink[BronzeRecord] = DualWriteSink(
        primary=lake,
        secondary=SnowflakeStreamingSink(channel=channel),
    )
    paths = sink.write([_bronze("AAPL"), _bronze("MSFT")])
    assert len(lake.records) == 2
    assert channel.batches[0][0] == "TICKS"
    assert len(channel.batches[0][1]) == 2
    assert paths
    assert sink.stats.rows == 2
    assert sink.stats.secondary_writes == 1


def test_secondary_failure_is_soft_by_default() -> None:
    class _Boom:
        def insert_rows(self, table: str, rows: list[dict[str, object]]) -> str:
            raise RuntimeError("warehouse down")

    lake = InMemoryBronzeSink()
    sink = DualWriteSink(
        primary=lake,
        secondary=SnowflakeStreamingSink(channel=_Boom()),  # type: ignore[arg-type]
    )
    paths = sink.write([_bronze()])
    assert lake.records
    assert paths
    assert sink.stats.secondary_errors == 1


def test_secondary_failure_raises_when_strict() -> None:
    class _Boom:
        def insert_rows(self, table: str, rows: list[dict[str, object]]) -> str:
            raise RuntimeError("warehouse down")

    sink = DualWriteSink(
        primary=InMemoryBronzeSink(),
        secondary=SnowflakeStreamingSink(channel=_Boom()),  # type: ignore[arg-type]
        fail_soft=False,
    )
    with pytest.raises(RuntimeError, match="warehouse down"):
        sink.write([_bronze()])


def test_maybe_wrap_skips_when_disabled() -> None:
    settings = Settings(app_env=AppEnv.LOCAL)
    primary = InMemoryBronzeSink()
    assert maybe_wrap_dual_write(primary, settings) is primary


def test_maybe_wrap_when_flag_set(tmp_path: Path) -> None:
    settings = Settings(app_env=AppEnv.LOCAL, snowflake={"dual_write": True, "local_capture": True})
    wrapped = maybe_wrap_dual_write(InMemoryBronzeSink(), settings, local_root=tmp_path)
    assert isinstance(wrapped, DualWriteSink)
    paths = wrapped.write([_bronze()])
    assert any("snowflake" in path.replace("\\", "/") for path in paths)


def test_build_bronze_wraps_when_configured(tmp_path: Path) -> None:
    settings = Settings(
        app_env=AppEnv.SNOWFLAKE,
        snowflake={
            "account": "xy12345",
            "user": "demo",
            "warehouse": "COMPUTE_WH",
            "database": "MARKET",
            "schema_name": "PUBLIC",
            "local_capture": True,
        },
    )
    sink = build_bronze_sink(settings, local_root=tmp_path)
    assert isinstance(sink, DualWriteSink)
    sink.write([_bronze()])
    assert list((tmp_path / "snowflake").rglob("*.jsonl"))


def test_stats_default() -> None:
    assert DualWriteStats().secondary_errors == 0
