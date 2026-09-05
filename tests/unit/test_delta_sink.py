"""Unit tests for the Delta / Iceberg lakehouse sink."""

from __future__ import annotations

from pathlib import Path

import pytest

from realtime_market_stream.config.settings import LakehouseFormat, LakehouseSettings, Settings
from realtime_market_stream.sinks.bronze import BronzeRecord, FilesystemBronzeSink
from realtime_market_stream.sinks.delta import (
    IcebergPartitionSink,
    build_bronze_sink,
    build_silver_sink,
    records_to_rows,
    table_path,
)


def _bronze(*symbols: str) -> list[BronzeRecord]:
    return [
        BronzeRecord(
            event_type="trade",
            symbol=symbol,
            event_date="2026-09-05",
            ingested_at="2026-09-05T17:00:00+00:00",
            payload={"price": 100.0 + i, "size": 10, "tick_id": f"t-{i}"},
        )
        for i, symbol in enumerate(symbols)
    ]


def test_records_to_rows_adds_partition_columns() -> None:
    rows = records_to_rows(_bronze("AAPL", "MSFT"))
    assert len(rows) == 2
    assert rows[0]["symbol"] == "AAPL"
    assert rows[0]["date"] == "2026-09-05"
    assert rows[0]["price"] == 100.0
    assert rows[1]["symbol"] == "MSFT"


def test_table_path_joins_layer() -> None:
    assert table_path("data/market-lake", "bronze") == "data/market-lake/bronze/ticks"
    assert table_path("s3://market-lake", "silver", "bars") == "s3://market-lake/silver/bars"


def test_build_bronze_defaults_to_jsonl() -> None:
    settings = Settings()
    assert settings.lakehouse.format is LakehouseFormat.JSONL
    sink = build_bronze_sink(settings, local_root="/tmp/lake-test")
    assert isinstance(sink, FilesystemBronzeSink)


def test_build_bronze_delta_adapter() -> None:
    settings = Settings(lakehouse=LakehouseSettings(format=LakehouseFormat.DELTA))
    sink = build_bronze_sink(settings, local_root="/tmp/lake-delta")
    assert sink.inner.table_uri.endswith("bronze/ticks")


def test_iceberg_partitioned_write(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    sink = IcebergPartitionSink(table_uri=str(tmp_path / "bronze" / "ticks"))
    written = sink.write(_bronze("AAPL", "AAPL", "MSFT"))
    assert written
    aapl = tmp_path / "bronze" / "ticks" / "symbol=AAPL" / "date=2026-09-05"
    msft = tmp_path / "bronze" / "ticks" / "symbol=MSFT" / "date=2026-09-05"
    assert aapl.is_dir()
    assert msft.is_dir()
    assert list(aapl.glob("*.parquet"))
    assert list((tmp_path / "bronze" / "ticks" / "_iceberg").glob("snap-*.json"))


def test_delta_write_if_engine_present(tmp_path: Path) -> None:
    deltalake = pytest.importorskip("deltalake")
    from realtime_market_stream.sinks.delta import DeltaLakeSink

    uri = str(tmp_path / "bronze" / "ticks")
    sink = DeltaLakeSink(table_uri=uri)
    written = sink.write(_bronze("NVDA"))
    assert written == [uri]
    table = deltalake.DeltaTable(uri)
    assert "symbol" in table.metadata().partition_columns
    assert "date" in table.metadata().partition_columns


def test_build_silver_iceberg() -> None:
    settings = Settings(lakehouse=LakehouseSettings(format=LakehouseFormat.ICEBERG))
    sink = build_silver_sink(settings, local_root="/tmp/lake-ice")
    assert sink.inner.table_uri.endswith("silver/ticks")
