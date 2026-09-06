"""Unit tests for the DuckDB/Polars query layer (JSONL fallback, no extras)."""

from __future__ import annotations

from pathlib import Path

import pytest

from realtime_market_stream.serving.query import (
    QueryBackend,
    QueryEngine,
    _assert_readonly_sql,
    detect_backends,
    resolve_backend,
)
from realtime_market_stream.sinks.bronze import BronzeRecord, FilesystemBronzeSink
from realtime_market_stream.sinks.gold import FilesystemGoldSink, GoldRecord
from realtime_market_stream.sinks.silver import FilesystemSilverSink, SilverRecord


def _seed(root: Path) -> None:
    FilesystemBronzeSink(root).write(
        [
            BronzeRecord(
                event_type="trade",
                symbol="AAPL",
                event_date="2026-09-06",
                ingested_at="2026-09-06T13:30:00+00:00",
                payload={
                    "tick_id": "t1",
                    "symbol": "AAPL",
                    "ts": "2026-09-06T13:30:00+00:00",
                    "price": 190.5,
                    "size": 10,
                    "event_type": "trade",
                },
            )
        ]
    )
    FilesystemSilverSink(root).write(
        [
            SilverRecord(
                event_type="ohlcv",
                symbol="AAPL",
                event_date="2026-09-06",
                ingested_at="2026-09-06T13:31:00+00:00",
                payload={
                    "symbol": "AAPL",
                    "event_type": "ohlcv",
                    "window_start": "2026-09-06T13:30:00+00:00",
                    "close": 190.5,
                    "volume": 100,
                },
            )
        ]
    )
    FilesystemGoldSink(root).write(
        [
            GoldRecord(
                event_type="gold_bar",
                symbol="AAPL",
                event_date="2026-09-06",
                ingested_at="2026-09-06T13:31:05+00:00",
                payload={
                    "symbol": "AAPL",
                    "is_anomaly": True,
                    "window_start": "2026-09-06T13:30:00+00:00",
                },
            ),
            GoldRecord(
                event_type="gold_bar",
                symbol="MSFT",
                event_date="2026-09-06",
                ingested_at="2026-09-06T13:31:05+00:00",
                payload={
                    "symbol": "MSFT",
                    "is_anomaly": False,
                    "window_start": "2026-09-06T13:30:00+00:00",
                },
            ),
        ]
    )


def test_detect_backends_always_includes_jsonl() -> None:
    available = detect_backends()
    assert available["jsonl"] is True
    assert set(available) == {"jsonl", "duckdb", "polars"}


def test_resolve_backend_auto_falls_back() -> None:
    backend = resolve_backend("auto")
    assert backend in {QueryBackend.JSONL, QueryBackend.DUCKDB, QueryBackend.POLARS}
    assert resolve_backend("jsonl") is QueryBackend.JSONL


def test_jsonl_scans_seeded_lakehouse(tmp_path: Path) -> None:
    _seed(tmp_path)
    engine = QueryEngine(tmp_path, backend="jsonl")
    ticks = engine.latest_ticks(symbol="AAPL")
    assert len(ticks) == 1
    assert ticks[0]["price"] == 190.5
    ohlc = engine.ohlc_bars(symbol="AAPL")
    assert len(ohlc) == 1
    assert ohlc[0]["close"] == 190.5
    anomalies = engine.anomalies()
    assert len(anomalies) == 1
    assert anomalies[0]["symbol"] == "AAPL"


def test_list_views() -> None:
    engine = QueryEngine(".", backend="jsonl")
    names = {item["name"] for item in engine.list_views()}
    assert names == {"bronze_ticks", "silver_ticks", "silver_ohlc", "gold_bars", "gold_anomalies"}


def test_unknown_view(tmp_path: Path) -> None:
    engine = QueryEngine(tmp_path, backend="jsonl")
    with pytest.raises(KeyError):
        engine.scan("does_not_exist")


def test_readonly_sql_rejects_mutations() -> None:
    _assert_readonly_sql("SELECT 1")
    with pytest.raises(ValueError):
        _assert_readonly_sql("DROP TABLE bronze_ticks")
    with pytest.raises(ValueError):
        _assert_readonly_sql("INSERT INTO gold_bars SELECT * FROM gold_bars")
