"""Unit tests for the optional Snowflake Streaming sink."""

from __future__ import annotations

from pathlib import Path

import pytest

from realtime_market_stream.config.settings import AppEnv, Settings
from realtime_market_stream.sinks.bronze import BronzeRecord
from realtime_market_stream.sinks.gold import GoldRecord
from realtime_market_stream.sinks.snowflake import (
    ConnectorSnowflakeChannel,
    InMemorySnowflakeChannel,
    SnowflakeStreamingSink,
    build_snowflake_sink,
    connect_kwargs_from_settings,
    records_to_snowflake_rows,
)


def _bronze(symbol: str = "AAPL") -> BronzeRecord:
    return BronzeRecord(
        event_type="trade",
        symbol=symbol,
        event_date="2026-09-06",
        ingested_at="2026-09-06T12:00:00+00:00",
        payload={"price": 190.5, "size": 10, "tick_id": f"{symbol}-1"},
    )


def _gold(symbol: str = "AAPL") -> GoldRecord:
    return GoldRecord(
        event_type="ohlcv",
        symbol=symbol,
        event_date="2026-09-06",
        ingested_at="2026-09-06T12:01:00+00:00",
        payload={"close": 191.0, "z_score": 0.4},
    )


def test_records_flatten_payload() -> None:
    rows = records_to_snowflake_rows([_bronze()])
    assert rows[0]["symbol"] == "AAPL"
    assert rows[0]["price"] == 190.5
    assert rows[0]["payload"]["tick_id"] == "AAPL-1"


def test_in_memory_channel_batches() -> None:
    channel = InMemorySnowflakeChannel()
    sink = SnowflakeStreamingSink(channel=channel, batch_size=2)
    written = sink.write([_bronze("AAPL"), _bronze("MSFT"), _bronze("GOOG")])
    assert len(written) == 2
    assert channel.batches[0][0] == "TICKS"
    assert len(channel.batches[0][1]) == 2
    assert len(channel.batches[1][1]) == 1


def test_gold_records_use_bars_table() -> None:
    channel = InMemorySnowflakeChannel()
    sink = SnowflakeStreamingSink(channel=channel, table_bars="GOLD_BARS")
    sink.write([_gold()])
    assert channel.batches[0][0] == "GOLD_BARS"


def test_local_capture_writes_jsonl(tmp_path: Path) -> None:
    settings = Settings(
        app_env=AppEnv.LOCAL,
        snowflake={
            "local_capture": True,
            "database": "MARKET",
            "schema_name": "STREAM",
            "table_ticks": "RAW_TICKS",
        },
    )
    sink = SnowflakeStreamingSink.from_settings(settings, local_root=tmp_path)
    paths = sink.write([_bronze()])
    assert len(paths) == 1
    out = Path(paths[0])
    assert out.is_file()
    assert "MARKET/STREAM/RAW_TICKS" in str(out).replace("\\", "/")
    line = out.read_text(encoding="utf-8").strip()
    assert "AAPL" in line


def test_build_sink_none_when_disabled() -> None:
    settings = Settings(app_env=AppEnv.LOCAL, snowflake={"local_capture": False})
    assert build_snowflake_sink(settings) is None


def test_build_sink_force_local(tmp_path: Path) -> None:
    settings = Settings(app_env=AppEnv.LOCAL, snowflake={"local_capture": True})
    sink = build_snowflake_sink(settings, local_root=tmp_path, force=True)
    assert sink is not None
    paths = sink.write([_bronze()])
    assert Path(paths[0]).is_file()


def test_live_channel_requires_config() -> None:
    settings = Settings(app_env=AppEnv.LOCAL, snowflake={"local_capture": False})
    with pytest.raises(ValueError, match="SNOWFLAKE_"):
        SnowflakeStreamingSink.from_settings(settings)


def test_connect_kwargs_omit_empty_password() -> None:
    settings = Settings(
        app_env=AppEnv.SNOWFLAKE,
        snowflake={
            "account": "xy12345",
            "user": "demo",
            "warehouse": "COMPUTE_WH",
            "database": "MARKET",
            "schema_name": "PUBLIC",
            "role": "SYSADMIN",
            "private_key_path": "/secrets/snowflake.p8",
        },
    )
    kwargs = connect_kwargs_from_settings(settings)
    assert "password" not in kwargs
    assert kwargs["role"] == "SYSADMIN"
    assert kwargs["private_key_file"] == "/secrets/snowflake.p8"


def test_connector_channel_uses_injected_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[tuple[str, list[tuple[object, ...]]]] = []

    class _Cursor:
        def executemany(self, sql: str, values: list[tuple[object, ...]]) -> None:
            executed.append((sql, values))

        def __enter__(self) -> _Cursor:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    class _Conn:
        def cursor(self) -> _Cursor:
            return _Cursor()

        def __enter__(self) -> _Conn:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    class _Connector:
        @staticmethod
        def connect(**kwargs: object) -> _Conn:
            assert kwargs["account"] == "xy12345"
            return _Conn()

    import realtime_market_stream.sinks.snowflake as module

    monkeypatch.setattr(module, "_import_snowflake_connector", lambda: _Connector)
    channel = ConnectorSnowflakeChannel(
        connect_kwargs={"account": "xy12345"},
        database="MARKET",
        schema_name="PUBLIC",
    )
    token = channel.insert_rows("TICKS", [{"symbol": "AAPL", "payload": {"x": 1}}])
    assert token.startswith("snowflake://MARKET.PUBLIC.TICKS")
    assert executed
    assert "INSERT INTO MARKET.PUBLIC.TICKS" in executed[0][0]
