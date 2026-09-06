"""Unit tests for pydantic-settings config and profile validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from realtime_market_stream.config import (
    AppEnv,
    Settings,
    clear_settings_cache,
    get_settings,
)


def test_local_defaults() -> None:
    settings = Settings(app_env=AppEnv.LOCAL)
    assert settings.app_env is AppEnv.LOCAL
    assert settings.kafka.bootstrap_servers == "localhost:19092"
    assert settings.kafka.topic_raw_ticks == "raw-ticks"
    assert settings.minio.bucket == "market-lake"
    assert settings.generator.tick_symbols == ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"]
    assert settings.generator.tick_rate_per_sec == 50
    assert settings.snowflake_dual_write_enabled is False
    assert settings.otel.enabled is False
    assert settings.otel.service_name == "realtime-market-stream"
    assert settings.otel.exporter == "console"


def test_tick_symbols_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TICK_SYMBOLS", "aapl, msft ,tsla")
    settings = Settings(_env_file=None)
    assert settings.generator.tick_symbols == ["AAPL", "MSFT", "TSLA"]


def test_snowflake_profile_requires_connection_fields() -> None:
    with pytest.raises(ValidationError, match="APP_ENV=snowflake"):
        Settings(app_env=AppEnv.SNOWFLAKE)


def test_snowflake_profile_accepts_complete_config() -> None:
    settings = Settings(
        app_env=AppEnv.SNOWFLAKE,
        snowflake={
            "account": "xy12345",
            "user": "demo",
            "warehouse": "COMPUTE_WH",
            "database": "MARKET",
            "schema_name": "PUBLIC",
        },
    )
    assert settings.snowflake.is_configured is True
    assert settings.snowflake_dual_write_enabled is True
    assert settings.snowflake.schema_name == "PUBLIC"


def test_get_settings_is_cached() -> None:
    clear_settings_cache()
    first = get_settings()
    second = get_settings()
    assert first is second
    clear_settings_cache()


def test_dual_write_flag_enables_property() -> None:
    settings = Settings(app_env=AppEnv.LOCAL, snowflake={"dual_write": True})
    assert settings.snowflake.dual_write is True
    assert settings.snowflake_dual_write_enabled is True
