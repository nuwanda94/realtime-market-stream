"""Typed application settings loaded from environment / `.env`.

Two profiles are supported via ``APP_ENV``:

* ``local`` (default) — Redpanda + MinIO lakehouse; Snowflake is optional.
* ``snowflake`` — dual-write path; Snowflake connection fields are required.

Never put real secrets in source control. Use `.env` (gitignored) or the process
environment. Placeholders live in `.env.example`.
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from typing import Annotated, Self

from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class AppEnv(StrEnum):
    """Deployment / config profile."""

    LOCAL = "local"
    SNOWFLAKE = "snowflake"


class KafkaSettings(BaseSettings):
    """Redpanda / Kafka connection and topic names."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_", extra="ignore")

    bootstrap_servers: str = Field(
        default="localhost:19092",
        validation_alias=AliasChoices("KAFKA_BOOTSTRAP_SERVERS", "bootstrap_servers"),
    )
    topic_raw_ticks: str = Field(default="raw-ticks")
    topic_enriched_ticks: str = Field(default="enriched-ticks")
    topic_alerts: str = Field(default="alerts")
    topic_dlq: str = Field(default="dlq")


class MinioSettings(BaseSettings):
    """S3-compatible MinIO lakehouse endpoint."""

    model_config = SettingsConfigDict(env_prefix="MINIO_", extra="ignore")

    endpoint: str = Field(default="http://localhost:9000")
    access_key: str = Field(default="minioadmin")
    secret_key: SecretStr = Field(default=SecretStr("minioadmin"))
    bucket: str = Field(default="market-lake")


class SnowflakeSettings(BaseSettings):
    """Optional Snowflake dual-write destination.

    All fields default to empty. When ``APP_ENV=snowflake`` the parent
    ``Settings`` model requires account, user, warehouse, database, and schema.
    Password may still be supplied via a secret manager later; it is optional
    here so key-pair auth remains possible.
    """

    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE_", extra="ignore")

    account: str = Field(default="")
    user: str = Field(default="")
    password: SecretStr = Field(default=SecretStr(""))
    warehouse: str = Field(default="")
    database: str = Field(default="")
    schema_name: str = Field(
        default="",
        validation_alias=AliasChoices("SNOWFLAKE_SCHEMA", "schema_name"),
    )

    @property
    def is_configured(self) -> bool:
        """True when the minimum connection identifiers are present."""
        return bool(
            self.account and self.user and self.warehouse and self.database and self.schema_name
        )


class GeneratorSettings(BaseSettings):
    """Synthetic tick generator knobs."""

    model_config = SettingsConfigDict(extra="ignore")

    # NoDecode: env values are CSV strings (TICK_SYMBOLS=AAPL,MSFT), not JSON arrays.
    tick_symbols: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"]
    )
    tick_rate_per_sec: int = Field(default=50, ge=1, le=100_000)

    @field_validator("tick_symbols", mode="before")
    @classmethod
    def _split_symbols(cls, value: object) -> object:
        if isinstance(value, str):
            return [part.strip().upper() for part in value.split(",") if part.strip()]
        return value


class Settings(BaseSettings):
    """Root settings object.

    Nested sections are populated from the same env / `.env` file using their
    prefixes (`KAFKA_`, `MINIO_`, `SNOWFLAKE_`).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_env: AppEnv = Field(default=AppEnv.LOCAL)
    log_level: str = Field(default="INFO")

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    minio: MinioSettings = Field(default_factory=MinioSettings)
    snowflake: SnowflakeSettings = Field(default_factory=SnowflakeSettings)
    generator: GeneratorSettings = Field(default_factory=GeneratorSettings)

    @field_validator("log_level", mode="before")
    @classmethod
    def _upper_log_level(cls, value: object) -> object:
        if isinstance(value, str):
            return value.upper()
        return value

    @model_validator(mode="after")
    def _require_snowflake_when_profile_enabled(self) -> Self:
        if self.app_env is AppEnv.SNOWFLAKE and not self.snowflake.is_configured:
            raise ValueError(
                "APP_ENV=snowflake requires SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, "
                "SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, and SNOWFLAKE_SCHEMA."
            )
        return self

    @property
    def snowflake_dual_write_enabled(self) -> bool:
        """Local profile may still dual-write if Snowflake is fully configured."""
        return self.app_env is AppEnv.SNOWFLAKE or self.snowflake.is_configured


def get_settings() -> Settings:
    """Return a process-wide cached Settings instance.

    Tests should call :func:`clear_settings_cache` (or construct ``Settings``
    directly with overrides) so env mutations are visible.
    """
    return _cached_settings()


@lru_cache(maxsize=1)
def _cached_settings() -> Settings:
    return Settings()


def clear_settings_cache() -> None:
    """Drop the cached instance (used by tests)."""
    _cached_settings.cache_clear()
