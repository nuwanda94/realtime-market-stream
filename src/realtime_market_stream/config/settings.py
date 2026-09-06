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


class LakehouseFormat(StrEnum):
    """On-disk table format for Bronze/Silver/Gold."""

    JSONL = "jsonl"
    DELTA = "delta"
    ICEBERG = "iceberg"


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


class LakehouseSettings(BaseSettings):
    """Medallion lakehouse writer settings.

    ``format=jsonl`` is the zero-dependency default used by tests and CI.
    Set ``LAKEHOUSE_FORMAT=delta`` (or ``iceberg``) after installing
    ``pip install -e '.[lakehouse]'``. Partitioning is always
    ``symbol`` + ``date``.
    """

    model_config = SettingsConfigDict(env_prefix="LAKEHOUSE_", extra="ignore")

    format: LakehouseFormat = Field(default=LakehouseFormat.JSONL)
    uri: str = Field(
        default="",
        description=(
            "Table root. Empty uses data/{MINIO_BUCKET}. "
            "Use s3://market-lake to write through MinIO with the Delta engine."
        ),
    )

    @field_validator("format", mode="before")
    @classmethod
    def _lower_format(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower()
        return value


class CheckpointSettings(BaseSettings):
    """Local processor checkpoint directory (offsets + Silver state)."""

    model_config = SettingsConfigDict(env_prefix="CHECKPOINT_", extra="ignore")

    dir: str = Field(
        default=".checkpoints",
        description="Directory for atomic JSON checkpoints (gitignored).",
    )


class SnowflakeSettings(BaseSettings):
    """Optional Snowflake dual-write destination.

    All connection fields default to empty. When ``APP_ENV=snowflake`` the
    parent ``Settings`` model requires account, user, warehouse, database,
    and schema. Password may still be supplied via a secret manager later;
    it is optional here so key-pair auth remains possible.

    ``local_capture`` (default True) writes Snowpipe-Streaming-shaped JSONL
    batches locally so the sink works with zero cloud cost.

    ``dual_write`` fans each Bronze/Silver/Gold batch to the lakehouse and
    Snowflake. It is also implied by ``APP_ENV=snowflake`` or a fully
    configured connection.
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
    role: str = Field(default="")
    table_ticks: str = Field(default="TICKS")
    table_bars: str = Field(default="BARS")
    private_key_path: str = Field(
        default="",
        description="Optional path to a PKCS8 private key for key-pair auth.",
    )
    channel_name: str = Field(default="market-stream")
    batch_size: int = Field(default=500, ge=1, le=100_000)
    local_capture: bool = Field(
        default=True,
        description="Write JSONL batches locally instead of opening a Snowflake session.",
    )
    dual_write: bool = Field(
        default=False,
        description="Fan each processor batch to lakehouse + Snowflake.",
    )

    @field_validator("local_capture", "dual_write", mode="before")
    @classmethod
    def _parse_bool(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return value

    @property
    def is_configured(self) -> bool:
        """True when the minimum connection identifiers are present."""
        return bool(
            self.account and self.user and self.warehouse and self.database and self.schema_name
        )


class GeneratorSettings(BaseSettings):
    """Synthetic tick generator knobs."""

    model_config = SettingsConfigDict(extra="ignore")

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


class OtelSettings(BaseSettings):
    """Optional OpenTelemetry tracing (disabled by default, zero extra cost)."""

    model_config = SettingsConfigDict(env_prefix="OTEL_", extra="ignore")

    enabled: bool = Field(default=False)
    service_name: str = Field(default="realtime-market-stream")
    exporter: str = Field(
        default="console",
        description="console | otlp | none. otlp needs OTEL_EXPORTER_OTLP_ENDPOINT.",
    )
    endpoint: str = Field(
        default="",
        validation_alias=AliasChoices("OTEL_EXPORTER_OTLP_ENDPOINT", "endpoint"),
        description="OTLP HTTP endpoint, e.g. http://localhost:4318/v1/traces",
    )
    sample_ratio: float = Field(default=1.0, ge=0.0, le=1.0)

    @field_validator("exporter", mode="before")
    @classmethod
    def _lower_exporter(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator("enabled", mode="before")
    @classmethod
    def _parse_enabled(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return value


class FlowControlSettings(BaseSettings):
    """Ingest rate limits and producer backpressure (local-first)."""

    model_config = SettingsConfigDict(env_prefix="FLOW_", extra="ignore")

    enabled: bool = Field(default=True)
    ingest_max_per_sec: float = Field(default=0.0, ge=0.0)
    ingest_burst: float = Field(default=100.0, ge=1.0)
    producer_max_inflight: int = Field(default=500, ge=0)
    producer_block_timeout_sec: float = Field(default=5.0, ge=0.0)
    api_requests_per_sec: float = Field(default=50.0, ge=0.0)
    api_burst: float = Field(default=100.0, ge=1.0)

    @field_validator("enabled", mode="before")
    @classmethod
    def _parse_enabled(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return value


class Settings(BaseSettings):
    """Root settings object."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_env: AppEnv = Field(default=AppEnv.LOCAL)
    log_level: str = Field(default="INFO")
    schema_registry_url: str = Field(default="")

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    minio: MinioSettings = Field(default_factory=MinioSettings)
    lakehouse: LakehouseSettings = Field(default_factory=LakehouseSettings)
    checkpoint: CheckpointSettings = Field(default_factory=CheckpointSettings)
    snowflake: SnowflakeSettings = Field(default_factory=SnowflakeSettings)
    generator: GeneratorSettings = Field(default_factory=GeneratorSettings)
    otel: OtelSettings = Field(default_factory=OtelSettings)
    flow: FlowControlSettings = Field(default_factory=FlowControlSettings)

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
        return (
            self.app_env is AppEnv.SNOWFLAKE
            or self.snowflake.dual_write
            or self.snowflake.is_configured
        )


def get_settings() -> Settings:
    return _cached_settings()


@lru_cache(maxsize=1)
def _cached_settings() -> Settings:
    return Settings()


def clear_settings_cache() -> None:
    _cached_settings.cache_clear()
