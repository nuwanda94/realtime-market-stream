"""Optional Snowflake Streaming sink.

Local-first design:

* Default path never talks to the cloud. When Snowflake is not configured the
  factory returns ``None``.
* ``SNOWFLAKE_LOCAL_CAPTURE=true`` (default) writes JSONL under
  ``data/snowflake/{database}/{schema}/{table}/`` so laptops and CI can
  exercise the same batching API as Snowpipe Streaming.
* When ``SNOWFLAKE_LOCAL_CAPTURE=false`` and credentials are present, rows are
  flushed through ``snowflake-connector-python`` INSERT batches — the cheap
  stand-in for Snowpipe Streaming / Kafka connector ingest. Install extras:
  ``pip install -e '.[snowflake]'``.

Dual-write wiring (lakehouse + this sink) is a separate Phase 3 task.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol
from uuid import uuid4

from realtime_market_stream.config.settings import Settings
from realtime_market_stream.sinks.bronze import BronzeRecord
from realtime_market_stream.sinks.gold import GoldRecord
from realtime_market_stream.sinks.silver import SilverRecord

logger = logging.getLogger(__name__)

SnowflakeRecord = BronzeRecord | SilverRecord | GoldRecord


class SnowflakeChannel(Protocol):
    """Flush one batched insert to a destination table."""

    def insert_rows(self, table: str, rows: list[dict[str, Any]]) -> str: ...


def records_to_snowflake_rows(records: list[SnowflakeRecord]) -> list[dict[str, Any]]:
    """Flatten medallion records into VARIANT-friendly warehouse rows."""
    rows: list[dict[str, Any]] = []
    for record in records:
        payload = dict(record.payload)
        row: dict[str, Any] = {
            "event_type": record.event_type,
            "symbol": record.symbol,
            "event_date": record.event_date,
            "ingested_at": record.ingested_at,
            "payload": payload,
        }
        for key, value in payload.items():
            if key not in row:
                row[key] = value
        rows.append(row)
    return rows


@dataclass
class InMemorySnowflakeChannel:
    """Test double that keeps inserted batches in process memory."""

    batches: list[tuple[str, list[dict[str, Any]]]] = field(default_factory=list)

    def insert_rows(self, table: str, rows: list[dict[str, Any]]) -> str:
        self.batches.append((table, list(rows)))
        return f"memory://{table}/{len(self.batches)}"


@dataclass
class LocalJsonlSnowflakeChannel:
    """Zero-cloud capture of the same row batches a Streaming channel would send."""

    root: Path
    database: str
    schema_name: str

    def insert_rows(self, table: str, rows: list[dict[str, Any]]) -> str:
        directory = self.root / self.database / self.schema_name / table
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"part-{uuid4().hex[:12]}.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, separators=(",", ":"), default=str))
                handle.write("\n")
        logger.info("snowflake local capture %s rows -> %s", len(rows), path)
        return str(path)


@dataclass
class ConnectorSnowflakeChannel:
    """INSERT-batch channel backed by snowflake-connector-python.

    This is the Kafka-connector / Snowpipe Streaming *path* without pulling in
    the proprietary Streaming SDK (not available as a zero-cost extra). Rows
    are bound as a single parameterized multi-row INSERT.
    """

    connect_kwargs: Mapping[str, Any]
    database: str
    schema_name: str

    def insert_rows(self, table: str, rows: list[dict[str, Any]]) -> str:
        connector = _import_snowflake_connector()
        qualified = f"{self.database}.{self.schema_name}.{table}"
        columns = _union_columns(rows)
        placeholders = ", ".join(["%s"] * len(columns))
        col_sql = ", ".join(f'"{col.upper()}"' for col in columns)
        sql = f"INSERT INTO {qualified} ({col_sql}) VALUES ({placeholders})"
        values = [tuple(_bind_value(row.get(col)) for col in columns) for row in rows]
        with connector.connect(**dict(self.connect_kwargs)) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(sql, values)
        logger.info("snowflake insert %s rows -> %s", len(rows), qualified)
        return f"snowflake://{qualified}"


def _union_columns(rows: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                columns.append(key)
    return columns


def _bind_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return value


def _import_snowflake_connector() -> Any:
    try:
        import snowflake.connector as connector
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Live Snowflake sink requires snowflake-connector-python. "
            "Install extras: pip install -e '.[snowflake]'"
        ) from exc
    return connector


def connect_kwargs_from_settings(settings: Settings) -> dict[str, Any]:
    """Build connector kwargs from settings. Never logs secrets."""
    sf = settings.snowflake
    kwargs: dict[str, Any] = {
        "account": sf.account,
        "user": sf.user,
        "warehouse": sf.warehouse,
        "database": sf.database,
        "schema": sf.schema_name,
    }
    password = sf.password.get_secret_value()
    if password:
        kwargs["password"] = password
    if sf.role:
        kwargs["role"] = sf.role
    if sf.private_key_path:
        kwargs["private_key_file"] = sf.private_key_path
    return kwargs


@dataclass
class SnowflakeStreamingSink:
    """Batch records and flush them through a Snowflake channel.

    Mimics Snowpipe Streaming channel semantics: open a named channel per
    destination table, append rows, return a flush token (URI / path).
    """

    channel: SnowflakeChannel
    table_ticks: str = "TICKS"
    table_bars: str = "BARS"
    batch_size: int = 500
    channel_name: str = "market-stream"

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        channel: SnowflakeChannel | None = None,
        local_root: str | Path | None = None,
    ) -> SnowflakeStreamingSink:
        sf = settings.snowflake
        if channel is None:
            channel = _channel_from_settings(settings, local_root=local_root)
        return cls(
            channel=channel,
            table_ticks=sf.table_ticks,
            table_bars=sf.table_bars,
            batch_size=sf.batch_size,
            channel_name=sf.channel_name,
        )

    def table_for(self, records: list[SnowflakeRecord]) -> str:
        if records and all(isinstance(record, GoldRecord) for record in records):
            return self.table_bars
        return self.table_ticks

    def write(self, records: list[SnowflakeRecord]) -> list[str]:
        if not records:
            return []
        table = self.table_for(records)
        rows = records_to_snowflake_rows(records)
        written: list[str] = []
        for start in range(0, len(rows), self.batch_size):
            chunk = rows[start : start + self.batch_size]
            token = self.channel.insert_rows(table, chunk)
            written.append(token)
        logger.info(
            "snowflake streaming flush channel=%s table=%s rows=%s batches=%s",
            self.channel_name,
            table,
            len(rows),
            len(written),
        )
        return written


def _channel_from_settings(
    settings: Settings, *, local_root: str | Path | None
) -> SnowflakeChannel:
    sf = settings.snowflake
    if sf.local_capture:
        root = Path(local_root) if local_root is not None else Path("data") / "snowflake"
        database = sf.database or "LOCAL"
        schema_name = sf.schema_name or "PUBLIC"
        return LocalJsonlSnowflakeChannel(
            root=root, database=database, schema_name=schema_name
        )
    if not sf.is_configured:
        raise ValueError(
            "Snowflake streaming sink needs SNOWFLAKE_* connection fields "
            "when SNOWFLAKE_LOCAL_CAPTURE=false."
        )
    return ConnectorSnowflakeChannel(
        connect_kwargs=connect_kwargs_from_settings(settings),
        database=sf.database,
        schema_name=sf.schema_name,
    )


def build_snowflake_sink(
    settings: Settings,
    *,
    local_root: str | Path | None = None,
    force: bool = False,
) -> SnowflakeStreamingSink | None:
    """Return a sink when Snowflake is in play, otherwise ``None``.

    Enabled when ``APP_ENV=snowflake``, connection fields are set, local
    capture is requested, or ``force=True`` (CLI dry-runs).
    """
    sf = settings.snowflake
    if not (
        force
        or settings.snowflake_dual_write_enabled
        or sf.local_capture
        or sf.is_configured
    ):
        return None
    return SnowflakeStreamingSink.from_settings(settings, local_root=local_root)
