"""Render and optionally apply Snowflake warehouse setup scripts.

Local-first: the default path only prints templated SQL. Applying to a live
account requires SNOWFLAKE_* credentials and ``pip install -e '.[snowflake]'``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from realtime_market_stream.config.settings import Settings, SnowflakeSettings

PACKAGE_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SQL_DIR = PACKAGE_ROOT / "infra" / "snowflake"

# Conservative identifiers used when settings are empty (local dry-run).
DEFAULTS: dict[str, str] = {
    "ACCOUNT": "xy12345",
    "USER": "STREAM_USER",
    "ROLE": "MARKET_STREAM_ROLE",
    "WAREHOUSE": "MARKET_STREAM_WH",
    "DATABASE": "MARKET_STREAM",
    "SCHEMA": "PUBLIC",
    "TABLE_TICKS": "TICKS",
    "TABLE_BARS": "BARS",
    "STAGE": "MARKET_STREAM_STAGE",
    "PIPE": "MARKET_STREAM_PIPE",
    "INGEST_ROLE": "MARKET_STREAM_INGEST",
    "READER_ROLE": "MARKET_STREAM_READER",
}


@dataclass(frozen=True)
class SnowflakeSetupContext:
    """Substitution map for infra/snowflake/*.sql templates."""

    values: Mapping[str, str]

    @classmethod
    def from_settings(cls, settings: Settings) -> SnowflakeSetupContext:
        return cls.from_snowflake(settings.snowflake)

    @classmethod
    def from_snowflake(cls, sf: SnowflakeSettings) -> SnowflakeSetupContext:
        values = {
            "ACCOUNT": sf.account or DEFAULTS["ACCOUNT"],
            "USER": sf.user or DEFAULTS["USER"],
            "ROLE": sf.role or DEFAULTS["ROLE"],
            "WAREHOUSE": sf.warehouse or DEFAULTS["WAREHOUSE"],
            "DATABASE": sf.database or DEFAULTS["DATABASE"],
            "SCHEMA": sf.schema_name or DEFAULTS["SCHEMA"],
            "TABLE_TICKS": sf.table_ticks or DEFAULTS["TABLE_TICKS"],
            "TABLE_BARS": sf.table_bars or DEFAULTS["TABLE_BARS"],
            "STAGE": DEFAULTS["STAGE"],
            "PIPE": DEFAULTS["PIPE"],
            "INGEST_ROLE": DEFAULTS["INGEST_ROLE"],
            "READER_ROLE": DEFAULTS["READER_ROLE"],
        }
        return cls(values=values)

    def render(self, sql: str) -> str:
        rendered = sql
        for key, value in self.values.items():
            rendered = rendered.replace("{{" + key + "}}", _safe_ident(value))
        return rendered


def _safe_ident(value: str) -> str:
    """Allow only unquoted Snowflake identifier characters."""
    cleaned = value.strip()
    if not cleaned:
        raise ValueError("Snowflake identifier cannot be empty")
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_$")
    if any(ch not in allowed for ch in cleaned):
        raise ValueError(f"Unsafe Snowflake identifier: {value!r}")
    return cleaned


def list_sql_files(sql_dir: Path | None = None) -> list[Path]:
    root = sql_dir or DEFAULT_SQL_DIR
    return sorted(path for path in root.glob("*.sql") if path.is_file())


def render_setup_scripts(
    settings: Settings,
    *,
    sql_dir: Path | None = None,
) -> list[tuple[Path, str]]:
    """Return (path, rendered SQL) for every script under infra/snowflake."""
    ctx = SnowflakeSetupContext.from_settings(settings)
    rendered: list[tuple[Path, str]] = []
    for path in list_sql_files(sql_dir):
        raw = path.read_text(encoding="utf-8")
        rendered.append((path, ctx.render(raw)))
    return rendered


def apply_sql(
    statements: str,
    connect_kwargs: Mapping[str, Any],
) -> int:
    """Execute rendered SQL against Snowflake. Splits on semicolons."""
    try:
        import snowflake.connector as connector
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Live apply requires snowflake-connector-python. "
            "Install extras: pip install -e '.[snowflake]'"
        ) from exc

    chunks = [part.strip() for part in statements.split(";") if part.strip()]
    executed = 0
    with connector.connect(**dict(connect_kwargs)) as connection:
        with connection.cursor() as cursor:
            for chunk in chunks:
                cursor.execute(chunk)
                executed += 1
    return executed
