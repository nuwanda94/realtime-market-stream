"""Unit tests for Snowflake setup SQL rendering (no cloud)."""

from __future__ import annotations

from pathlib import Path

import pytest

from realtime_market_stream.config.settings import Settings, SnowflakeSettings
from realtime_market_stream.sinks.snowflake_setup import (
    SnowflakeSetupContext,
    render_setup_scripts,
)


def test_context_uses_settings_and_defaults() -> None:
    sf = SnowflakeSettings(
        account="acct1",
        user="svc",
        warehouse="WH_XS",
        database="MKT",
        schema_name="RAW",
        table_ticks="TICKS",
        table_bars="BARS",
        role="APP_ROLE",
    )
    ctx = SnowflakeSetupContext.from_snowflake(sf)
    assert ctx.values["DATABASE"] == "MKT"
    assert ctx.values["SCHEMA"] == "RAW"
    assert ctx.values["STAGE"] == "MARKET_STREAM_STAGE"
    rendered = ctx.render("USE DATABASE {{DATABASE}}; USE SCHEMA {{SCHEMA}};")
    assert rendered == "USE DATABASE MKT; USE SCHEMA RAW;"


def test_rejects_unsafe_identifiers() -> None:
    ctx = SnowflakeSetupContext(values={"DATABASE": "foo; DROP TABLE T"})
    with pytest.raises(ValueError, match="Unsafe"):
        ctx.render("USE DATABASE {{DATABASE}};")


def test_render_bundled_scripts() -> None:
    settings = Settings()
    scripts = render_setup_scripts(settings)
    names = [path.name for path, _ in scripts]
    assert names == [
        "00_roles.sql",
        "01_warehouse_database.sql",
        "02_tables.sql",
        "03_stage_pipe.sql",
    ]
    joined = "\n".join(sql for _, sql in scripts)
    assert "{{" not in joined
    assert "CREATE TABLE IF NOT EXISTS" in joined
    assert "CREATE PIPE IF NOT EXISTS" in joined
    assert "MARKET_STREAM" in joined


def test_render_custom_sql_dir(tmp_path: Path) -> None:
    sample = tmp_path / "00_demo.sql"
    sample.write_text("SELECT '{{DATABASE}}';\n", encoding="utf-8")
    settings = Settings(
        snowflake=SnowflakeSettings(database="CUSTOM_DB"),
    )
    scripts = render_setup_scripts(settings, sql_dir=tmp_path)
    assert len(scripts) == 1
    assert scripts[0][1] == "SELECT 'CUSTOM_DB';\n"
