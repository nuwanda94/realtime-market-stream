#!/usr/bin/env python3
"""Render (and optionally apply) Snowflake setup SQL.

Default is dry-run: print templated SQL from infra/snowflake/*.sql using
values from pydantic-settings. No cloud calls.

    python scripts/setup_snowflake.py
    python scripts/setup_snowflake.py --apply   # live; needs credentials + extras
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from realtime_market_stream.config.settings import get_settings
from realtime_market_stream.sinks.snowflake import connect_kwargs_from_settings
from realtime_market_stream.sinks.snowflake_setup import apply_sql, render_setup_scripts


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sql-dir",
        default="",
        help="Override infra/snowflake directory.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute rendered SQL against Snowflake (not local-first).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    settings = get_settings()
    sql_dir = Path(args.sql_dir) if args.sql_dir else None
    scripts = render_setup_scripts(settings, sql_dir=sql_dir)
    if not scripts:
        print("no SQL files found", file=sys.stderr)
        return 1

    combined: list[str] = []
    for path, sql in scripts:
        print(f"-- ===== {path.name} =====")
        print(sql.rstrip())
        print()
        combined.append(sql)

    if not args.apply:
        print("dry-run complete (pass --apply to execute against Snowflake)")
        return 0

    if settings.snowflake.local_capture and not settings.snowflake.is_configured:
        print(
            "refusing --apply: SNOWFLAKE_LOCAL_CAPTURE=true and connection "
            "fields are empty. Set credentials and SNOWFLAKE_LOCAL_CAPTURE=false.",
            file=sys.stderr,
        )
        return 2

    kwargs = connect_kwargs_from_settings(settings)
    executed = apply_sql("\n".join(combined), kwargs)
    print(f"applied statements={executed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
