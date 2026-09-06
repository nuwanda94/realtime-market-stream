"""CLI for the lakehouse query layer.

Examples::

    python scripts/run_query.py --view silver_ohlc --symbol AAPL --limit 10
    python scripts/run_query.py --sql "SELECT symbol, count(*) AS n FROM bronze_ticks GROUP BY 1"
    make query VIEW=gold_anomalies SYMBOL=AAPL
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from realtime_market_stream.config import get_settings
from realtime_market_stream.serving.query import QueryEngine, detect_backends


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Query Bronze/Silver/Gold lakehouse views")
    parser.add_argument("--view", default="silver_ohlc", help="Named view (see --list-views)")
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--sql", default=None, help="Read-only DuckDB SQL (requires duckdb)")
    parser.add_argument("--backend", default="auto", help="auto | jsonl | duckdb | polars")
    parser.add_argument("--data-root", default=None)
    parser.add_argument("--list-views", action="store_true")
    parser.add_argument("--list-backends", action="store_true")
    args = parser.parse_args(argv)

    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    engine = QueryEngine.from_settings(settings, local_root=args.data_root, backend=args.backend)

    if args.list_backends:
        print(json.dumps({"requested": args.backend, "available": detect_backends()}, indent=2))
        return 0
    if args.list_views:
        payload: dict[str, Any] = {
            "backend": str(engine.backend),
            "root": str(engine.root),
            "views": engine.list_views(),
        }
        print(json.dumps(payload, indent=2, default=str))
        return 0

    try:
        if args.sql:
            rows = engine.sql(args.sql)
        else:
            rows = engine.scan(args.view, symbol=args.symbol, limit=args.limit)
    except (KeyError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps({"backend": str(engine.backend), "count": len(rows), "items": rows}, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
