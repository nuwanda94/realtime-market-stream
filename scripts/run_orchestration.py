#!/usr/bin/env python3
"""Run orchestration jobs without Airflow (local-first).

Usage:
    python scripts/run_orchestration.py backfill --count 10
    python scripts/run_orchestration.py schema
    python scripts/run_orchestration.py freshness --data-root ./data/market-lake
"""

from __future__ import annotations

import argparse
import json
import sys

from realtime_market_stream.orchestration.jobs import (
    result_as_dict,
    run_backfill,
    run_freshness_check,
    run_schema_evolution_check,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run RMS orchestration jobs locally.")
    sub = parser.add_subparsers(dest="command", required=True)

    backfill = sub.add_parser("backfill", help="Synthetic Bronze backfill")
    backfill.add_argument("--count", type=int, default=20)
    backfill.add_argument("--data-root", default="")
    backfill.add_argument("--seed", type=int, default=42)

    sub.add_parser("schema", help="Compare bundled JSON Schema to Pydantic models")

    freshness = sub.add_parser("freshness", help="Lakehouse freshness probe")
    freshness.add_argument("--view", default="bronze_ticks")
    freshness.add_argument("--data-root", default="")
    freshness.add_argument("--max-age-seconds", type=int, default=86_400)

    args = parser.parse_args(argv)
    if args.command == "backfill":
        result = run_backfill(
            count=args.count,
            data_root=args.data_root or None,
            seed=args.seed,
        )
    elif args.command == "schema":
        result = run_schema_evolution_check()
    else:
        result = run_freshness_check(
            view=args.view,
            data_root=args.data_root or None,
            max_age_seconds=args.max_age_seconds,
        )
    print(json.dumps(result_as_dict(result), indent=2))
    if hasattr(result, "ok") and not result.ok:
        return 1
    if hasattr(result, "stale") and result.stale:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
