#!/usr/bin/env python3
"""Print synthetic ticks to stdout (JSONL). Does not require Redpanda.

Usage:
    python scripts/generate_ticks.py --count 20
    python scripts/generate_ticks.py --symbols AAPL,NVDA --rate 10 --count 50
"""

from __future__ import annotations

import argparse
import json
import sys

from realtime_market_stream.ingestion.generator import SyntheticTickGenerator
from realtime_market_stream.schemas.ticks import TradeTick


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Emit synthetic market ticks as JSONL.")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols (default: settings).")
    parser.add_argument("--rate", type=int, default=0, help="Aggregate ticks/sec (default: settings).")
    parser.add_argument("--count", type=int, default=10, help="Number of trade ticks to emit.")
    parser.add_argument("--seed", type=int, default=None, help="Optional RNG seed.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    kwargs: dict[str, object] = {"seed": args.seed}
    if args.symbols:
        kwargs["symbols"] = [part.strip() for part in args.symbols.split(",") if part.strip()]
    if args.rate > 0:
        kwargs["ticks_per_sec"] = args.rate
    generator = SyntheticTickGenerator(**kwargs)  # type: ignore[arg-type]

    emitted = 0
    target = max(1, args.count)
    while emitted < target:
        for event in generator.next_events():
            sys.stdout.write(json.dumps(event.to_kafka_value()) + "\n")
            if isinstance(event, TradeTick):
                emitted += 1
            if emitted >= target:
                break
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
