#!/usr/bin/env python3
"""Run the Bronze stream processor.

Usage:
    python scripts/run_bronze.py --max-records 20 --batch-size 10
    python scripts/run_bronze.py --data-root ./data/market-lake
"""

from __future__ import annotations

import argparse
import logging
import sys

from realtime_market_stream.config.settings import get_settings
from realtime_market_stream.processing.bronze import run_bronze
from realtime_market_stream.sinks.delta import build_bronze_sink


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Land raw-ticks into the Bronze lakehouse layer.")
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Stop after consuming N Kafka records. Default: run until the consumer idles.",
    )
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument(
        "--data-root",
        default="",
        help="Local lakehouse root (default: data/{MINIO_BUCKET}).",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = get_settings()
    sink = build_bronze_sink(settings, local_root=args.data_root or None)
    stats = run_bronze(
        max_records=args.max_records,
        batch_size=args.batch_size,
        sink=sink,
        settings=settings,
    )
    print(
        f"consumed={stats.consumed} written={stats.written} dlq={stats.dlq} batches={stats.batches}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
