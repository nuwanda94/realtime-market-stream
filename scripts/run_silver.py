#!/usr/bin/env python3
"""Run the Silver stream processor.

Usage:
    python scripts/run_silver.py --max-records 20 --window-seconds 60
    python scripts/run_silver.py --data-root ./data/market-lake
"""

from __future__ import annotations

import argparse
import logging
import sys

from realtime_market_stream.config.settings import get_settings
from realtime_market_stream.observability.tracing import configure_tracing, shutdown_tracing, start_span
from realtime_market_stream.processing.silver import run_silver
from realtime_market_stream.sinks.delta import build_silver_sink


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dedup, enrich, and window raw-ticks into the Silver layer."
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Stop after consuming N Kafka records. Default: run until the consumer idles.",
    )
    parser.add_argument("--window-seconds", type=int, default=60)
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
    configure_tracing(settings)
    sink = build_silver_sink(settings, local_root=args.data_root or None)
    try:
        with start_span("processor.silver.run", window_seconds=args.window_seconds):
            stats = run_silver(
                max_records=args.max_records,
                window_seconds=args.window_seconds,
                sink=sink,
                settings=settings,
            )
    finally:
        shutdown_tracing()
    print(
        "consumed={c} trades={t} dups={d} bars={b} written={w} published={p} dlq={q}".format(
            c=stats.consumed,
            t=stats.trades_accepted,
            d=stats.duplicates,
            b=stats.bars_closed,
            w=stats.written,
            p=stats.published,
            q=stats.dlq,
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
