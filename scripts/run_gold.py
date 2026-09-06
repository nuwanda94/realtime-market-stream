#!/usr/bin/env python3
"""Run the Gold stream processor.

Usage:
    python scripts/run_gold.py --max-records 20 --lookback 20
    python scripts/run_gold.py --data-root ./data/market-lake
"""

from __future__ import annotations

import argparse
import logging
import sys

from realtime_market_stream.config.settings import get_settings
from realtime_market_stream.observability.tracing import configure_tracing, shutdown_tracing, start_span
from realtime_market_stream.processing.gold import run_gold
from realtime_market_stream.sinks.delta import build_gold_sink


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score enriched OHLCV bars and land Gold anomaly features."
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Stop after consuming N Kafka records. Default: run until the consumer idles.",
    )
    parser.add_argument("--lookback", type=int, default=20)
    parser.add_argument("--z-threshold", type=float, default=3.0)
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
    sink = build_gold_sink(settings, local_root=args.data_root or None)
    try:
        with start_span("processor.gold.run", lookback=args.lookback):
            stats = run_gold(
                max_records=args.max_records,
                lookback=args.lookback,
                z_threshold=args.z_threshold,
                sink=sink,
                settings=settings,
            )
    finally:
        shutdown_tracing()
    print(
        "consumed={c} scored={s} anomalies={a} written={w} alerts={p} dlq={q}".format(
            c=stats.consumed,
            s=stats.bars_scored,
            a=stats.anomalies,
            w=stats.written,
            p=stats.published,
            q=stats.dlq,
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
