#!/usr/bin/env python3
"""Run the ingestion service (synthetic by default, optional websocket).

Usage:
    python scripts/run_ingestion.py --source synthetic --count 20
    python scripts/run_ingestion.py --source auto --ws-url ws://localhost:8765/ticks
"""

from __future__ import annotations

import argparse
import logging
import sys

from realtime_market_stream.config.settings import get_settings
from realtime_market_stream.ingestion.service import run_ingestion
from realtime_market_stream.observability.tracing import configure_tracing, shutdown_tracing, start_span


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest market ticks into Redpanda.")
    parser.add_argument(
        "--source",
        choices=("auto", "synthetic", "websocket"),
        default="auto",
        help="auto tries websocket then falls back to synthetic.",
    )
    parser.add_argument(
        "--ws-url",
        default="",
        help="Live websocket URL (optional). Empty disables live source.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Stop after N trade ticks. Default: run until interrupted.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    configure_tracing(get_settings())
    try:
        with start_span("ingestion.run", source=args.source):
            stats = run_ingestion(source=args.source, websocket_url=args.ws_url, count=args.count)
    finally:
        shutdown_tracing()
    print(
        f"published={stats.published} dlq={stats.dlq} "
        f"live={stats.source_live} synthetic={stats.source_synthetic}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
