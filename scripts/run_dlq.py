#!/usr/bin/env python3
"""Inspect or replay the market-data dead-letter queue.

Usage:
    python scripts/run_dlq.py --inspect --max-records 20
    python scripts/run_dlq.py --dry-run --max-records 20
    python scripts/run_dlq.py --max-records 20
    python scripts/run_dlq.py --error-contains "invalid json"
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from realtime_market_stream.config.settings import get_settings
from realtime_market_stream.processing.dlq import DlqReplayTool, is_replayable


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect or replay the DLQ topic.")
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Stop after consuming N DLQ records. Default: run until the consumer idles.",
    )
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Print envelopes as JSONL; do not republish.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Classify replayable records without producing to raw-ticks.",
    )
    parser.add_argument(
        "--error-contains",
        default="",
        help="Only consider envelopes whose error text contains this substring.",
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
    tool = DlqReplayTool(settings=settings)

    if args.inspect:
        records = tool.inspect(max_records=args.max_records, error_contains=args.error_contains)
        for record in records:
            line = {
                "error": record.error,
                "received_at": record.received_at,
                "replayable": is_replayable(record),
                "payload": record.payload,
            }
            print(json.dumps(line, default=str))
        stats = tool.stats
    else:
        stats = tool.replay(
            max_records=args.max_records,
            dry_run=args.dry_run,
            error_contains=args.error_contains,
        )

    print(
        (
            f"consumed={stats.consumed} replayed={stats.replayed} "
            f"skipped={stats.skipped} inspect_only={stats.inspect_only}"
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
