#!/usr/bin/env python3
"""Idempotently create Redpanda/Kafka topics for the market stream pipeline.

Requires a reachable broker (``make compose-up``) and ``kafka-python``.

Usage:
    python scripts/create_topics.py
    python scripts/create_topics.py --dry-run
    python scripts/create_topics.py --bootstrap localhost:19092
"""

from __future__ import annotations

import argparse
import logging
import sys

from realtime_market_stream.config import get_settings
from realtime_market_stream.ingestion.topics import default_topic_specs, ensure_topics


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create pipeline Kafka topics if missing.")
    parser.add_argument(
        "--bootstrap",
        default="",
        help="Bootstrap servers (default: KAFKA_BOOTSTRAP_SERVERS / settings).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned topics without contacting the broker.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    settings = get_settings()
    bootstrap = args.bootstrap or settings.kafka.bootstrap_servers
    specs = default_topic_specs(settings.kafka)

    print(f"broker: {bootstrap}")
    for spec in specs:
        print(
            f"  - {spec.name} "
            f"(partitions={spec.partitions}, rf={spec.replication_factor})"
        )

    try:
        results = ensure_topics(specs, bootstrap_servers=bootstrap, dry_run=args.dry_run)
    except ImportError:
        print(
            "kafka-python is required to talk to the broker. "
            "Install the package extras or run: pip install kafka-python",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001 — CLI surface, report and exit
        print(f"failed to create topics: {exc}", file=sys.stderr)
        return 2

    for name, status in results.items():
        print(f"{name}: {status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
