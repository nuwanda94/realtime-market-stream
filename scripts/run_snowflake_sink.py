#!/usr/bin/env python3
"""Flush synthetic ticks through the Snowflake Streaming sink.

Local-first: by default writes JSONL capture files under data/snowflake/.
Live INSERT requires SNOWFLAKE_LOCAL_CAPTURE=false, connection fields, and
``pip install -e '.[snowflake]'``.

Usage:
    python scripts/run_snowflake_sink.py --count 5
    python scripts/run_snowflake_sink.py --count 5 --data-root /tmp/sf
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

from realtime_market_stream.config.settings import get_settings
from realtime_market_stream.ingestion.generator import SyntheticTickGenerator
from realtime_market_stream.schemas.ticks import OhlcvBar, TradeTick
from realtime_market_stream.sinks.bronze import BronzeRecord
from realtime_market_stream.sinks.snowflake import build_snowflake_sink


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--data-root", default="")
    return parser.parse_args(argv)


def _as_bronze(event: TradeTick | OhlcvBar) -> BronzeRecord:
    payload = event.to_kafka_value()
    ts = payload.get("ts") or payload.get("window_start") or datetime.now(UTC).isoformat()
    event_date = str(ts)[:10]
    return BronzeRecord(
        event_type=str(payload.get("event_type", "trade")),
        symbol=str(payload.get("symbol", "UNK")),
        event_date=event_date,
        ingested_at=str(ts),
        payload=payload,
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    settings = get_settings()
    local_root = Path(args.data_root) if args.data_root else None
    sink = build_snowflake_sink(settings, local_root=local_root, force=True)
    if sink is None:
        print("Snowflake sink disabled", file=sys.stderr)
        return 1
    generator = SyntheticTickGenerator.from_settings(settings.generator, seed=1)
    records: list[BronzeRecord] = []
    produced = 0
    while produced < args.count:
        for event in generator.next_events():
            records.append(_as_bronze(event))
            if isinstance(event, TradeTick):
                produced += 1
                if produced >= args.count:
                    break
    written = sink.write(records)
    print(f"flushed={len(records)} files={len(written)}")
    for path in written:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
