#!/usr/bin/env python3
"""Dump bundled JSON Schema documents (and optionally regenerate them).

Usage
-----
    python scripts/export_schemas.py
    python scripts/export_schemas.py --regen
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from realtime_market_stream.schemas.registry import (  # noqa: E402
    SCHEMA_VERSION,
    SUBJECT_FILENAMES,
    SchemaSubject,
    build_json_schema,
    get_schema_registry,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export JSON Schema subjects.")
    parser.add_argument(
        "--regen",
        action="store_true",
        help="Overwrite bundled files from live Pydantic models.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=ROOT / "src" / "realtime_market_stream" / "schemas" / "json",
        help="Destination directory.",
    )
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    if args.regen:
        for subject in SchemaSubject:
            document = build_json_schema(subject, version=SCHEMA_VERSION)
            path = args.out / SUBJECT_FILENAMES[subject]
            path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
            print(f"wrote {path.relative_to(ROOT)}")
        return 0

    registry = get_schema_registry()
    for name in registry.list_subjects():
        registered = registry.get(name)
        print(f"{registered.schema_id}\t{registered.schema.get('$id')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
