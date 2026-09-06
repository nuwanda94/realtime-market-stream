"""Read Hive-partitioned JSONL lakehouse files for the serving API.

Zero extra dependencies: walk ``data/{bucket}/{layer}/...`` and parse JSONL
parts written by the filesystem sinks. Prefer :class:`QueryEngine` when DuckDB
or Polars extras are installed (see ``serving/query.py``).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from realtime_market_stream.config.settings import Settings


def lakehouse_root(settings: Settings, *, local_root: str | Path | None = None) -> Path:
    """Resolve the on-disk lakehouse root used by JSONL sinks."""

    if local_root is not None:
        return Path(local_root)
    configured = settings.lakehouse.uri.strip()
    if configured and not configured.startswith("s3://"):
        return Path(configured)
    return Path("data") / settings.minio.bucket


def _parse_hive_part(name: str, key: str) -> str | None:
    prefix = f"{key}="
    if name.startswith(prefix):
        return name[len(prefix) :]
    return None


def _row_ts(row: dict[str, Any]) -> str:
    for key in ("ts", "window_start", "window_end", "ingested_at"):
        value = row.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            yield payload


class LakehouseStore:
    """Filesystem scanner over Bronze / Silver / Gold JSONL partitions."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _scan(
        self,
        layer_glob: str,
        *,
        symbol: str | None = None,
        event_type: str | None = None,
        extra_filter: Callable[[dict[str, Any]], bool] | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if not self.root.exists():
            return []
        wanted = symbol.strip().upper() if symbol else None
        rows: list[dict[str, Any]] = []
        for path in self.root.glob(layer_glob):
            if path.suffix not in {".jsonl", ".json"}:
                continue
            part_symbol = None
            part_event = None
            for parent in path.parents:
                if part_symbol is None:
                    part_symbol = _parse_hive_part(parent.name, "symbol")
                if part_event is None:
                    part_event = _parse_hive_part(parent.name, "event_type")
            if wanted and part_symbol and part_symbol.upper() != wanted:
                continue
            if event_type and part_event and part_event.lower() != event_type.lower():
                continue
            for row in _read_jsonl(path):
                if wanted:
                    row_sym = str(row.get("symbol", part_symbol or "")).upper()
                    if row_sym != wanted:
                        continue
                if event_type:
                    row_type = str(row.get("event_type", part_event or "")).lower()
                    if row_type and row_type != event_type.lower():
                        continue
                if extra_filter is not None and not extra_filter(row):
                    continue
                rows.append(row)
        rows.sort(key=_row_ts, reverse=True)
        return rows[:limit]

    def latest_ticks(
        self,
        *,
        symbol: str | None = None,
        limit: int = 50,
        layer: str = "bronze",
    ) -> list[dict[str, Any]]:
        layer = layer.lower()
        if layer == "silver":
            pattern = "silver/ticks/event_type=trade/**/*.jsonl"
        else:
            pattern = "bronze/ticks/event_type=trade/**/*.jsonl"
        return self._scan(pattern, symbol=symbol, event_type="trade", limit=limit)

    def ohlc_bars(self, *, symbol: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        return self._scan(
            "silver/ticks/event_type=ohlcv/**/*.jsonl",
            symbol=symbol,
            event_type="ohlcv",
            limit=limit,
        )

    def anomalies(self, *, symbol: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        def _is_anomaly(row: dict[str, Any]) -> bool:
            flag = row.get("is_anomaly")
            return flag is True or flag == "true" or flag == 1

        return self._scan(
            "gold/bars/**/*.jsonl",
            symbol=symbol,
            extra_filter=_is_anomaly,
            limit=limit,
        )
