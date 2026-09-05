"""Bronze lakehouse sink: Hive-partitioned raw ticks.

Path layout (Delta/Iceberg-shaped, local-first)::

    {root}/bronze/ticks/event_type={trade|ohlcv}/symbol={SYM}/date={YYYY-MM-DD}/{batch}.jsonl

``FilesystemBronzeSink`` writes to a local directory so tests and laptops
work with zero extra dependencies. Point ``root`` at a FUSE/MinIO mount or
replace this class with a deltalake writer in a later sink task.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from realtime_market_stream.config.settings import Settings


@dataclass(frozen=True)
class BronzeRecord:
    """One schema-validated event ready for the Bronze layer."""

    event_type: str
    symbol: str
    event_date: str
    ingested_at: str
    payload: dict[str, Any]

    def partition_path(self) -> str:
        return (
            f"bronze/ticks/event_type={self.event_type}/symbol={self.symbol}/date={self.event_date}"
        )

    def to_jsonl_line(self) -> str:
        row = {
            "event_type": self.event_type,
            "symbol": self.symbol,
            "event_date": self.event_date,
            "ingested_at": self.ingested_at,
            **self.payload,
        }
        return json.dumps(row, separators=(",", ":"), default=str)


class BronzeSink(Protocol):
    """Write accepted Bronze records to object storage / disk."""

    def write(self, records: list[BronzeRecord]) -> list[str]: ...


class FilesystemBronzeSink:
    """Partitioned JSONL writer used until a dedicated Delta sink lands."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    @classmethod
    def from_settings(
        cls, settings: Settings, *, local_root: str | Path | None = None
    ) -> FilesystemBronzeSink:
        """Default to ``./data/market-lake`` so MinIO can be mounted later.

        The bucket name from settings is used as the last path segment so the
        on-disk layout matches ``s3://{bucket}/bronze/...``.
        """
        if local_root is not None:
            return cls(local_root)
        return cls(Path("data") / settings.minio.bucket)

    def write(self, records: list[BronzeRecord]) -> list[str]:
        if not records:
            return []
        groups: dict[str, list[BronzeRecord]] = {}
        for record in records:
            groups.setdefault(record.partition_path(), []).append(record)
        written: list[str] = []
        for partition, group in groups.items():
            directory = self.root / partition
            directory.mkdir(parents=True, exist_ok=True)
            filename = f"part-{uuid4().hex[:12]}.jsonl"
            path = directory / filename
            with path.open("w", encoding="utf-8") as handle:
                for record in group:
                    handle.write(record.to_jsonl_line())
                    handle.write("\n")
            written.append(str(path))
        return written


class InMemoryBronzeSink:
    """Test double that keeps records in process memory."""

    def __init__(self) -> None:
        self.records: list[BronzeRecord] = []
        self.writes: int = 0

    def write(self, records: list[BronzeRecord]) -> list[str]:
        self.records.extend(records)
        self.writes += 1
        return [f"memory://batch-{self.writes}"]
