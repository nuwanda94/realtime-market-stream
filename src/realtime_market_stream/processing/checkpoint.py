"""Local checkpoint store for processor offsets and Silver state.

Exactly-once here is the practical local-first combination of:

1. Manual Kafka commits **after** a successful lakehouse write (not auto-commit).
2. A durable file checkpoint of the last committed ``(topic, partition, offset)``.
3. Restored Silver state (dedup ids, prev-close, open windows) so a restart
   does not double-count trades or re-close bars.

Kafka itself remains at-least-once. Idempotency on replay comes from the
Silver ``tick_id`` cache plus skipping already-committed offsets.
"""

from __future__ import annotations

import json
import logging
import tempfile
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

CHECKPOINT_VERSION = 1


@dataclass(frozen=True)
class ConsumedRecord:
    """One Kafka (or fake) record plus coordinates used for commits."""

    key: bytes | None
    value: bytes
    topic: str = ""
    partition: int = 0
    offset: int = -1

    def offset_key(self) -> str:
        return f"{self.topic}:{self.partition}"


class OffsetCommitter(Protocol):
    """Optional hook so real Kafka consumers can commit the broker offset."""

    def commit_offsets(self, records: list[ConsumedRecord]) -> None: ...


@dataclass
class OffsetMap:
    """Highest *committed* offset per ``topic:partition``."""

    committed: dict[str, int] = field(default_factory=dict)

    def already_processed(self, record: ConsumedRecord) -> bool:
        if record.offset < 0:
            return False
        last = self.committed.get(record.offset_key())
        if last is None:
            return False
        return record.offset <= last

    def advance(self, records: Iterable[ConsumedRecord]) -> None:
        for record in records:
            if record.offset < 0:
                continue
            key = record.offset_key()
            current = self.committed.get(key, -1)
            if record.offset > current:
                self.committed[key] = record.offset


@dataclass
class ProcessorCheckpoint:
    """Serializable snapshot written atomically to disk."""

    consumer_group: str
    offsets: dict[str, int] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)
    updated_at: str = ""
    version: int = CHECKPOINT_VERSION

    def to_json(self) -> str:
        payload = asdict(self)
        return json.dumps(payload, indent=2, sort_keys=True)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ProcessorCheckpoint:
        offsets_raw = raw.get("offsets") or {}
        offsets = {str(k): int(v) for k, v in offsets_raw.items()}
        state = raw.get("state") if isinstance(raw.get("state"), dict) else {}
        return cls(
            consumer_group=str(raw.get("consumer_group", "")),
            offsets=offsets,
            state=dict(state),
            updated_at=str(raw.get("updated_at", "")),
            version=int(raw.get("version", CHECKPOINT_VERSION)),
        )


class FileCheckpointStore:
    """Atomic JSON checkpoint files under ``CHECKPOINT_DIR/{group}.json``."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, consumer_group: str) -> Path:
        safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in consumer_group)
        return self.root / f"{safe}.json"

    def load(self, consumer_group: str) -> ProcessorCheckpoint:
        path = self.path_for(consumer_group)
        if not path.is_file():
            return ProcessorCheckpoint(consumer_group=consumer_group)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("checkpoint unreadable at %s: %s", path, exc)
            return ProcessorCheckpoint(consumer_group=consumer_group)
        if not isinstance(raw, dict):
            return ProcessorCheckpoint(consumer_group=consumer_group)
        checkpoint = ProcessorCheckpoint.from_dict(raw)
        if not checkpoint.consumer_group:
            checkpoint.consumer_group = consumer_group
        return checkpoint

    def save(self, checkpoint: ProcessorCheckpoint) -> Path:
        checkpoint.updated_at = datetime.now(tz=UTC).isoformat()
        path = self.path_for(checkpoint.consumer_group)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = checkpoint.to_json()
        fd, tmp_name = tempfile.mkstemp(prefix=path.name, suffix=".tmp", dir=path.parent)
        tmp_path = Path(tmp_name)
        try:
            with open(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.write("\n")
            tmp_path.replace(path)
        except Exception:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        logger.debug("wrote checkpoint %s", path)
        return path


def records_from_values(values: Iterable[bytes]) -> list[ConsumedRecord]:
    """Wrap raw values as offset-less records (unit tests / in-process batches)."""
    return [ConsumedRecord(key=None, value=value) for value in values]


def iter_unprocessed(
    records: Iterable[ConsumedRecord], offsets: OffsetMap
) -> Iterator[ConsumedRecord]:
    """Yield records whose offset has not yet been committed."""
    for record in records:
        if offsets.already_processed(record):
            continue
        yield record
