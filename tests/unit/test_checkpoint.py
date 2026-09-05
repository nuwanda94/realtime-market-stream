"""Unit tests for the file checkpoint store and offset map."""

from __future__ import annotations

from pathlib import Path

from realtime_market_stream.processing.checkpoint import (
    ConsumedRecord,
    FileCheckpointStore,
    OffsetMap,
    ProcessorCheckpoint,
    coerce_consumed,
    iter_unprocessed,
    records_from_values,
)


def test_offset_map_skips_committed() -> None:
    offsets = OffsetMap(committed={"raw-ticks:0": 4})
    older = ConsumedRecord(key=None, value=b"a", topic="raw-ticks", partition=0, offset=3)
    same = ConsumedRecord(key=None, value=b"b", topic="raw-ticks", partition=0, offset=4)
    newer = ConsumedRecord(key=None, value=b"c", topic="raw-ticks", partition=0, offset=5)
    assert offsets.already_processed(older)
    assert offsets.already_processed(same)
    assert not offsets.already_processed(newer)
    offsets.advance([newer])
    assert offsets.committed["raw-ticks:0"] == 5


def test_file_checkpoint_roundtrip(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path)
    saved = ProcessorCheckpoint(
        consumer_group="bronze-processor",
        offsets={"raw-ticks:0": 11},
        state={"seen_tick_ids": ["abc"]},
    )
    path = store.save(saved)
    assert path.is_file()
    loaded = store.load("bronze-processor")
    assert loaded.offsets["raw-ticks:0"] == 11
    assert loaded.state["seen_tick_ids"] == ["abc"]
    assert loaded.updated_at


def test_corrupt_checkpoint_returns_empty(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path)
    path = store.path_for("broken")
    path.write_text("{not-json", encoding="utf-8")
    loaded = store.load("broken")
    assert loaded.offsets == {}
    assert loaded.consumer_group == "broken"


def test_coerce_and_iter_unprocessed() -> None:
    raw = coerce_consumed((b"k", b"v"))
    assert raw.value == b"v"
    records = records_from_values([b"one", b"two"])
    assert len(records) == 2
    offsets = OffsetMap()
    assert list(iter_unprocessed(records, offsets)) == records
