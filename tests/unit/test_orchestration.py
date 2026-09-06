"""Unit tests for Airflow-independent orchestration jobs."""

from __future__ import annotations

from pathlib import Path

from realtime_market_stream.orchestration.jobs import (
    run_backfill,
    run_freshness_check,
    run_schema_evolution_check,
)


def test_schema_evolution_matches_bundled() -> None:
    result = run_schema_evolution_check()
    assert result.ok
    assert "trade-tick" in result.subjects
    assert result.drifted == []


def test_backfill_writes_bronze(tmp_path: Path) -> None:
    result = run_backfill(count=8, data_root=tmp_path, seed=7)
    assert result.generated == 8
    assert result.written >= 8
    assert result.dlq == 0
    files = list(tmp_path.glob("bronze/ticks/**/*.jsonl"))
    assert files


def test_freshness_after_backfill(tmp_path: Path) -> None:
    run_backfill(count=5, data_root=tmp_path, seed=1)
    check = run_freshness_check(
        view="bronze_ticks",
        data_root=tmp_path,
        max_age_seconds=86_400,
    )
    assert check.rows > 0
    assert check.stale is False
    assert check.latest_ts


def test_freshness_empty_is_stale(tmp_path: Path) -> None:
    check = run_freshness_check(view="bronze_ticks", data_root=tmp_path, max_age_seconds=60)
    assert check.rows == 0
    assert check.stale is True
