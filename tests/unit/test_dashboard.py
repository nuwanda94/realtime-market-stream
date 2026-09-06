"""Unit tests for dashboard payload helpers (no Streamlit required)."""

from __future__ import annotations

from pathlib import Path

from realtime_market_stream.serving.dashboard import (
    latency_ms,
    load_dashboard_payload,
    summarize_ticks,
)
from realtime_market_stream.serving.store import LakehouseStore


def test_latency_ms_from_event_and_ingest() -> None:
    row = {
        "ts": "2026-09-06T12:00:00+00:00",
        "ingested_at": "2026-09-06T12:00:00.250+00:00",
    }
    assert latency_ms(row) == 250.0


def test_summarize_ticks_empty() -> None:
    stats = summarize_ticks([])
    assert stats["tick_count"] == 0
    assert stats["avg_latency_ms"] is None


def test_load_dashboard_payload_from_jsonl(tmp_path: Path) -> None:
    part = tmp_path / "bronze" / "ticks" / "event_type=trade" / "symbol=AAPL" / "date=2026-09-06"
    part.mkdir(parents=True)
    (part / "part-000.jsonl").write_text(
        '{"tick_id":"t1","symbol":"AAPL","event_type":"trade","price":100,'
        '"ts":"2026-09-06T12:00:00+00:00","ingested_at":"2026-09-06T12:00:01+00:00"}\n',
        encoding="utf-8",
    )
    gold = tmp_path / "gold" / "bars" / "symbol=AAPL" / "date=2026-09-06"
    gold.mkdir(parents=True)
    (gold / "part-000.jsonl").write_text(
        '{"symbol":"AAPL","is_anomaly":true,"close":101,"window_end":"2026-09-06T12:01:00+00:00"}\n',
        encoding="utf-8",
    )

    payload = load_dashboard_payload(store=LakehouseStore(tmp_path), symbol="AAPL", limit=10)
    assert payload["stats"]["tick_count"] == 1
    assert payload["stats"]["avg_latency_ms"] == 1000.0
    assert len(payload["anomalies"]) == 1
    assert payload["lakehouse_root"] == str(tmp_path)
