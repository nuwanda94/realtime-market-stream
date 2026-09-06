"""Unit tests for the FastAPI serving layer (no broker required)."""

from __future__ import annotations

from pathlib import Path

import pytest

from realtime_market_stream.config.settings import Settings
from realtime_market_stream.serving.api import create_app
from realtime_market_stream.serving.store import LakehouseStore
from realtime_market_stream.sinks.bronze import BronzeRecord, FilesystemBronzeSink
from realtime_market_stream.sinks.gold import FilesystemGoldSink, GoldRecord
from realtime_market_stream.sinks.silver import FilesystemSilverSink, SilverRecord


pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402


def _seed(root: Path) -> None:
    bronze = FilesystemBronzeSink(root)
    bronze.write(
        [
            BronzeRecord(
                event_type="trade",
                symbol="AAPL",
                event_date="2026-09-06",
                ingested_at="2026-09-06T13:30:00+00:00",
                payload={
                    "tick_id": "t1",
                    "symbol": "AAPL",
                    "ts": "2026-09-06T13:30:00+00:00",
                    "price": 190.5,
                    "size": 10,
                    "side": "buy",
                    "event_type": "trade",
                },
            )
        ]
    )
    silver = FilesystemSilverSink(root)
    silver.write(
        [
            SilverRecord(
                event_type="ohlcv",
                symbol="AAPL",
                event_date="2026-09-06",
                ingested_at="2026-09-06T13:31:00+00:00",
                payload={
                    "symbol": "AAPL",
                    "event_type": "ohlcv",
                    "window_start": "2026-09-06T13:30:00+00:00",
                    "window_end": "2026-09-06T13:31:00+00:00",
                    "open": 190.0,
                    "high": 191.0,
                    "low": 189.5,
                    "close": 190.5,
                    "volume": 100,
                    "trade_count": 4,
                    "vwap": 190.2,
                },
            )
        ]
    )
    gold = FilesystemGoldSink(root)
    gold.write(
        [
            GoldRecord(
                event_type="gold_bar",
                symbol="AAPL",
                event_date="2026-09-06",
                ingested_at="2026-09-06T13:31:05+00:00",
                payload={
                    "symbol": "AAPL",
                    "event_type": "gold_bar",
                    "window_start": "2026-09-06T13:30:00+00:00",
                    "close": 190.5,
                    "is_anomaly": True,
                    "zscore_return": 3.4,
                },
            ),
            GoldRecord(
                event_type="gold_bar",
                symbol="MSFT",
                event_date="2026-09-06",
                ingested_at="2026-09-06T13:31:05+00:00",
                payload={
                    "symbol": "MSFT",
                    "event_type": "gold_bar",
                    "window_start": "2026-09-06T13:30:00+00:00",
                    "close": 420.0,
                    "is_anomaly": False,
                    "zscore_return": 0.1,
                },
            ),
        ]
    )


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    _seed(tmp_path)
    settings = Settings()
    application = create_app(settings=settings, store=LakehouseStore(tmp_path))
    return TestClient(application)


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert "lakehouse_root" in body


def test_latest_ticks(client: TestClient) -> None:
    response = client.get("/ticks/latest", params={"symbol": "AAPL"})
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["items"][0]["symbol"] == "AAPL"
    assert body["items"][0]["price"] == 190.5


def test_ohlc(client: TestClient) -> None:
    response = client.get("/ohlc", params={"symbol": "aapl"})
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["items"][0]["close"] == 190.5


def test_anomalies_filters_flag(client: TestClient) -> None:
    response = client.get("/anomalies")
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["items"][0]["symbol"] == "AAPL"
    assert body["items"][0]["is_anomaly"] is True


def test_metrics(client: TestClient) -> None:
    client.get("/health")
    response = client.get("/metrics")
    assert response.status_code == 200
    text = response.text
    assert "rms_up 1" in text
    assert "rms_http_requests_total" in text
