"""Unit tests for token-bucket rate limiting and inflight backpressure."""

from __future__ import annotations

import time

from realtime_market_stream.config.settings import FlowControlSettings, Settings
from realtime_market_stream.processing.backpressure import (
    BackpressurePublisher,
    FlowController,
    InflightGate,
    TokenBucket,
)


def test_token_bucket_disabled_always_allows() -> None:
    bucket = TokenBucket(rate_per_sec=0)
    assert bucket.try_acquire() is True
    assert bucket.acquire() is True


def test_token_bucket_rejects_when_empty() -> None:
    bucket = TokenBucket(rate_per_sec=10, burst=1)
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False


def test_token_bucket_refills() -> None:
    bucket = TokenBucket(rate_per_sec=100, burst=1)
    assert bucket.try_acquire() is True
    time.sleep(0.03)
    assert bucket.try_acquire() is True


def test_inflight_gate_blocks_at_cap() -> None:
    gate = InflightGate(max_inflight=1)
    assert gate.try_acquire() is True
    assert gate.try_acquire() is False
    gate.release()
    assert gate.try_acquire() is True


def test_inflight_disabled() -> None:
    gate = InflightGate(max_inflight=0)
    assert gate.disabled is True
    assert gate.acquire() is True


def test_flow_controller_disabled_is_noop() -> None:
    ctrl = FlowController(FlowControlSettings(enabled=False, ingest_max_per_sec=1, api_requests_per_sec=1))
    assert ctrl.allow_ingest() is True
    assert ctrl.allow_api() is True
    assert ctrl.enter_inflight() is True


def test_backpressure_publisher_drops_when_rate_limited() -> None:
    class Fake:
        def __init__(self) -> None:
            self.sent: list[str] = []

        def send(self, topic: str, key: bytes | None, value: bytes) -> None:
            self.sent.append(topic)

        def flush(self) -> None:
            return None

        def close(self) -> None:
            return None

    settings = FlowControlSettings(
        enabled=True,
        ingest_max_per_sec=50,
        ingest_burst=1,
        producer_max_inflight=10,
        producer_block_timeout_sec=0.0,
        api_requests_per_sec=50,
    )
    ctrl = FlowController(settings)
    inner = Fake()
    pub = BackpressurePublisher(inner, ctrl)
    pub.send("raw-ticks", b"A", b"{}")
    pub.send("raw-ticks", b"A", b"{}")
    assert len(inner.sent) == 1
    assert ctrl.stats.dropped >= 1 or ctrl.stats.rate_limited >= 1


def test_settings_exposes_flow_section() -> None:
    settings = Settings()
    assert settings.flow.enabled is True
    assert settings.flow.producer_max_inflight == 500
    assert settings.flow.ingest_max_per_sec == 0.0


def test_api_returns_429_when_bucket_empty() -> None:
    import pytest

    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from realtime_market_stream.serving.api import create_app
    from realtime_market_stream.serving.store import LakehouseStore

    tight = FlowController(
        FlowControlSettings(
            enabled=True,
            api_requests_per_sec=1,
            api_burst=1,
            ingest_max_per_sec=0,
        )
    )
    app = create_app(settings=Settings(), store=LakehouseStore("/tmp/rms-empty"), flow=tight)
    client = TestClient(app)
    first = client.get("/")
    second = client.get("/")
    assert first.status_code == 200
    assert second.status_code == 429
    health = client.get("/health")
    assert health.status_code == 200
