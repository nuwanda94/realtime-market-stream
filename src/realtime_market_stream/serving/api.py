"""FastAPI serving layer over the local lakehouse.

Endpoints (Phase 2):
* ``GET /health`` — process liveness + lakehouse root
* ``GET /metrics`` — Prometheus text exposition of request counters
* ``GET /ticks/latest`` — newest Bronze/Silver trade ticks
* ``GET /ohlc`` — Silver OHLCV bars
* ``GET /anomalies`` — Gold bars flagged ``is_anomaly``

Reads Hive-partitioned JSONL written by the filesystem sinks. Delta/Iceberg
querying is deferred to the dedicated query-layer task.
"""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from realtime_market_stream.config.settings import Settings, get_settings
from realtime_market_stream.observability.tracing import instrument_fastapi, start_span
from realtime_market_stream.processing.backpressure import FlowController, controller_from_settings
from realtime_market_stream.serving.store import LakehouseStore, lakehouse_root

_API_EXEMPT_PATHS = frozenset({"/health", "/metrics"})


class HealthResponse(BaseModel):
    status: str
    app_env: str
    lakehouse_root: str
    lakehouse_format: str


class TickListResponse(BaseModel):
    count: int
    items: list[dict[str, Any]] = Field(default_factory=list)


class OhlcListResponse(BaseModel):
    count: int
    items: list[dict[str, Any]] = Field(default_factory=list)


class AnomalyListResponse(BaseModel):
    count: int
    items: list[dict[str, Any]] = Field(default_factory=list)


class Metrics:
    """In-process counters for ``GET /metrics`` (no extra deps)."""

    def __init__(self) -> None:
        self.requests: dict[str, int] = defaultdict(int)
        self.rate_limited: int = 0
        self.started_at = time.time()

    def inc(self, path: str) -> None:
        self.requests[path] += 1

    def render(self) -> str:
        lines = [
            "# HELP rms_up 1 if the serving process is running",
            "# TYPE rms_up gauge",
            "rms_up 1",
            "# HELP rms_uptime_seconds Seconds since process start",
            "# TYPE rms_uptime_seconds gauge",
            f"rms_uptime_seconds {time.time() - self.started_at:.3f}",
            "# HELP rms_http_requests_total Requests by path",
            "# TYPE rms_http_requests_total counter",
        ]
        for path, count in sorted(self.requests.items()):
            escaped = path.replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f'rms_http_requests_total{{path="{escaped}"}} {count}')
        lines.extend(
            [
                "# HELP rms_http_rate_limited_total Requests rejected with HTTP 429",
                "# TYPE rms_http_rate_limited_total counter",
                f"rms_http_rate_limited_total {self.rate_limited}",
            ]
        )
        return "\n".join(lines) + "\n"


def create_app(
    *,
    settings: Settings | None = None,
    store: LakehouseStore | None = None,
    flow: FlowController | None = None,
) -> FastAPI:
    """Application factory used by uvicorn and tests."""

    resolved = settings or get_settings()
    lake_store = store or LakehouseStore(lakehouse_root(resolved))
    metrics = Metrics()
    flow_ctrl = flow or controller_from_settings(resolved.flow)

    application = FastAPI(
        title="realtime-market-stream API",
        version="0.3.0",
        description="Latest ticks, OHLC bars, and Gold anomalies from the local lakehouse.",
    )
    application.state.settings = resolved
    application.state.store = lake_store
    application.state.metrics = metrics
    application.state.flow = flow_ctrl
    instrument_fastapi(application, resolved)

    @application.middleware("http")
    async def _count_and_limit(request: Request, call_next: Any) -> Any:
        path = request.url.path
        if path not in _API_EXEMPT_PATHS and not flow_ctrl.allow_api():
            metrics.rate_limited += 1
            return JSONResponse(
                status_code=429,
                content={"detail": "rate limit exceeded"},
                headers={"Retry-After": "1"},
            )
        metrics.inc(path)
        return await call_next(request)

    @application.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(
            status="ok",
            app_env=str(resolved.app_env),
            lakehouse_root=str(lake_store.root),
            lakehouse_format=str(resolved.lakehouse.format),
        )

    @application.get("/metrics", response_class=PlainTextResponse)
    def prometheus_metrics() -> str:
        return metrics.render()

    @application.get("/ticks/latest", response_model=TickListResponse)
    def latest_ticks(
        symbol: str | None = Query(default=None, description="Optional ticker filter"),
        limit: int = Query(default=50, ge=1, le=1000),
        layer: str = Query(default="bronze", pattern="^(bronze|silver)$"),
    ) -> TickListResponse:
        with start_span("api.ticks.latest", layer=layer, limit=limit):
            items = lake_store.latest_ticks(symbol=symbol, limit=limit, layer=layer)
        return TickListResponse(count=len(items), items=items)

    @application.get("/ohlc", response_model=OhlcListResponse)
    def ohlc(
        symbol: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=1000),
    ) -> OhlcListResponse:
        with start_span("api.ohlc", limit=limit):
            items = lake_store.ohlc_bars(symbol=symbol, limit=limit)
        return OhlcListResponse(count=len(items), items=items)

    @application.get("/anomalies", response_model=AnomalyListResponse)
    def anomalies(
        symbol: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=1000),
    ) -> AnomalyListResponse:
        with start_span("api.anomalies", limit=limit):
            items = lake_store.anomalies(symbol=symbol, limit=limit)
        return AnomalyListResponse(count=len(items), items=items)

    @application.get("/")
    def root() -> dict[str, Any]:
        return {
            "service": "realtime-market-stream",
            "docs": "/docs",
            "health": "/health",
        }

    return application


app = create_app()
