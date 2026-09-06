"""Lakehouse-backed helpers for the live Streamlit dashboard.

Streamlit stays an optional extra (``pip install -e '.[dashboard]'``). These
helpers talk to :class:`LakehouseStore` so unit tests and CI stay zero-cost.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from realtime_market_stream.config.settings import Settings, get_settings
from realtime_market_stream.serving.store import LakehouseStore, lakehouse_root


def _parse_ts(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def latency_ms(row: dict[str, Any], *, now: datetime | None = None) -> float | None:
    """Event-time lag in milliseconds: ``now - ts`` (or ingested_at - ts)."""

    event = _parse_ts(row.get("ts"))
    if event is None:
        return None
    ingested = _parse_ts(row.get("ingested_at"))
    reference = ingested if ingested is not None else (now or datetime.now(timezone.utc))
    return max(0.0, (reference - event).total_seconds() * 1000.0)


def summarize_ticks(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Headline metrics for the dashboard header."""

    lags = [lag for row in rows if (lag := latency_ms(row)) is not None]
    symbols = sorted({str(row.get("symbol", "")).upper() for row in rows if row.get("symbol")})
    return {
        "tick_count": len(rows),
        "symbol_count": len(symbols),
        "symbols": symbols,
        "avg_latency_ms": (sum(lags) / len(lags)) if lags else None,
        "p95_latency_ms": (sorted(lags)[int(0.95 * (len(lags) - 1))] if lags else None),
        "max_latency_ms": max(lags) if lags else None,
    }


def load_dashboard_payload(
    *,
    settings: Settings | None = None,
    store: LakehouseStore | None = None,
    symbol: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """Bundle ticks, OHLC bars, anomalies, and latency stats."""

    resolved = settings or get_settings()
    lake = store or LakehouseStore(lakehouse_root(resolved))
    ticks = lake.latest_ticks(symbol=symbol, limit=limit, layer="bronze")
    silver_ticks = lake.latest_ticks(symbol=symbol, limit=limit, layer="silver")
    ohlc = lake.ohlc_bars(symbol=symbol, limit=limit)
    alerts = lake.anomalies(symbol=symbol, limit=limit)
    stats = summarize_ticks(ticks or silver_ticks)
    return {
        "lakehouse_root": str(lake.root),
        "ticks": ticks,
        "silver_ticks": silver_ticks,
        "ohlc": ohlc,
        "anomalies": alerts,
        "stats": stats,
    }
