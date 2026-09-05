"""Tick ingestion: live websocket client and synthetic fallback."""

from realtime_market_stream.ingestion.generator import SyntheticTickGenerator

__all__ = ["SyntheticTickGenerator"]
