"""Tick ingestion: live websocket client and synthetic fallback."""

from realtime_market_stream.ingestion.generator import SyntheticTickGenerator
from realtime_market_stream.ingestion.service import IngestionService, run_ingestion

__all__ = ["IngestionService", "SyntheticTickGenerator", "run_ingestion"]
