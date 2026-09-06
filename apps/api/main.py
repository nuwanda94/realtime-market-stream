"""Uvicorn entrypoint: ``uvicorn apps.api.main:app --reload --port 8000``.

The application package lives under ``src/realtime_market_stream/serving``.
"""

from realtime_market_stream.serving.api import app

__all__ = ["app"]
