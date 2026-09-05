"""Canonical trade-tick and OHLCV bar models."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class Side(StrEnum):
    """Aggressor side of a trade."""

    BUY = "buy"
    SELL = "sell"


class TradeTick(BaseModel):
    """A single last-sale / print event."""

    tick_id: UUID
    symbol: str
    ts: datetime
    price: float = Field(gt=0)
    size: int = Field(gt=0)
    side: Side
    venue: str = "SYNTH"
    sequence: int = Field(ge=0)

    @field_validator("symbol")
    @classmethod
    def _upper_symbol(cls, value: str) -> str:
        return value.strip().upper()

    def to_kafka_value(self) -> dict[str, Any]:
        """JSON-serializable payload for the raw-ticks topic."""
        payload = self.model_dump(mode="json")
        payload["event_type"] = "trade"
        return payload


class OhlcvBar(BaseModel):
    """A closed OHLCV candle."""

    symbol: str
    window_start: datetime
    window_end: datetime
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: int = Field(ge=0)
    trade_count: int = Field(ge=0)
    vwap: float = Field(gt=0)

    @field_validator("symbol")
    @classmethod
    def _upper_symbol(cls, value: str) -> str:
        return value.strip().upper()

    def to_kafka_value(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        payload["event_type"] = "ohlcv"
        return payload
