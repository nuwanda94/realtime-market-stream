"""Local-first JSON Schema registry for market event payloads.

The default path needs no Schema Registry server: versioned JSON Schema
documents live next to this module and are validated in-process. An optional
``SCHEMA_REGISTRY_URL`` setting is accepted for a future Redpanda / Confluent
client; it is never required for the laptop path.

Subjects follow Kafka-style names so they can be registered later without
renaming:

* ``trade-tick`` — last-sale / print events (``TradeTick``)
* ``ohlcv-bar`` — closed OHLCV candles (``OhlcvBar``)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from realtime_market_stream.schemas.ticks import OhlcvBar, TradeTick

SCHEMA_PACKAGE = "realtime_market_stream.schemas.json"
SCHEMA_VERSION = 1


class SchemaSubject(StrEnum):
    """Registered subjects. Values are stable Kafka-style names."""

    TRADE_TICK = "trade-tick"
    OHLCV_BAR = "ohlcv-bar"


SUBJECT_MODELS: dict[SchemaSubject, type[BaseModel]] = {
    SchemaSubject.TRADE_TICK: TradeTick,
    SchemaSubject.OHLCV_BAR: OhlcvBar,
}

SUBJECT_FILENAMES: dict[SchemaSubject, str] = {
    SchemaSubject.TRADE_TICK: "trade_tick.v1.json",
    SchemaSubject.OHLCV_BAR: "ohlcv_bar.v1.json",
}


class SchemaRegistryError(Exception):
    """Raised when a subject is unknown or a payload fails JSON Schema checks."""


def infer_subject(payload: dict[str, Any]) -> SchemaSubject:
    """Pick a subject from ``event_type`` or from the payload's shape."""
    event_type = str(payload.get("event_type", "")).lower()
    if event_type in {"ohlcv", "ohlcv-bar", "bar"}:
        return SchemaSubject.OHLCV_BAR
    if event_type in {"trade", "trade-tick", "tick"}:
        return SchemaSubject.TRADE_TICK
    if {"open", "high", "low", "close", "window_start"} <= set(payload):
        return SchemaSubject.OHLCV_BAR
    return SchemaSubject.TRADE_TICK


def model_for_subject(subject: SchemaSubject | str) -> type[BaseModel]:
    key = SchemaSubject(subject)
    return SUBJECT_MODELS[key]


def build_json_schema(
    subject: SchemaSubject | str, *, version: int = SCHEMA_VERSION
) -> dict[str, Any]:
    """Export a JSON Schema document from the canonical Pydantic model."""
    key = SchemaSubject(subject)
    model = SUBJECT_MODELS[key]
    schema = model.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = f"https://realtime-market-stream.local/schemas/{key.value}/v{version}"
    schema["title"] = key.value
    schema["x-subject"] = key.value
    schema["x-version"] = version
    return schema


def load_bundled_schema(subject: SchemaSubject | str) -> dict[str, Any]:
    """Load the checked-in JSON Schema for ``subject``."""
    key = SchemaSubject(subject)
    filename = SUBJECT_FILENAMES[key]
    try:
        root = resources.files(SCHEMA_PACKAGE)
        data = (root / filename).read_text(encoding="utf-8")
    except (FileNotFoundError, ModuleNotFoundError, TypeError):
        path = Path(__file__).resolve().parent / "json" / filename
        data = path.read_text(encoding="utf-8")
    document = json.loads(data)
    if not isinstance(document, dict):
        raise SchemaRegistryError(f"schema {filename} is not a JSON object")
    return document


def _type_ok(expected: str | list[str], value: Any) -> bool:
    kinds = {expected} if isinstance(expected, str) else set(expected)
    if "null" in kinds and value is None:
        return True
    if value is None:
        return False
    mapping: dict[str, type | tuple[type, ...]] = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "object": dict,
        "array": list,
    }
    for kind in kinds:
        py = mapping.get(kind)
        if kind == "boolean" and isinstance(value, bool):
            return True
        if kind == "number" and isinstance(value, bool):
            continue
        if py is not None and isinstance(value, py) and not isinstance(value, bool):
            return True
    return False


def validate_against_json_schema(payload: dict[str, Any], schema: dict[str, Any]) -> None:
    """Minimal required-field + type check against a JSON Schema object.

    Full Draft-2020 evaluation is not required for the local path; Pydantic
    remains the source of truth for coercion. This check rejects obviously
    malformed records before they hit the models.
    """
    if schema.get("type") == "object" and not isinstance(payload, dict):
        raise SchemaRegistryError("payload is not a JSON object")
    required = schema.get("required") or []
    missing = [name for name in required if name not in payload]
    if missing:
        raise SchemaRegistryError(f"missing required fields: {', '.join(missing)}")
    properties = schema.get("properties") or {}
    for name, spec in properties.items():
        if name not in payload:
            continue
        expected = spec.get("type")
        if expected and not _type_ok(expected, payload[name]):
            raise SchemaRegistryError(
                f"field {name!r} expected type {expected!r}, got {type(payload[name]).__name__}"
            )


@dataclass(frozen=True)
class RegisteredSchema:
    """One versioned schema document."""

    subject: SchemaSubject
    version: int
    schema: dict[str, Any]

    @property
    def schema_id(self) -> str:
        return f"{self.subject.value}-v{self.version}"


class SchemaRegistry:
    """In-process registry backed by bundled JSON Schema files.

    Parameters
    ----------
    url:
        Optional remote Schema Registry base URL (e.g. ``http://localhost:18081``).
        Unused for validation today; stored so callers can detect a remote
        profile without inventing credentials.
    """

    def __init__(self, url: str = "") -> None:
        self.url = url.rstrip("/")
        self._cache: dict[SchemaSubject, RegisteredSchema] = {}
        for subject in SchemaSubject:
            document = load_bundled_schema(subject)
            version = int(document.get("x-version", SCHEMA_VERSION))
            self._cache[subject] = RegisteredSchema(
                subject=subject,
                version=version,
                schema=document,
            )

    @property
    def is_remote(self) -> bool:
        return bool(self.url)

    def get(self, subject: SchemaSubject | str) -> RegisteredSchema:
        key = SchemaSubject(subject)
        return self._cache[key]

    def list_subjects(self) -> list[str]:
        return [item.value for item in SchemaSubject]

    def validate(
        self,
        payload: dict[str, Any],
        subject: SchemaSubject | str | None = None,
    ) -> BaseModel:
        """JSON Schema gate + Pydantic parse. Raises ``SchemaRegistryError``."""
        key = SchemaSubject(subject) if subject is not None else infer_subject(payload)
        registered = self.get(key)
        validate_against_json_schema(payload, registered.schema)
        model = SUBJECT_MODELS[key]
        try:
            return model.model_validate(payload)
        except ValidationError as exc:
            raise SchemaRegistryError(str(exc)) from exc


@lru_cache(maxsize=1)
def get_schema_registry(url: str = "") -> SchemaRegistry:
    """Process-wide registry (tests should construct ``SchemaRegistry`` directly)."""
    return SchemaRegistry(url=url)
