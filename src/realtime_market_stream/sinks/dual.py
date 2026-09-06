"""Dual-write wrapper: local lakehouse (primary) + optional Snowflake.

Lakehouse remains the source of truth. Snowflake is best-effort unless
``fail_soft=False``. Enabled when ``APP_ENV=snowflake``, connection
fields are set, or ``SNOWFLAKE_DUAL_WRITE=true``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Generic, Protocol, TypeVar

from realtime_market_stream.config.settings import Settings
from realtime_market_stream.sinks.snowflake import (
    SnowflakeStreamingSink,
    build_snowflake_sink,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class WritableSink(Protocol[T]):
    """Any Bronze / Silver / Gold sink with a batch ``write``."""

    def write(self, records: list[T]) -> list[str]: ...


@dataclass
class DualWriteStats:
    """Counters for the fan-out path."""

    primary_writes: int = 0
    primary_paths: int = 0
    secondary_writes: int = 0
    secondary_paths: int = 0
    secondary_errors: int = 0
    rows: int = 0


@dataclass
class DualWriteSink(Generic[T]):
    """Write a batch to the lakehouse first, then to Snowflake.

    Primary failures propagate (no checkpoint should advance). Secondary
    failures are logged when ``fail_soft`` is True so a warehouse outage
    never blocks the local pipeline.
    """

    primary: WritableSink[T]
    secondary: SnowflakeStreamingSink
    fail_soft: bool = True
    stats: DualWriteStats = field(default_factory=DualWriteStats)

    def write(self, records: list[T]) -> list[str]:
        if not records:
            return []
        primary_paths = self.primary.write(records)
        self.stats.primary_writes += 1
        self.stats.primary_paths += len(primary_paths)
        self.stats.rows += len(records)
        secondary_paths = self._write_secondary(records)
        return list(primary_paths) + list(secondary_paths)

    def _write_secondary(self, records: list[T]) -> list[str]:
        try:
            # Snowflake sink accepts Bronze | Silver | Gold records.
            paths = self.secondary.write(list(records))  # type: ignore[arg-type]
        except Exception as exc:  # noqa: BLE001
            self.stats.secondary_errors += 1
            logger.warning("dual-write Snowflake flush failed: %s", exc, exc_info=True)
            if not self.fail_soft:
                raise
            return []
        self.stats.secondary_writes += 1
        self.stats.secondary_paths += len(paths)
        logger.info(
            "dual-write flushed rows=%s lakehouse=%s snowflake=%s",
            len(records),
            self.stats.primary_paths,
            len(paths),
        )
        return paths


def maybe_wrap_dual_write(
    primary: Any,
    settings: Settings,
    *,
    local_root: str | Path | None = None,
    fail_soft: bool = True,
) -> Any:
    """Wrap ``primary`` when dual-write is enabled; otherwise return it."""
    if not settings.snowflake_dual_write_enabled:
        return primary
    secondary = build_snowflake_sink(settings, local_root=_snowflake_root(local_root), force=True)
    if secondary is None:
        return primary
    return DualWriteSink(primary=primary, secondary=secondary, fail_soft=fail_soft)


def _snowflake_root(local_root: str | Path | None) -> Path | None:
    """Keep Snowflake capture next to the lakehouse root when one is given."""
    if local_root is None:
        return None
    return Path(local_root) / "snowflake"
