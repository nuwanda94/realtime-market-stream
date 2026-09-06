"""Delta Lake / Iceberg-shaped lakehouse sinks.

Writes Bronze, Silver, or Gold rows as append-only tables partitioned by
``symbol`` + ``date`` (UTC event date). Default engine is Delta Lake
(``deltalake`` + ``pyarrow``). Iceberg is the same physical layout with
Hive partitions and a lightweight metadata sidecar so laptops work
without a catalog service.

Storage:

* Local path: ``data/{bucket}/{bronze|silver|gold}/{ticks|bars}``
* MinIO: ``s3://{bucket}/...`` when ``LAKEHOUSE_URI`` starts with ``s3://``

``deltalake`` / ``pyarrow`` are optional extras (``.[lakehouse]``). The
sink raises a clear error if they are missing at write time so the rest
of the package stays importable in CI.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol
from urllib.parse import urlparse
from uuid import uuid4

from realtime_market_stream.config.settings import LakehouseFormat, Settings
from realtime_market_stream.sinks.bronze import BronzeRecord
from realtime_market_stream.sinks.gold import GoldRecord
from realtime_market_stream.sinks.silver import SilverRecord

logger = logging.getLogger(__name__)

LakeRecord = BronzeRecord | SilverRecord | GoldRecord

PARTITION_COLUMNS: tuple[str, str] = ("symbol", "date")


class LakehouseSink(Protocol):
    """Append partitioned rows to a lakehouse table."""

    def write(self, records: list[LakeRecord]) -> list[str]: ...


def records_to_rows(records: list[LakeRecord]) -> list[dict[str, Any]]:
    """Flatten sink records into table rows with partition columns."""
    rows: list[dict[str, Any]] = []
    for record in records:
        row: dict[str, Any] = {
            "event_type": record.event_type,
            "symbol": record.symbol,
            "date": record.event_date,
            "event_date": record.event_date,
            "ingested_at": record.ingested_at,
        }
        for key, value in record.payload.items():
            if key not in row:
                row[key] = value
        rows.append(row)
    return rows


def table_path(root: str, layer: str, table: str = "ticks") -> str:
    root = root.rstrip("/")
    return f"{root}/{layer}/{table}"


def minio_storage_options(settings: Settings) -> dict[str, str]:
    parsed = urlparse(settings.minio.endpoint)
    return {
        "AWS_ACCESS_KEY_ID": settings.minio.access_key,
        "AWS_SECRET_ACCESS_KEY": settings.minio.secret_key.get_secret_value(),
        "AWS_ENDPOINT_URL": settings.minio.endpoint,
        "AWS_ALLOW_HTTP": "true" if parsed.scheme != "https" else "false",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_REGION": "us-east-1",
    }


def _import_arrow() -> Any:
    try:
        import pyarrow as pa
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Delta/Iceberg sinks require pyarrow. Install extras: pip install -e '.[lakehouse]'"
        ) from exc
    return pa


def _import_deltalake() -> Any:
    try:
        from deltalake import write_deltalake
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Delta sink requires deltalake. Install extras: pip install -e '.[lakehouse]'"
        ) from exc
    return write_deltalake


def rows_to_arrow_table(rows: list[dict[str, Any]]) -> Any:
    pa = _import_arrow()
    if not rows:
        return pa.table(
            {
                "event_type": pa.array([], type=pa.string()),
                "symbol": pa.array([], type=pa.string()),
                "date": pa.array([], type=pa.string()),
                "event_date": pa.array([], type=pa.string()),
                "ingested_at": pa.array([], type=pa.string()),
            }
        )
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                keys.append(key)
    columns: dict[str, list[Any]] = {key: [] for key in keys}
    for row in rows:
        for key in keys:
            value = row.get(key)
            if isinstance(value, (dict, list)):
                value = json.dumps(value, default=str)
            columns[key].append(value)
    return pa.table({key: pa.array(values) for key, values in columns.items()})


@dataclass
class DeltaLakeSink:
    table_uri: str
    storage_options: Mapping[str, str] | None = None
    partition_by: tuple[str, ...] = PARTITION_COLUMNS

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        layer: str = "bronze",
        table: str = "ticks",
        local_root: str | Path | None = None,
    ) -> DeltaLakeSink:
        uri = _resolve_table_uri(settings, layer=layer, table=table, local_root=local_root)
        options = None
        if uri.startswith("s3://"):
            options = minio_storage_options(settings)
        return cls(table_uri=uri, storage_options=options)

    def write(self, records: list[LakeRecord]) -> list[str]:
        if not records:
            return []
        write_deltalake = _import_deltalake()
        table = rows_to_arrow_table(records_to_rows(records))
        kwargs: dict[str, Any] = {
            "mode": "append",
            "partition_by": list(self.partition_by),
            "schema_mode": "merge",
        }
        if self.storage_options:
            kwargs["storage_options"] = dict(self.storage_options)
        write_deltalake(self.table_uri, table, **kwargs)
        logger.info("delta append %s rows -> %s", len(records), self.table_uri)
        return [self.table_uri]


@dataclass
class IcebergPartitionSink:
    table_uri: str
    partition_by: tuple[str, ...] = PARTITION_COLUMNS

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        layer: str = "bronze",
        table: str = "ticks",
        local_root: str | Path | None = None,
    ) -> IcebergPartitionSink:
        uri = _resolve_table_uri(settings, layer=layer, table=table, local_root=local_root)
        if uri.startswith("s3://"):
            raise ValueError(
                "Catalog-free Iceberg sink writes local paths only. "
                "Use LAKEHOUSE_FORMAT=delta for MinIO/S3."
            )
        return cls(table_uri=uri)

    def write(self, records: list[LakeRecord]) -> list[str]:
        if not records:
            return []
        if self.table_uri.startswith("s3://"):
            raise ValueError("IcebergPartitionSink requires a local filesystem path")
        pa = _import_arrow()
        pq = _import_parquet()
        table = rows_to_arrow_table(records_to_rows(records))
        root = Path(self.table_uri)
        root.mkdir(parents=True, exist_ok=True)
        written: list[str] = []
        df_cols = table.column_names
        if "symbol" not in df_cols or "date" not in df_cols:
            raise ValueError("Iceberg rows must include symbol and date partition columns")
        grouped: dict[tuple[str, str], list[int]] = {}
        symbols = table.column("symbol").to_pylist()
        dates = table.column("date").to_pylist()
        for idx, (symbol, date) in enumerate(zip(symbols, dates, strict=True)):
            grouped.setdefault((str(symbol), str(date)), []).append(idx)
        for (symbol, date), indices in grouped.items():
            part_dir = root / f"symbol={symbol}" / f"date={date}"
            part_dir.mkdir(parents=True, exist_ok=True)
            subset = table.take(pa.array(indices, type=pa.int64()))
            file_path = part_dir / f"part-{uuid4().hex[:12]}.parquet"
            pq.write_table(subset, file_path)
            written.append(str(file_path))
        sidecar = root / "_iceberg"
        sidecar.mkdir(parents=True, exist_ok=True)
        snap = sidecar / f"snap-{uuid4().hex[:12]}.json"
        snap.write_text(
            json.dumps(
                {
                    "format": "iceberg-sidecar",
                    "partition_by": list(self.partition_by),
                    "files": written,
                    "rows": len(records),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        logger.info("iceberg-shaped append %s rows -> %s", len(records), self.table_uri)
        return written


def _import_parquet() -> Any:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Iceberg sink requires pyarrow. Install extras: pip install -e '.[lakehouse]'"
        ) from exc
    return pq


def _resolve_table_uri(
    settings: Settings,
    *,
    layer: str,
    table: str,
    local_root: str | Path | None,
) -> str:
    if local_root is not None:
        return table_path(str(Path(local_root)), layer, table)
    configured = settings.lakehouse.uri.strip()
    if configured:
        return table_path(configured.rstrip("/"), layer, table)
    return table_path(str(Path("data") / settings.minio.bucket), layer, table)


class BronzeDeltaSink:
    def __init__(self, inner: DeltaLakeSink) -> None:
        self.inner = inner

    def write(self, records: list[BronzeRecord]) -> list[str]:
        return self.inner.write(list(records))


class SilverDeltaSink:
    def __init__(self, inner: DeltaLakeSink) -> None:
        self.inner = inner

    def write(self, records: list[SilverRecord]) -> list[str]:
        return self.inner.write(list(records))


class GoldDeltaSink:
    def __init__(self, inner: DeltaLakeSink) -> None:
        self.inner = inner

    def write(self, records: list[GoldRecord]) -> list[str]:
        return self.inner.write(list(records))


class BronzeIcebergSink:
    def __init__(self, inner: IcebergPartitionSink) -> None:
        self.inner = inner

    def write(self, records: list[BronzeRecord]) -> list[str]:
        return self.inner.write(list(records))


class SilverIcebergSink:
    def __init__(self, inner: IcebergPartitionSink) -> None:
        self.inner = inner

    def write(self, records: list[SilverRecord]) -> list[str]:
        return self.inner.write(list(records))


class GoldIcebergSink:
    def __init__(self, inner: IcebergPartitionSink) -> None:
        self.inner = inner

    def write(self, records: list[GoldRecord]) -> list[str]:
        return self.inner.write(list(records))


def _wrap(primary: Any, settings: Settings, local_root: str | Path | None) -> Any:
    from realtime_market_stream.sinks.dual import maybe_wrap_dual_write

    return maybe_wrap_dual_write(primary, settings, local_root=local_root)


def build_bronze_sink(settings: Settings, *, local_root: str | Path | None = None) -> Any:
    fmt = settings.lakehouse.format
    if fmt is LakehouseFormat.DELTA:
        sink: Any = BronzeDeltaSink(
            DeltaLakeSink.from_settings(settings, layer="bronze", local_root=local_root)
        )
    elif fmt is LakehouseFormat.ICEBERG:
        sink = BronzeIcebergSink(
            IcebergPartitionSink.from_settings(settings, layer="bronze", local_root=local_root)
        )
    else:
        from realtime_market_stream.sinks.bronze import FilesystemBronzeSink

        sink = FilesystemBronzeSink.from_settings(settings, local_root=local_root)
    return _wrap(sink, settings, local_root)


def build_silver_sink(settings: Settings, *, local_root: str | Path | None = None) -> Any:
    fmt = settings.lakehouse.format
    if fmt is LakehouseFormat.DELTA:
        sink: Any = SilverDeltaSink(
            DeltaLakeSink.from_settings(settings, layer="silver", local_root=local_root)
        )
    elif fmt is LakehouseFormat.ICEBERG:
        sink = SilverIcebergSink(
            IcebergPartitionSink.from_settings(settings, layer="silver", local_root=local_root)
        )
    else:
        from realtime_market_stream.sinks.silver import FilesystemSilverSink

        sink = FilesystemSilverSink.from_settings(settings, local_root=local_root)
    return _wrap(sink, settings, local_root)


def build_gold_sink(settings: Settings, *, local_root: str | Path | None = None) -> Any:
    fmt = settings.lakehouse.format
    if fmt is LakehouseFormat.DELTA:
        sink: Any = GoldDeltaSink(
            DeltaLakeSink.from_settings(
                settings, layer="gold", table="bars", local_root=local_root
            )
        )
    elif fmt is LakehouseFormat.ICEBERG:
        sink = GoldIcebergSink(
            IcebergPartitionSink.from_settings(
                settings, layer="gold", table="bars", local_root=local_root
            )
        )
    else:
        from realtime_market_stream.sinks.gold import FilesystemGoldSink

        sink = FilesystemGoldSink.from_settings(settings, local_root=local_root)
    return _wrap(sink, settings, local_root)
