"""DuckDB / Polars query layer over the medallion lakehouse.

Local-first and zero-cloud-cost: scan Hive-partitioned JSONL (and parquet
when present) under ``data/{bucket}/{bronze|silver|gold}/...``.

Backends:

* ``jsonl`` — always available; reuses :class:`LakehouseStore`.
* ``duckDB`` — optional (``pip install -e '.[query]'``). Registers SQL views
  over JSONL/parquet globs and runs read-only ``SELECT`` / ``WITH``.
* ``polars`` — optional; materializes the same scans as a DataFrame.

``LAKEHOUSE_FORMAT=delta`` tables can be queried via DuckDB ``delta_scan``
when ``deltalake`` is installed; otherwise the JSONL/parquet glob is used.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from realtime_market_stream.config.settings import Settings
from realtime_market_stream.serving.store import LakehouseStore, lakehouse_root

logger = logging.getLogger(__name__)

_FORBIDDEN_SQL = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "attach",
    "copy",
    "pragma",
    "call",
    "install",
    "load",
    "export",
)


class QueryBackend(StrEnum):
    """Physical engine used to materialize rows."""

    JSONL = "jsonl"
    DUCKDB = "duckdb"
    POLARS = "polars"


@dataclass(frozen=True)
class LakehouseView:
    """Named scan over a lakehouse layer."""

    name: str
    layer_glob: str
    description: str
    event_type: str | None = None
    extra_filter: Callable[[dict[str, Any]], bool] | None = None


VIEWS: dict[str, LakehouseView] = {
    "bronze_ticks": LakehouseView(
        name="bronze_ticks",
        layer_glob="bronze/ticks/event_type=trade/**/*.{jsonl,json,parquet}",
        description="Bronze trade ticks",
        event_type="trade",
    ),
    "silver_ticks": LakehouseView(
        name="silver_ticks",
        layer_glob="silver/ticks/event_type=trade/**/*.{jsonl,json,parquet}",
        description="Silver enriched trade ticks",
        event_type="trade",
    ),
    "silver_ohlc": LakehouseView(
        name="silver_ohlc",
        layer_glob="silver/ticks/event_type=ohlcv/**/*.{jsonl,json,parquet}",
        description="Silver tumbling OHLCV bars",
        event_type="ohlcv",
    ),
    "gold_bars": LakehouseView(
        name="gold_bars",
        layer_glob="gold/bars/**/*.{jsonl,json,parquet}",
        description="Gold scored bars",
    ),
    "gold_anomalies": LakehouseView(
        name="gold_anomalies",
        layer_glob="gold/bars/**/*.{jsonl,json,parquet}",
        description="Gold bars flagged is_anomaly",
        extra_filter=lambda row: row.get("is_anomaly") in {True, "true", 1, "1"},
    ),
}


def detect_backends() -> dict[str, bool]:
    """Report which optional query engines can be imported."""

    available = {"jsonl": True, "duckdb": False, "polars": False}
    try:
        import duckdb  # noqa: F401

        available["duckdb"] = True
    except ImportError:
        pass
    try:
        import polars  # noqa: F401

        available["polars"] = True
    except ImportError:
        pass
    return available


def resolve_backend(requested: str = "auto") -> QueryBackend:
    requested = (requested or "auto").strip().lower()
    available = detect_backends()
    if requested == "auto":
        if available["duckdb"]:
            return QueryBackend.DUCKDB
        if available["polars"]:
            return QueryBackend.POLARS
        return QueryBackend.JSONL
    try:
        backend = QueryBackend(requested)
    except ValueError as exc:
        raise ValueError(
            f"Unknown query backend {requested!r}. Use auto, jsonl, duckdb, or polars."
        ) from exc
    if backend is QueryBackend.DUCKDB and not available["duckdb"]:
        raise RuntimeError("DuckDB is not installed. pip install -e '.[query]'")
    if backend is QueryBackend.POLARS and not available["polars"]:
        raise RuntimeError("Polars is not installed. pip install -e '.[query]'")
    return backend


def _row_ts(row: dict[str, Any]) -> str:
    for key in ("ts", "window_start", "window_end", "ingested_at"):
        value = row.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _assert_readonly_sql(sql: str) -> str:
    stripped = sql.strip().rstrip(";")
    if not stripped:
        raise ValueError("SQL is empty")
    head = stripped.split(None, 1)[0].lower()
    if head not in {"select", "with", "describe", "show", "explain"}:
        raise ValueError("Only read-only SELECT / WITH / DESCRIBE / SHOW / EXPLAIN are allowed")
    lowered = stripped.lower()
    for token in _FORBIDDEN_SQL:
        if f" {token} " in f" {lowered} ":
            raise ValueError(f"SQL contains forbidden keyword: {token}")
    return stripped


class QueryEngine:
    """Read lakehouse views via JSONL fallback, DuckDB SQL, or Polars."""

    def __init__(
        self,
        root: str | Path,
        *,
        backend: str | QueryBackend = "auto",
    ) -> None:
        self.root = Path(root)
        self.store = LakehouseStore(self.root)
        if isinstance(backend, QueryBackend):
            self.backend = backend
        else:
            try:
                self.backend = resolve_backend(backend)
            except RuntimeError:
                logger.warning("Requested backend unavailable; falling back to jsonl")
                self.backend = QueryBackend.JSONL

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        local_root: str | Path | None = None,
        backend: str = "auto",
    ) -> QueryEngine:
        return cls(lakehouse_root(settings, local_root=local_root), backend=backend)

    def list_views(self) -> list[dict[str, str]]:
        return [
            {
                "name": view.name,
                "glob": view.layer_glob,
                "description": view.description,
            }
            for view in VIEWS.values()
        ]

    def scan(
        self,
        view_name: str,
        *,
        symbol: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if view_name not in VIEWS:
            raise KeyError(f"Unknown view {view_name!r}. Known: {sorted(VIEWS)}")
        view = VIEWS[view_name]
        if self.backend is QueryBackend.DUCKDB:
            try:
                return self._scan_duckdb(view, symbol=symbol, limit=limit)
            except Exception as exc:  # noqa: BLE001
                logger.warning("DuckDB scan failed (%s); using JSONL fallback", exc)
        if self.backend is QueryBackend.POLARS:
            try:
                return self._scan_polars(view, symbol=symbol, limit=limit)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Polars scan failed (%s); using JSONL fallback", exc)
        return self._scan_jsonl(view, symbol=symbol, limit=limit)

    def sql(self, statement: str, parameters: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        """Run a read-only DuckDB statement against registered lakehouse views."""

        safe = _assert_readonly_sql(statement)
        duckdb = _import_duckdb()
        con = duckdb.connect(database=":memory:")
        try:
            self._register_duckdb_views(con)
            if parameters:
                relation = con.execute(safe, dict(parameters))
            else:
                relation = con.execute(safe)
            return _relation_to_rows(relation)
        finally:
            con.close()

    def to_polars(self, view_name: str, *, symbol: str | None = None, limit: int = 1000) -> Any:
        """Materialize a view as a Polars DataFrame (requires polars)."""

        pl = _import_polars()
        rows = self.scan(view_name, symbol=symbol, limit=limit)
        return pl.DataFrame(rows)

    def latest_ticks(
        self,
        *,
        symbol: str | None = None,
        limit: int = 50,
        layer: str = "bronze",
    ) -> list[dict[str, Any]]:
        view = "silver_ticks" if layer.lower() == "silver" else "bronze_ticks"
        return self.scan(view, symbol=symbol, limit=limit)

    def ohlc_bars(self, *, symbol: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        return self.scan("silver_ohlc", symbol=symbol, limit=limit)

    def anomalies(self, *, symbol: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        return self.scan("gold_anomalies", symbol=symbol, limit=limit)

    def _scan_jsonl(
        self,
        view: LakehouseView,
        *,
        symbol: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        # Store globs are JSONL-only; strip the brace expansion.
        jsonl_glob = view.layer_glob.replace(".{jsonl,json,parquet}", ".jsonl")
        return self.store._scan(  # noqa: SLF001 — shared scanner
            jsonl_glob,
            symbol=symbol,
            event_type=view.event_type,
            extra_filter=view.extra_filter,
            limit=limit,
        )

    def _scan_duckdb(
        self,
        view: LakehouseView,
        *,
        symbol: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        duckdb = _import_duckdb()
        con = duckdb.connect(database=":memory:")
        try:
            relation_sql = self._duckdb_source_sql(view)
            clauses = [f"SELECT * FROM ({relation_sql}) src"]
            params: dict[str, Any] = {"lim": int(limit)}
            if symbol:
                clauses.append("WHERE upper(CAST(src.symbol AS VARCHAR)) = upper($sym)")
                params["sym"] = symbol
            if view.extra_filter is not None and view.name == "gold_anomalies":
                joiner = "AND" if symbol else "WHERE"
                clauses.append(
                    f"{joiner} (CAST(src.is_anomaly AS VARCHAR) IN ('true', 'True', '1'))"
                )
            clauses.append("ORDER BY COALESCE(src.ts, src.window_start, src.ingested_at) DESC")
            clauses.append("LIMIT $lim")
            relation = con.execute(" ".join(clauses), params)
            return _relation_to_rows(relation)
        finally:
            con.close()

    def _scan_polars(
        self,
        view: LakehouseView,
        *,
        symbol: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        pl = _import_polars()
        files = self._matching_files(view)
        if not files:
            return []
        frames: list[Any] = []
        for path in files:
            if path.suffix == ".parquet":
                frames.append(pl.read_parquet(path))
            else:
                frames.append(pl.read_ndjson(path))
        if not frames:
            return []
        frame = pl.concat(frames, how="diagonal_relaxed")
        if symbol and "symbol" in frame.columns:
            frame = frame.filter(pl.col("symbol").cast(pl.Utf8).str.to_uppercase() == symbol.upper())
        if view.extra_filter is not None:
            rows = [row for row in frame.to_dicts() if view.extra_filter(row)]
        else:
            rows = frame.to_dicts()
        rows.sort(key=_row_ts, reverse=True)
        return rows[:limit]

    def _matching_files(self, view: LakehouseView) -> list[Path]:
        if not self.root.exists():
            return []
        # pathlib does not expand {a,b}; search both families.
        found: list[Path] = []
        for pattern in (
            view.layer_glob.replace(".{jsonl,json,parquet}", ".jsonl"),
            view.layer_glob.replace(".{jsonl,json,parquet}", ".json"),
            view.layer_glob.replace(".{jsonl,json,parquet}", ".parquet"),
        ):
            found.extend(path for path in self.root.glob(pattern) if path.is_file())
        return found

    def _duckdb_source_sql(self, view: LakehouseView) -> str:
        files = self._matching_files(view)
        if not files:
            return "SELECT * FROM (SELECT NULL AS symbol WHERE FALSE)"
        jsonl = [p for p in files if p.suffix in {".jsonl", ".json"}]
        parquet = [p for p in files if p.suffix == ".parquet"]
        parts: list[str] = []
        if jsonl:
            listed = ", ".join(_sql_quote(str(p)) for p in jsonl)
            parts.append(f"SELECT * FROM read_json_auto([{listed}], union_by_name=true)")
        if parquet:
            listed = ", ".join(_sql_quote(str(p)) for p in parquet)
            parts.append(f"SELECT * FROM read_parquet([{listed}], union_by_name=true)")
        if len(parts) == 1:
            return parts[0]
        return " UNION ALL BY NAME ".join(parts)

    def _register_duckdb_views(self, con: Any) -> None:
        for view in VIEWS.values():
            source = self._duckdb_source_sql(view)
            if view.name == "gold_anomalies":
                source = (
                    f"SELECT * FROM ({source}) src "
                    "WHERE CAST(src.is_anomaly AS VARCHAR) IN ('true', 'True', '1')"
                )
            con.execute(f"CREATE OR REPLACE VIEW {view.name} AS {source}")


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _relation_to_rows(relation: Any) -> list[dict[str, Any]]:
    description = relation.description or []
    columns = [col[0] for col in description]
    rows: list[dict[str, Any]] = []
    for values in relation.fetchall():
        rows.append(dict(zip(columns, values, strict=False)))
    return rows


def _import_duckdb() -> Any:
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("DuckDB is not installed. pip install -e '.[query]'") from exc
    return duckdb


def _import_polars() -> Any:
    try:
        import polars
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Polars is not installed. pip install -e '.[query]'") from exc
    return polars
