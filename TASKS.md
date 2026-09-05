# Task Tracker

Checklist of remaining work for the real-time market data streaming platform.

Use Conventional Commits (`feat:`, `chore:`, `fix:`) when implementing items.

## Current Progress

### Phase 0 – Foundation & Scaffolding
- [x] chore: Initialize repository structure, MIT license, .gitignore, basic README, folder layout
- [x] chore: Docker Compose base (Redpanda, MinIO, Prometheus, Grafana, Postgres for Airflow) — done 2026-09-05 (`docker-compose.yml` + infra provisioning)
- [x] chore: Project tooling (pyproject.toml / uv or Poetry, pre-commit, ruff, mypy, Makefile) — done 2026-09-05 (`pyproject.toml`, `.pre-commit-config.yaml`, `Makefile`)
- [x] chore: CI skeleton (GitHub Actions: lint, test, build) — done 2026-09-05 (`.github/workflows/ci.yml`)
- [x] chore: Config management (pydantic-settings, local vs Snowflake profiles) — done 2026-09-05 (`src/realtime_market_stream/config/settings.py`)
- [x] feat: Synthetic tick generator (realistic OHLCV + trade ticks, configurable rate) — done 2026-09-05 (`src/realtime_market_stream/ingestion/generator.py`)
- [x] chore: Topic creation script / auto-create Kafka topics — done 2026-09-05 (`scripts/create_topics.py`, `src/realtime_market_stream/ingestion/topics.py`, Compose `redpanda-init`)

### Phase 1 – Core Streaming Pipeline
- [x] feat: Ingestion service — done 2026-09-05 (`src/realtime_market_stream/ingestion/service.py`, websocket + synthetic fallback, schema validation, DLQ)
- [x] feat: Stream processor Bronze — done 2026-09-05 (`src/realtime_market_stream/processing/bronze.py`, partitioned sink, DLQ on schema failure)
- [x] feat: Stream processor Silver — done 2026-09-05 (`src/realtime_market_stream/processing/silver.py`, dedup, enrichment, tumbling OHLC, `enriched-ticks`)
- [x] feat: Delta / Iceberg sink — done 2026-09-05 (`src/realtime_market_stream/sinks/delta.py`, symbol+date partitions, `LAKEHOUSE_FORMAT=delta|iceberg|jsonl`)
- [x] chore: Schema registry / JSON Schema — done 2026-09-06 (`src/realtime_market_stream/schemas/registry.py`, bundled `schemas/json/*.v1.json`, optional `SCHEMA_REGISTRY_URL`)
- [x] feat: Dead-letter queue handling + replay tool — done 2026-09-06 (`src/realtime_market_stream/processing/dlq.py`, `scripts/run_dlq.py`, `make replay-dlq`)
- [x] fix: Idempotency & exactly-once — done 2026-09-06 (`processing/checkpoint.py`, manual Kafka commit after sink write, Silver state restore)
- [x] chore: Unit + integration tests — done 2026-09-06 (`tests/unit/*`, `tests/integration/test_local_pipeline.py`, checkpoint + in-process pipeline)

### Phase 2 – Gold Layer, Analytics & Serving
- [ ] feat: Gold aggregations + anomaly scores
- [ ] feat: FastAPI service
- [ ] feat: Live Streamlit dashboard
- [ ] feat: Query layer (DuckDB / Polars)
- [ ] chore: OpenTelemetry instrumentation
- [ ] fix: Backpressure & rate limiting

### Phase 3 – Snowflake Integration & Production Hardening
- [ ] feat: Snowflake Streaming sink
- [ ] feat: Dual-write mode
- [ ] chore: Snowflake setup scripts
- [ ] feat: Airflow DAGs
- [ ] fix: Data quality rules
- [ ] chore: Cost & performance documentation

### Phase 4 – Polish, Docs, Observability & Release
- [ ] chore: Architecture docs & diagrams
- [ ] chore: Excellent README
- [ ] feat: Replay & backfill tooling
- [ ] feat: Simple alerting
- [ ] fix: Performance tuning
- [ ] chore: Contribution guide + issue templates
- [ ] chore: Release packaging
- [ ] fix: Edge cases
