# Task Tracker

Checklist of remaining work for the real-time market data streaming platform.

Use Conventional Commits (`feat:`, `chore:`, `fix:`) when implementing items.

## Current Progress

### Phase 0 – Foundation & Scaffolding
- [x] chore: Initialize repository structure, MIT license, .gitignore, basic README, folder layout
- [x] chore: Docker Compose base (Redpanda, MinIO, Prometheus, Grafana, Postgres for Airflow) — done 2026-09-05 (`docker-compose.yml` + infra provisioning)
- [ ] chore: Project tooling (pyproject.toml / uv or Poetry, pre-commit, ruff, mypy, Makefile)
- [ ] chore: CI skeleton (GitHub Actions: lint, test, build)
- [ ] chore: Config management (pydantic-settings, local vs Snowflake profiles)
- [ ] feat: Synthetic tick generator (realistic OHLCV + trade ticks, configurable rate)
- [ ] chore: Topic creation script / auto-create Kafka topics

### Phase 1 – Core Streaming Pipeline
- [ ] feat: Ingestion service
- [ ] feat: Stream processor Bronze
- [ ] feat: Stream processor Silver
- [ ] feat: Delta / Iceberg sink
- [ ] chore: Schema registry / JSON Schema
- [ ] feat: Dead-letter queue handling + replay tool
- [ ] fix: Idempotency & exactly-once
- [ ] chore: Unit + integration tests

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
