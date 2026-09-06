# Real-time Market Data Streaming Platform – Project Plan

**Repo**: https://github.com/nuwanda94/realtime-market-stream  
**Owner**: Karan Verma (@nuwanda94)  
**Goal**: Local-first real-time market data pipeline (synthetic + live ticks) → Redpanda/Kafka → stream processing → medallion lakehouse (Bronze/Silver/Gold on Delta/Iceberg) with optional Snowflake sink, FastAPI + live dashboard, Airflow orchestration, full observability.

**Conventions**: feat / chore / fix (Conventional Commits).

Progress is tracked in `TASKS.md`.

---

## Phase 0 – Foundation & Scaffolding (v0.1-scaffold)

- [x] **chore**: Initialize repository structure, MIT license, .gitignore, basic README, folder layout
- [x] **chore**: Docker Compose base (Redpanda, MinIO, Prometheus, Grafana, Postgres for Airflow)
- [x] **chore**: Project tooling (pyproject.toml / uv or Poetry, pre-commit, ruff, mypy, Makefile) — 2026-09-05
- [x] **chore**: CI skeleton (GitHub Actions: lint, test, build) — 2026-09-05 (`.github/workflows/ci.yml`)
- [x] **chore**: Config management (pydantic-settings, local vs Snowflake profiles) — 2026-09-05
- [x] **feat**: Synthetic tick generator (realistic OHLCV + trade ticks, configurable rate) — 2026-09-05
- [x] **chore**: Topic creation script / auto-create Kafka topics (raw-ticks, enriched-ticks, alerts, dlq) — 2026-09-05

## Phase 1 – Core Streaming Pipeline (v0.2-core-stream)

- [x] **feat**: Ingestion service (websocket client + synthetic fallback, schema validation) — 2026-09-05
- [x] **feat**: Stream processor Bronze (deserialize, schema enforce, write raw to partitioned lakehouse) — 2026-09-05
- [x] **feat**: Stream processor Silver (windowed OHLC, volume, dedup, enrichment) — 2026-09-05
- [x] **feat**: Delta / Iceberg sink with partitioning (symbol + date) — 2026-09-05 (`sinks/delta.py`)
- [x] **chore**: Schema registry / JSON Schema support — 2026-09-06 (`schemas/registry.py`, bundled JSON Schema v1)
- [x] **feat**: Dead-letter queue handling + replay tool — 2026-09-06 (`processing/dlq.py`, `scripts/run_dlq.py`)
- [x] **fix**: Idempotency & exactly-once (checkpointing) — 2026-09-06 (`processing/checkpoint.py`)
- [x] **chore**: Unit + integration tests (pytest unit + in-process pipeline; optional live Redpanda via `-m broker`) — 2026-09-06

## Phase 2 – Gold Layer, Analytics & Serving (v0.3-serving)

- [x] **feat**: Gold aggregations + anomaly scores (z-score / IQR) — 2026-09-06 (`processing/gold.py`, `sinks/gold.py`)
- [x] **feat**: FastAPI service (latest ticks, OHLC, anomalies, health, metrics) — 2026-09-06 (`serving/api.py`, `make api`)
- [x] **feat**: Live Streamlit dashboard (charts, alerts, latency metrics) — 2026-09-06 (`serving/dashboard.py`, `apps/dashboard/app.py`, `make dashboard`)
- [ ] **feat**: Query layer (DuckDB / Polars views over Delta)
- [ ] **chore**: OpenTelemetry instrumentation
- [ ] **fix**: Backpressure & rate limiting

## Phase 3 – Snowflake Integration & Production Hardening (v0.4-snowflake)

- [ ] **feat**: Snowflake Streaming sink (Snowpipe Streaming or Kafka connector path)
- [ ] **feat**: Dual-write mode (local Delta + optional Snowflake)
- [ ] **chore**: Snowflake setup scripts (tables, stages, pipes, RBAC examples)
- [ ] **feat**: Airflow DAGs (backfill, DQ checks, schema evolution)
- [ ] **fix**: Data quality rules (freshness, nulls, volume anomalies)
- [ ] **chore**: Cost & performance documentation

## Phase 4 – Polish, Docs, Observability & Release (v1.0)

- [ ] **chore**: Architecture docs & diagrams (Mermaid)
- [ ] **chore**: Excellent README (one-command start, demo notes, architecture)
- [ ] **feat**: Replay & backfill tooling
- [ ] **feat**: Simple alerting (webhook / Slack)
- [ ] **fix**: Performance tuning
- [ ] **chore**: Contribution guide + issue templates
- [ ] **chore**: Release packaging (Docker images, changelog)
- [ ] **fix**: Edge cases (network flaps, late data, schema evolution)

---

**Tech decisions (locked)**:
- Messaging: Redpanda (Kafka-compatible)
- Stream processing (Python-first): Quix Streams or Bytewax preferred for velocity
- Table format: Delta Lake on MinIO (Iceberg alternative OK)
- Orchestration: Airflow
- Dashboard: Streamlit
- Config: pydantic-settings
