# Task Tracker – Single Item Per Run

**Instructions for the automation agent**:

1. Read this file and `PROJECT_PLAN.md`.
2. Find the **first unchecked** task (top to bottom, Phase 0 → 4).
3. Implement **only that one task**.
4. After successful implementation and push, mark it done by changing `- [ ]` to `- [x]` and add a short note (commit SHA or date).
5. Commit message must follow Conventional Commits: `feat: ...`, `chore: ...`, or `fix: ...`.
6. Prefer creating a feature branch `feat/xxx`, `chore/xxx` or `fix/xxx`, then open a PR **or** push directly to `main` if the change is small and safe. For the automation, direct push to `main` with clear message is acceptable to keep velocity high.
7. Never implement more than one task in a single run.
8. If a task is blocked (missing dependency), skip to the next unblocked one and leave a note.
9. Keep the local-first, zero-cloud-cost principle for the core path.

## Current Progress

### Phase 0
- [ ] chore: Initialize repository structure, MIT license, .gitignore, basic README, folder layout
- [ ] chore: Docker Compose base (Redpanda, MinIO, Prometheus, Grafana, Postgres for Airflow)
- [ ] chore: Project tooling (pyproject.toml / uv or Poetry, pre-commit, ruff, mypy, Makefile)
- [ ] chore: CI skeleton (GitHub Actions: lint, test, build)
- [ ] chore: Config management (pydantic-settings, local vs Snowflake profiles)
- [ ] feat: Synthetic tick generator (realistic OHLCV + trade ticks, configurable rate)
- [ ] chore: Topic creation script / auto-create Kafka topics

### Phase 1
- [ ] feat: Ingestion service
- [ ] feat: Stream processor Bronze
- [ ] feat: Stream processor Silver
- [ ] feat: Delta / Iceberg sink
- [ ] chore: Schema registry / JSON Schema
- [ ] feat: Dead-letter queue handling + replay tool
- [ ] fix: Idempotency & exactly-once
- [ ] chore: Unit + integration tests

### Phase 2
- [ ] feat: Gold aggregations + anomaly scores
- [ ] feat: FastAPI service
- [ ] feat: Live Streamlit dashboard
- [ ] feat: Query layer (DuckDB / Polars)
- [ ] chore: OpenTelemetry instrumentation
- [ ] fix: Backpressure & rate limiting

### Phase 3
- [ ] feat: Snowflake Streaming sink
- [ ] feat: Dual-write mode
- [ ] chore: Snowflake setup scripts
- [ ] feat: Airflow DAGs
- [ ] fix: Data quality rules
- [ ] chore: Cost & performance documentation

### Phase 4
- [ ] chore: Architecture docs & diagrams
- [ ] chore: Excellent README
- [ ] feat: Replay & backfill tooling
- [ ] feat: Simple alerting
- [ ] fix: Performance tuning
- [ ] chore: Contribution guide + issue templates
- [ ] chore: Release packaging
- [ ] fix: Edge cases
