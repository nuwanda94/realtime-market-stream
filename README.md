# realtime-market-stream

**Local-first real-time market data streaming platform** that lands ticks into a medallion lakehouse (Bronze → Silver → Gold) with an optional Snowflake path.

Built iteratively with Conventional Commits (`feat` / `chore` / `fix`).

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-active%20development-blue)](TASKS.md)
[![CI](https://github.com/nuwanda94/realtime-market-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/nuwanda94/realtime-market-stream/actions/workflows/ci.yml)

---

## Objectives

1. **Demonstrate production-grade real-time streaming patterns**  
   End-to-end pipeline from live/synthetic market ticks through messaging, stream processing, and medallion storage — fully runnable on a laptop with zero cloud cost.

2. **Local-first by design**  
   Core path uses Redpanda + Delta Lake on MinIO + Python stream processors. Cloud services (Snowflake, AWS) are optional dual-write destinations, never required.

3. **Finance-domain relevance**  
   Realistic market tick data (OHLCV, trades), windowed aggregations, anomaly detection, and serving layer suitable for research, dashboards, or downstream feature stores / RAG systems.

4. **Clean, extensible architecture**  
   Clear separation of ingestion, processing (Bronze/Silver/Gold), sinks, serving (FastAPI + Streamlit), and observability. Easy to swap stream engines or storage formats.

5. **Portfolio & interview showcase**  
   Transparent progress via `PROJECT_PLAN.md` + `TASKS.md`, Conventional Commits, good documentation, and a one-command local experience. Complements existing work on lakehouse patterns and finance RAG.

6. **Operational excellence**  
   Schema enforcement, dead-letter handling, idempotency, data quality checks, metrics/tracing, and Airflow for supporting batch jobs.

---

## High-level Architecture

```
Live / Synthetic Market Ticks
          |
          v
   Ingestion (Python)
          |
          v
     Redpanda / Kafka
   (raw-ticks, enriched, alerts, dlq)
          |
          v
 Stream Processing (Quix Streams / Bytewax / PyFlink)
   • Bronze  – raw, schema-validated
   • Silver  – cleaned, windowed OHLC, enriched
   • Gold    – features, anomaly scores
          |
          v
 +---------------------+----------------------+
 |  Local Lakehouse    |  Optional Cloud      |
 |  Delta Lake /       |  Snowflake           |
 |  Iceberg on MinIO   |  (Snowpipe Streaming)|
 +---------------------+----------------------+
          |
          v
 FastAPI  +  Streamlit Dashboard  +  DuckDB/Polars queries
```

---

## Current Status

Active development.  
See progress and remaining work in:

- [Project Plan](PROJECT_PLAN.md) – full phased roadmap
- [Task Tracker](TASKS.md) – checklist of remaining work

Phase 0 local infra is available: `docker compose up -d` starts Redpanda, MinIO, Prometheus, Grafana, and Postgres (Airflow metadata). The `redpanda-init` job creates `raw-ticks`, `enriched-ticks`, `alerts`, and `dlq` (names overridable via `KAFKA_TOPIC_*`). You can also run `make create-topics` from the host.

Configuration uses **pydantic-settings**. Copy `.env.example` to `.env` and pick a profile:

- `APP_ENV=local` (default) — Redpanda + MinIO; Snowflake fields may stay empty.
- `APP_ENV=snowflake` — requires `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, and `SNOWFLAKE_SCHEMA`.

Load settings in code with `from realtime_market_stream.config import get_settings`.

After ingesting ticks (`make ingest`), land them in Bronze with `make bronze MAX_RECORDS=50`. Records are schema-validated and written under `data/{MINIO_BUCKET}/bronze/ticks/event_type=.../symbol=.../date=.../` (JSONL micro-batches; dedicated Delta writer is a later task). Invalid payloads go to the `dlq` topic.

CI (GitHub Actions) runs ruff, mypy, pytest (3.11 + 3.12), and a package build on every push and pull request to `main`.

---

## Tech Stack (locked decisions)

| Layer              | Choice                                      |
|--------------------|---------------------------------------------|
| Messaging          | Redpanda (Kafka-compatible)                 |
| Stream processing  | Quix Streams or Bytewax (Python-first)      |
| Table format       | Delta Lake on MinIO (Iceberg alternative)   |
| Orchestration      | Airflow                                     |
| Serving            | FastAPI + Streamlit                         |
| Query              | DuckDB / Polars                             |
| Config             | pydantic-settings                           |
| Observability      | OpenTelemetry + Prometheus + Grafana        |
| Tooling            | pyproject.toml + uv/pip, ruff, mypy, pre-commit |

---

## Repository Layout

```
realtime-market-stream/
├── src/realtime_market_stream/     # application package
│   ├── config/
│   ├── ingestion/
│   ├── processing/
│   ├── sinks/
│   ├── serving/
│   ├── schemas/
│   └── observability/
├── apps/
│   ├── dashboard/                  # Streamlit
│   └── api/                        # FastAPI entrypoint
├── infra/
│   ├── docker/
│   ├── grafana/
│   ├── prometheus/
│   └── redpanda/
├── airflow/dags/
├── scripts/
├── tests/
├── docs/
├── .github/workflows/ci.yml
├── docker-compose.yml
├── pyproject.toml
├── Makefile
├── PROJECT_PLAN.md
├── TASKS.md
└── .env.example
```

---

## Quick Start

```bash
cp .env.example .env
python3 -m venv .venv && source .venv/bin/activate
make install-dev
make test
docker compose up -d
make create-topics          # also runs automatically via redpanda-init
```

Developer loop:

```bash
make lint format typecheck test
make pre-commit-install   # once per clone
```

| URL | Service |
|-----|---------|
| http://localhost:8080 | Redpanda Console |
| http://localhost:9001 | MinIO Console (`minioadmin` / `minioadmin`) |
| http://localhost:3000 | Grafana (`admin` / `admin`) |
| http://localhost:9090 | Prometheus |
| localhost:19092 | Redpanda Kafka API |
| localhost:5432 | Postgres (`airflow` / `airflow`) |

See [infra/docker/README.md](infra/docker/README.md) for details.

---

## Author

**Karan Verma** ([@nuwanda94](https://github.com/nuwanda94))  
Data Engineer @ Morningstar  
Snowflake • AWS • Airflow • Python • SQL

Related work:
- [data-lakehouse-ministack](https://github.com/nuwanda94/data-lakehouse-ministack) – local medallion lakehouse
- [fintruth-rag](https://github.com/nuwanda94/fintruth-rag) – SEC-grounded financial research assistant

---

## License

MIT – see [LICENSE](LICENSE).
