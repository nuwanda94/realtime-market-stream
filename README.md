# realtime-market-stream

**Local-first real-time market data streaming platform** that lands ticks into a medallion lakehouse (Bronze → Silver → Gold) with an optional Snowflake path.

Built iteratively with Conventional Commits (`feat` / `chore` / `fix`).

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-active%20development-blue)](TASKS.md)

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
          │
          ▼
   Ingestion (Python)
          │
          ▼
     Redpanda / Kafka
   (raw-ticks, enriched, alerts, dlq)
          │
          ▼
 Stream Processing (Quix Streams / Bytewax / PyFlink)
   • Bronze  – raw, schema-validated
   • Silver  – cleaned, windowed OHLC, enriched
   • Gold    – features, anomaly scores
          │
          ▼
 ┌─────────────────────┬──────────────────────┐
 │  Local Lakehouse    │  Optional Cloud      │
 │  Delta Lake /       │  Snowflake           │
 │  Iceberg on MinIO   │  (Snowpipe Streaming)│
 └─────────────────────┴──────────────────────┘
          │
          ▼
 FastAPI  +  Streamlit Dashboard  +  DuckDB/Polars queries
```

---

## Current Status

Active development.  
See progress and remaining work in:

- [Project Plan](PROJECT_PLAN.md) – full phased roadmap
- [Task Tracker](TASKS.md) – checklist of remaining work

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
├── PROJECT_PLAN.md
├── TASKS.md
└── .env.example
```

---

## Quick Start (once scaffolding is complete)

```bash
cp .env.example .env
make up          # starts Redpanda, MinIO, etc.
make produce     # synthetic ticks
make dashboard   # open Streamlit
```

Detailed instructions will appear here as the corresponding tasks are completed.

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
