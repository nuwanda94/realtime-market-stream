# realtime-market-stream

Local-first real-time market data streaming platform → medallion lakehouse (Delta Lake / Iceberg) with optional Snowflake sink.

**Status**: Under active automated construction following the plan in `PROJECT_PLAN.md` and tracker in `TASKS.md`.

One task is implemented per hourly automation run using Conventional Commits (`feat` / `chore` / `fix`).

## Quick links
- [Project Plan](PROJECT_PLAN.md)
- [Task Tracker](TASKS.md)

## Vision
Ingest live or synthetic market ticks → Redpanda → stream processing (Quix Streams / Bytewax) → Bronze / Silver / Gold tables on MinIO (Delta) → FastAPI + Streamlit dashboard. Optional dual-write to Snowflake. Fully local, zero cloud cost for the core path.

## Repository layout

```
realtime-market-stream/
├── src/realtime_market_stream/   # application package
│   ├── config/                   # pydantic-settings (later)
│   ├── ingestion/                # websocket + synthetic producer
│   ├── processing/               # bronze / silver / gold stream jobs
│   ├── sinks/                    # Delta / Iceberg / Snowflake writers
│   ├── serving/                  # FastAPI + query layer
│   ├── schemas/                  # tick / OHLC / alert models
│   └── observability/            # metrics, tracing helpers
├── apps/
│   ├── dashboard/                # Streamlit UI
│   └── api/                      # FastAPI entrypoint wrapper
├── infra/
│   ├── docker/                   # compose + service Dockerfiles
│   ├── grafana/
│   ├── prometheus/
│   └── redpanda/
├── airflow/dags/
├── scripts/                      # topics, replay, backfill
├── tests/
├── docs/
└── .env.example
```

Copy `.env.example` to `.env` before running local services. Do not commit real credentials.

Built by Karan Verma (@nuwanda94) – Data Engineer @ Morningstar.
