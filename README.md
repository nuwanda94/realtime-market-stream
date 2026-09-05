# realtime-market-stream

Local-first real-time market data streaming platform → medallion lakehouse (Delta Lake / Iceberg) with optional Snowflake sink.

**Status**: Under active automated construction following the plan in `PROJECT_PLAN.md` and tracker in `TASKS.md`.

One task is implemented per hourly automation run using Conventional Commits (`feat` / `chore` / `fix`).

## Quick links
- [Project Plan](PROJECT_PLAN.md)
- [Task Tracker](TASKS.md)

## Vision
Ingest live or synthetic market ticks → Redpanda → stream processing (Quix Streams / Bytewax) → Bronze / Silver / Gold tables on MinIO (Delta) → FastAPI + Streamlit dashboard. Optional dual-write to Snowflake. Fully local, zero cloud cost for the core path.

Built by Karan Verma (@nuwanda94) – Data Engineer @ Morningstar.
