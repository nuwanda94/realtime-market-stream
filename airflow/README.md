# Airflow DAGs

Local-first orchestration. Job bodies live in
`src/realtime_market_stream/orchestration/jobs.py` so they run in CI and on a
laptop **without** installing Apache Airflow.

| DAG id | Schedule | Purpose |
|--------|----------|---------|
| `rms_backfill` | daily | Schema gate then synthetic Bronze backfill |
| `rms_dq_checks` | hourly | Freshness, required-field nulls, volume IQR |
| `rms_schema_evolution` | daily | Fail if bundled JSON Schema drifts from Pydantic |

## Run jobs without Airflow

```bash
make backfill COUNT=20 DATA_ROOT=./data/market-lake
make schema-check
make freshness DATA_ROOT=./data/market-lake
make dq DATA_ROOT=./data/market-lake VIEW=bronze_ticks
```

Trigger parameters (when using a real Airflow scheduler) via DAG run conf:

```json
{
  "count": 50,
  "data_root": "/opt/airflow/data/market-lake",
  "view": "bronze_ticks",
  "max_age_seconds": 86400,
  "iqr_multiplier": 3.0
}
```

## Point a local Airflow at these files

Postgres for metadata is already in `docker-compose.yml`. A full Airflow
webserver/scheduler is intentionally **not** bundled (image size / RAM). To
use your own Airflow:

```bash
export AIRFLOW_HOME=/tmp/rms-airflow
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
# airflow standalone   # optional; needs apache-airflow installed separately
```

Keep `PYTHONPATH` pointed at `src/` so DAG callables can import
`realtime_market_stream`.
