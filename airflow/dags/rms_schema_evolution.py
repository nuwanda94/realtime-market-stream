"""rms_schema_evolution — bundled JSON Schema vs live Pydantic models."""

from __future__ import annotations

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
except ImportError:  # pragma: no cover
    DAG = None  # type: ignore[misc, assignment]


def _check(**_context: object) -> dict[str, object]:
    from realtime_market_stream.orchestration.jobs import (
        result_as_dict,
        run_schema_evolution_check,
    )

    result = run_schema_evolution_check()
    payload = result_as_dict(result)
    if not result.ok:
        raise RuntimeError(f"schema drift detected: {payload}")
    return payload


if DAG is not None:
    with DAG(
        dag_id="rms_schema_evolution",
        description="Fail when bundled JSON Schema drifts from Pydantic models.",
        schedule_interval="@daily",
        start_date=days_ago(1),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "realtime-market-stream",
            "retries": 0,
        },
        tags=["rms", "schema"],
    ) as dag:
        PythonOperator(task_id="compare_bundled_schemas", python_callable=_check)
