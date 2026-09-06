from __future__ import annotations

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover - optional Airflow dependency
    DAG = None  # type: ignore[misc, assignment]
    PythonOperator = None  # type: ignore[misc, assignment]

from datetime import datetime

from realtime_market_stream.orchestration.jobs import run_schema_evolution_check, result_as_dict


def _task_schema_check(**context):
    result = run_schema_evolution_check()
    print(result_as_dict(result))
    if not result.ok:
        raise RuntimeError(f"Schema evolution check failed: {result.details}")
    return result_as_dict(result)


if DAG is not None and PythonOperator is not None:
    with DAG(
        dag_id="rms_schema_evolution",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["realtime-market-stream", "schema"],
    ) as dag:
        PythonOperator(
            task_id="schema_evolution_check",
            python_callable=_task_schema_check,
        )
