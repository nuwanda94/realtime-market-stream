"""rms_backfill — seed Bronze from the synthetic generator.

Parse-safe without Airflow: the DAG object is only constructed when
``airflow`` is importable. Job bodies live in
``realtime_market_stream.orchestration.jobs``.
"""

from __future__ import annotations

from datetime import timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
except ImportError:  # pragma: no cover - local / CI without Airflow
    DAG = None  # type: ignore[misc, assignment]


def _backfill(**context: object) -> dict[str, object]:
    from realtime_market_stream.orchestration.jobs import result_as_dict, run_backfill

    conf = {}
    dag_run = context.get("dag_run") if isinstance(context, dict) else None
    if dag_run is not None:
        conf = getattr(dag_run, "conf", None) or {}
    count = int(conf.get("count", 20)) if isinstance(conf, dict) else 20
    data_root = conf.get("data_root") if isinstance(conf, dict) else None
    result = run_backfill(count=count, data_root=data_root)
    return result_as_dict(result)


def _schema(**_context: object) -> dict[str, object]:
    from realtime_market_stream.orchestration.jobs import result_as_dict, run_schema_evolution_check

    return result_as_dict(run_schema_evolution_check())


if DAG is not None:
    with DAG(
        dag_id="rms_backfill",
        description="Generate synthetic ticks and land them in Bronze (local-first).",
        schedule_interval="@daily",
        start_date=days_ago(1),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "realtime-market-stream",
            "retries": 1,
            "retry_delay": timedelta(minutes=2),
        },
        tags=["rms", "backfill", "bronze"],
    ) as dag:
        schema_gate = PythonOperator(
            task_id="schema_evolution_gate",
            python_callable=_schema,
        )
        backfill = PythonOperator(
            task_id="synthetic_bronze_backfill",
            python_callable=_backfill,
        )
        schema_gate >> backfill
