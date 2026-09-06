"""rms_dq_checks — lakehouse freshness probe.

Heavier null / volume anomaly rules land in a later task. This DAG only
asks whether Bronze (or another view) has recent rows.
"""

from __future__ import annotations

from datetime import timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
except ImportError:  # pragma: no cover
    DAG = None  # type: ignore[misc, assignment]


def _freshness(**context: object) -> dict[str, object]:
    from realtime_market_stream.orchestration.jobs import result_as_dict, run_freshness_check

    conf: dict[str, object] = {}
    dag_run = context.get("dag_run") if isinstance(context, dict) else None
    if dag_run is not None:
        conf = dict(getattr(dag_run, "conf", None) or {})
    view = str(conf.get("view", "bronze_ticks"))
    max_age = int(conf.get("max_age_seconds", 86_400))
    data_root = conf.get("data_root")
    result = run_freshness_check(
        view=view,
        max_age_seconds=max_age,
        data_root=str(data_root) if data_root else None,
    )
    payload = result_as_dict(result)
    if result.stale:
        raise RuntimeError(f"freshness check failed: {payload}")
    return payload


if DAG is not None:
    with DAG(
        dag_id="rms_dq_checks",
        description="Lakehouse freshness check (row count + latest timestamp).",
        schedule_interval="@hourly",
        start_date=days_ago(1),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "realtime-market-stream",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        tags=["rms", "dq", "freshness"],
    ) as dag:
        PythonOperator(task_id="bronze_freshness", python_callable=_freshness)
