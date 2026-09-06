"""rms_dq_checks — freshness, nulls, and volume-anomaly rules.

Job body lives in ``orchestration.jobs.run_data_quality_check`` so the same
rules run locally via ``make dq`` without Airflow.
"""

from __future__ import annotations

from datetime import timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
except ImportError:  # pragma: no cover
    DAG = None  # type: ignore[misc, assignment]


def _quality(**context: object) -> dict[str, object]:
    from realtime_market_stream.orchestration.jobs import result_as_dict, run_data_quality_check

    conf: dict[str, object] = {}
    dag_run = context.get("dag_run") if isinstance(context, dict) else None
    if dag_run is not None:
        conf = dict(getattr(dag_run, "conf", None) or {})
    view = str(conf.get("view", "bronze_ticks"))
    max_age = int(conf.get("max_age_seconds", 86_400))
    iqr = float(conf.get("iqr_multiplier", 3.0))
    data_root = conf.get("data_root")
    result = run_data_quality_check(
        view=view,
        max_age_seconds=max_age,
        iqr_multiplier=iqr,
        data_root=str(data_root) if data_root else None,
    )
    payload = result_as_dict(result)
    if not result.ok:
        raise RuntimeError(f"data quality check failed: {payload}")
    return payload


if DAG is not None:
    with DAG(
        dag_id="rms_dq_checks",
        description="Lakehouse DQ: freshness, required-field nulls, volume IQR.",
        schedule_interval="@hourly",
        start_date=days_ago(1),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "realtime-market-stream",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        tags=["rms", "dq", "freshness", "quality"],
    ) as dag:
        PythonOperator(task_id="data_quality", python_callable=_quality)
