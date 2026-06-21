"""
sample_platform_dag.py

A minimal DAG that demonstrates the platform is wired up correctly: it checks
that Trino and MinIO are reachable from inside the cluster network, then logs
success. This is a smoke test — replace it with your own pipeline logic, or
see iceberg_etl_dag.py for a DAG that moves real data through Trino.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sample_platform_health_check",
    default_args=default_args,
    description="Smoke test that the platform services are reachable",
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["platform", "sample"],
) as dag:

    # Keep these commands simple and quote-safe: a plain curl with -f makes the
    # task fail on any non-2xx response, which is all the smoke test needs.
    check_trino = BashOperator(
        task_id="check_trino",
        bash_command="curl -sf http://trino:8080/v1/info && echo ' <- Trino reachable'",
    )

    check_minio = BashOperator(
        task_id="check_minio",
        bash_command="curl -sf http://minio:9000/minio/health/live && echo 'MinIO healthy'",
    )

    def log_success():
        print("Platform health check passed.")

    log_result = PythonOperator(
        task_id="log_result",
        python_callable=log_success,
    )

    [check_trino, check_minio] >> log_result
