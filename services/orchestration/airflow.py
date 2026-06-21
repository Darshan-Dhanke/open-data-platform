"""
Airflow service block.

Runs Airflow in CeleryExecutor mode:
  - airflow-webserver  : UI at http://localhost:${AIRFLOW_PORT}
  - airflow-scheduler  : DAG parsing and task scheduling
  - airflow-worker     : Celery task execution
  - airflow-init       : one-shot DB migration + admin user creation
  - redis              : Celery broker

DAGs directory is mounted from ./dags/ so you can add DAGs without
rebuilding. On first run, airflow-init creates an admin user with
credentials from AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD in .env.

Exposes:
  - Airflow UI : http://localhost:${AIRFLOW_PORT}  (default 8082)
"""

from __future__ import annotations

from pathlib import Path


AIRFLOW_IMAGE = "darshandhanke07/odp-airflow:2.9.1"


def service_blocks(selections: dict) -> dict:
    _write_airflow_env_file()
    _ensure_dags_dir()

    common_env = _common_env()
    common_volumes = _common_volumes()

    # When Marquez is the catalog/lineage backend, point Airflow's built-in
    # OpenLineage provider at it. With these set, every DAG/task run emits
    # OpenLineage events to Marquez automatically — no per-DAG code needed.
    # Emission is non-blocking, so tasks still succeed if Marquez is down.
    if selections.get("catalog") == "marquez":
        common_env["AIRFLOW__OPENLINEAGE__TRANSPORT"] = (
            '{"type": "http", "url": "http://marquez:5000"}'
        )
        common_env["AIRFLOW__OPENLINEAGE__NAMESPACE"] = "open-data-platform"

    # Airflow needs its own postgres DB — reuse the platform postgresql
    # but with a different database name to stay isolated from HMS.
    db_depends = {"postgresql": {"condition": "service_healthy"}}
    redis_depends = {"redis": {"condition": "service_healthy"}}

    blocks = {}

    # Redis — Celery broker
    blocks["redis"] = {
        "image": "darshandhanke07/odp-redis:7-alpine",
        "container_name": "redis",
        "healthcheck": {
            "test": ["CMD", "redis-cli", "ping"],
            "interval": "10s",
            "timeout": "5s",
            "retries": 5,
        },
        "networks": ["platform"],
    }

    # Init container — runs DB migration and creates admin user then exits.
    # Airflow shares the postgresql instance but needs its own database.
    # The AIRFLOW__DATABASE__SQL_ALCHEMY_CONN points to the airflow DB;
    # Airflow's db migrate will create it if the user has createdb rights.
    blocks["airflow-init"] = {
        "image": AIRFLOW_IMAGE,
        "container_name": "airflow-init",
        "entrypoint": "/bin/bash",
        "command": [
            "-c",
            "airflow db migrate && "
            "airflow users create "
            "--username ${AIRFLOW_ADMIN_USER:-admin} "
            "--password ${AIRFLOW_ADMIN_PASSWORD:-admin} "
            "--firstname Admin --lastname User "
            "--role Admin --email admin@example.com || true",
        ],
        "environment": common_env,
        "volumes": common_volumes,
        "depends_on": {**db_depends, **redis_depends},
        "networks": ["platform"],
    }

    # Webserver
    blocks["airflow-webserver"] = {
        "image": AIRFLOW_IMAGE,
        "container_name": "airflow-webserver",
        "command": "webserver",
        "ports": [
            "${AIRFLOW_PORT:-8082}:8080",
        ],
        "environment": common_env,
        "volumes": common_volumes,
        "depends_on": {
            "airflow-init": {"condition": "service_completed_successfully"},
        },
        "healthcheck": {
            "test": ["CMD", "curl", "-f", "http://localhost:8080/health"],
            "interval": "30s",
            "timeout": "10s",
            "retries": 5,
            "start_period": "60s",
        },
        "networks": ["platform"],
    }

    # Scheduler
    blocks["airflow-scheduler"] = {
        "image": AIRFLOW_IMAGE,
        "container_name": "airflow-scheduler",
        "command": "scheduler",
        "environment": common_env,
        "volumes": common_volumes,
        "depends_on": {
            "airflow-init": {"condition": "service_completed_successfully"},
        },
        "healthcheck": {
            "test": [
                "CMD-SHELL",
                "airflow jobs check --job-type SchedulerJob --hostname $(hostname)",
            ],
            "interval": "30s",
            "timeout": "10s",
            "retries": 5,
            "start_period": "30s",
        },
        "networks": ["platform"],
    }

    # Celery worker
    worker_env = {**common_env, "DUMB_INIT_SETSID": "0"}
    # The Great Expectations quality layer runs its validations as Celery tasks
    # on the worker only (the DAG imports GX lazily inside the task, so the
    # scheduler/webserver never need it). Installing GX only here keeps the
    # other Airflow containers lean and their startup fast.
    if selections.get("quality") == "great_expectations":
        worker_env["_PIP_ADDITIONAL_REQUIREMENTS"] = (
            "great-expectations==1.18.1 trino==0.337.0"
        )
    elif selections.get("quality") == "soda":
        worker_env["_PIP_ADDITIONAL_REQUIREMENTS"] = "soda-core-trino==3.3.9"

    blocks["airflow-worker"] = {
        "image": AIRFLOW_IMAGE,
        "command": "celery worker",
        "environment": worker_env,
        "volumes": common_volumes,
        "depends_on": {
            "airflow-scheduler": {"condition": "service_healthy"},
        },
        "deploy": {
            "replicas": "${AIRFLOW_WORKER_REPLICAS:-1}",
        },
        "networks": ["platform"],
    }

    return blocks


def _common_env() -> dict:
    return {
        "AIRFLOW__CORE__EXECUTOR":
            "CeleryExecutor",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN":
            "postgresql+psycopg2://${POSTGRES_USER:-hive}:${POSTGRES_PASSWORD:-hive}"
            "@postgresql:5432/${POSTGRES_DB:-metastore}",
        "AIRFLOW__CELERY__RESULT_BACKEND":
            "db+postgresql://${POSTGRES_USER:-hive}:${POSTGRES_PASSWORD:-hive}"
            "@postgresql:5432/${POSTGRES_DB:-metastore}",
        "AIRFLOW__CELERY__BROKER_URL":
            "redis://:@redis:6379/0",
        "AIRFLOW__CORE__FERNET_KEY":
            "${AIRFLOW_FERNET_KEY:-46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLt0nfdstY=}",
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "true",
        "AIRFLOW__CORE__LOAD_EXAMPLES":               "false",
        "AIRFLOW__API__AUTH_BACKENDS":
            "airflow.api.auth.backend.basic_auth,"
            "airflow.api.auth.backend.session",
        "AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK":    "true",
        "AIRFLOW_ADMIN_USER":    "${AIRFLOW_ADMIN_USER:-admin}",
        "AIRFLOW_ADMIN_PASSWORD": "${AIRFLOW_ADMIN_PASSWORD:-admin}",
        "_PIP_ADDITIONAL_REQUIREMENTS": "",
    }


def _common_volumes() -> list:
    return [
        "./dags:/opt/airflow/dags",
        "./configs/airflow/logs:/opt/airflow/logs",
        "./configs/airflow/plugins:/opt/airflow/plugins",
    ]


def _write_airflow_env_file() -> None:
    """Write AIRFLOW_UID file needed on Linux hosts. No-op on Windows."""
    Path("configs/airflow/logs").mkdir(parents=True, exist_ok=True)
    Path("configs/airflow/plugins").mkdir(parents=True, exist_ok=True)


def _ensure_dags_dir() -> None:
    """Create dags/ with a sample DAG so Airflow starts with something visible."""
    dags_dir = Path("dags")
    dags_dir.mkdir(exist_ok=True)

    sample = dags_dir / "sample_platform_dag.py"
    if not sample.exists():
        sample.write_text(_sample_dag(), encoding="utf-8", newline="\n")


def _sample_dag() -> str:
    return '''"""
sample_platform_dag.py

A minimal DAG that demonstrates the platform is wired up correctly.
Replace this with your own pipeline logic.
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

    check_trino = BashOperator(
        task_id="check_trino",
        bash_command=(
            "curl -s -f http://trino:8080/v1/info "
            "| python3 -c \\"import sys,json; d=json.load(sys.stdin); "
            "print(f\\'Trino {d[\\"nodeVersion\\"][\\"version\\"]} — '
            "f'starting={d[\\"starting\\"]}\\')\\" "
        ),
    )

    check_minio = BashOperator(
        task_id="check_minio",
        bash_command="curl -sf http://minio:9000/minio/health/live && echo MinIO healthy",
    )

    def log_success():
        print("Platform health check passed.")

    log_result = PythonOperator(
        task_id="log_result",
        python_callable=log_success,
    )

    [check_trino, check_minio] >> log_result
'''


def env_vars(selections: dict) -> dict:
    return {
        "AIRFLOW_PORT":             "8082",
        "AIRFLOW_ADMIN_USER":       "admin",
        "AIRFLOW_ADMIN_PASSWORD":   "admin",
        "AIRFLOW_FERNET_KEY":       "46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLt0nfdstY=",
        "AIRFLOW_DB":               "airflow",
        "AIRFLOW_WORKER_REPLICAS":  "1",
    }


def named_volumes(selections: dict) -> list[str]:
    return []


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
