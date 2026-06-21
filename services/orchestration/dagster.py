"""
Dagster orchestration block — an alternative to Airflow.

Dagster runs against user-defined code, so this module ships a minimal sample
project (configs/dagster/defs.py) and runs `dagster dev` (webserver + daemon in
one process) from a slim Python image, installing Dagster at startup. It uses
the shared Postgres for run/event storage.

Exposes:
  - Dagster UI : http://localhost:${DAGSTER_PORT}  (default 3003)
"""

from __future__ import annotations

from pathlib import Path

PYTHON_IMAGE = "darshandhanke07/odp-python:3.11-slim"
DAGSTER_PIP = "dagster==1.8.13 dagster-webserver==1.8.13 dagster-postgres==0.24.13"


def service_blocks(selections: dict) -> dict:
    _write_dagster_project()

    return {
        "dagster-db-init": {
            "image": "darshandhanke07/odp-postgres:15-alpine",
            "container_name": "dagster-db-init",
            "depends_on": {"postgresql": {"condition": "service_healthy"}},
            "entrypoint": ["/bin/sh", "-c"],
            "command": [
                "export PGPASSWORD=${POSTGRES_PASSWORD:-hive}; "
                "psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
                "-tc \"SELECT 1 FROM pg_database WHERE datname='dagster'\" | grep -q 1 "
                "|| psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
                "-c 'CREATE DATABASE dagster'; echo 'dagster database ready.'"
            ],
            "networks": ["platform"],
        },
        "dagster": {
            "image": PYTHON_IMAGE,
            "container_name": "dagster",
            "working_dir": "/opt/dagster/app",
            "ports": ["${DAGSTER_PORT:-3003}:3000"],
            "environment": {
                "DAGSTER_HOME": "/opt/dagster/home",
                "DAGSTER_PG_USERNAME": "${POSTGRES_USER:-hive}",
                "DAGSTER_PG_PASSWORD": "${POSTGRES_PASSWORD:-hive}",
                "DAGSTER_PG_DB": "dagster",
            },
            "entrypoint": ["/bin/bash", "-c"],
            # $$ escapes Compose interpolation so the container shell expands
            # DAGSTER_HOME from the service environment (not the host/.env).
            "command": [
                f"pip install --no-cache-dir {DAGSTER_PIP} "
                "&& mkdir -p \"$$DAGSTER_HOME\" "
                "&& cp /opt/dagster/app/dagster.yaml \"$$DAGSTER_HOME/dagster.yaml\" "
                "&& dagster dev -h 0.0.0.0 -p 3000 -f /opt/dagster/app/defs.py"
            ],
            "volumes": ["./configs/dagster:/opt/dagster/app:ro"],
            "depends_on": {
                "dagster-db-init": {"condition": "service_completed_successfully"},
            },
            "healthcheck": {
                "test": ["CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://localhost:3000')\" || exit 1"],
                "interval": "30s",
                "timeout": "10s",
                "retries": 10,
                "start_period": "120s",
            },
            "networks": ["platform"],
        },
    }


def _write_dagster_project() -> None:
    d = Path("configs/dagster")
    d.mkdir(parents=True, exist_ok=True)
    (d / "defs.py").write_text(
        "from dagster import asset, Definitions\n"
        "\n"
        "\n"
        "@asset\n"
        "def hello_platform() -> str:\n"
        '    """A trivial asset proving the Dagster code location loads."""\n'
        '    return "hello from the open data platform"\n'
        "\n"
        "\n"
        "defs = Definitions(assets=[hello_platform])\n",
        encoding="utf-8", newline="\n",
    )
    # Store runs/events in the shared Postgres so they survive restarts.
    (d / "dagster.yaml").write_text(
        "storage:\n"
        "  postgres:\n"
        "    postgres_db:\n"
        "      username:\n"
        "        env: DAGSTER_PG_USERNAME\n"
        "      password:\n"
        "        env: DAGSTER_PG_PASSWORD\n"
        "      hostname: postgresql\n"
        "      db_name:\n"
        "        env: DAGSTER_PG_DB\n"
        "      port: 5432\n",
        encoding="utf-8", newline="\n",
    )


def env_vars(selections: dict) -> dict:
    return {"DAGSTER_PORT": "3003"}


def named_volumes(selections: dict) -> list[str]:
    return []


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
