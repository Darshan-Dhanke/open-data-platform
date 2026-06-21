"""
Prefect orchestration block — an alternative to Airflow.

Runs a single Prefect server (API + UI) backed by the platform's PostgreSQL.
Prefect is far lighter than Airflow (one container vs scheduler/webserver/worker
+ Redis), making it the low-footprint orchestration option.

Exposes:
  - Prefect UI/API : http://localhost:${PREFECT_PORT}  (default 4200)
"""

from __future__ import annotations

PREFECT_IMAGE = "prefecthq/prefect:2-latest"


def service_blocks(selections: dict) -> dict:
    depends = {}
    # Use the shared Postgres for Prefect's metadata DB (its own database).
    if "postgresql" not in depends:
        depends["postgresql"] = {"condition": "service_healthy"}

    return {
        # Create Prefect's database in the shared Postgres (idempotent), then exit.
        "prefect-db-init": {
            "image": "darshandhanke07/odp-postgres:15-alpine",
            "container_name": "prefect-db-init",
            "depends_on": {"postgresql": {"condition": "service_healthy"}},
            "entrypoint": ["/bin/sh", "-c"],
            "command": [
                "export PGPASSWORD=${POSTGRES_PASSWORD:-hive}; "
                "psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
                "-tc \"SELECT 1 FROM pg_database WHERE datname='prefect'\" | grep -q 1 "
                "|| psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
                "-c 'CREATE DATABASE prefect'; echo 'prefect database ready.'"
            ],
            "networks": ["platform"],
        },
        "prefect": {
            "image": PREFECT_IMAGE,
            "container_name": "prefect",
            "command": ["prefect", "server", "start", "--host", "0.0.0.0"],
            "ports": ["${PREFECT_PORT:-4200}:4200"],
            "environment": {
                "PREFECT_SERVER_API_HOST": "0.0.0.0",
                "PREFECT_API_DATABASE_CONNECTION_URL":
                    "postgresql+asyncpg://${POSTGRES_USER:-hive}:${POSTGRES_PASSWORD:-hive}"
                    "@postgresql:5432/prefect",
                # The browser talks to the API on the host port.
                "PREFECT_UI_API_URL": "http://localhost:${PREFECT_PORT:-4200}/api",
            },
            "depends_on": {
                "prefect-db-init": {"condition": "service_completed_successfully"},
            },
            "healthcheck": {
                "test": ["CMD-SHELL", "python -c \"import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:4200/api/health').read() else 1)\" || exit 1"],
                "interval": "30s",
                "timeout": "10s",
                "retries": 5,
                "start_period": "30s",
            },
            "networks": ["platform"],
        },
    }


def env_vars(selections: dict) -> dict:
    return {"PREFECT_PORT": "4200"}


def named_volumes(selections: dict) -> list[str]:
    return []


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
