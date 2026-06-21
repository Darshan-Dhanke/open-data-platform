"""
Metabase visualization block.

A lightweight BI/dashboard tool. It stores its own app data in the shared
Postgres (a dedicated `metabase` database) and ships with the Starburst Trino
driver so it can query the lakehouse through Trino. The driver is fetched into
a plugins volume by a one-shot init container (the base image has no driver).

Exposes:
  - Metabase : http://localhost:${METABASE_PORT}  (default 3002)
"""

from __future__ import annotations

METABASE_IMAGE = "metabase/metabase:v0.50.26"
# Starburst's community Metabase driver — adds Trino as a queryable database.
TRINO_DRIVER_URL = (
    "https://github.com/starburstdata/metabase-driver/releases/download/"
    "6.1.0/starburst-6.1.0.metabase-driver.jar"
)


def service_blocks(selections: dict) -> dict:
    return {
        "metabase-db-init": {
            "image": "darshandhanke07/odp-postgres:15-alpine",
            "container_name": "metabase-db-init",
            "depends_on": {"postgresql": {"condition": "service_healthy"}},
            "entrypoint": ["/bin/sh", "-c"],
            "command": [
                "export PGPASSWORD=${POSTGRES_PASSWORD:-hive}; "
                "psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
                "-tc \"SELECT 1 FROM pg_database WHERE datname='metabase'\" | grep -q 1 "
                "|| psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
                "-c 'CREATE DATABASE metabase'; echo 'metabase database ready.'"
            ],
            "networks": ["platform"],
        },
        # Download the Trino driver into the shared plugins volume.
        "metabase-plugin-init": {
            "image": "curlimages/curl:8.10.1",
            "container_name": "metabase-plugin-init",
            "entrypoint": ["/bin/sh", "-c"],
            "command": [
                "test -f /plugins/starburst.metabase-driver.jar "
                "|| curl -fsSL -o /plugins/starburst.metabase-driver.jar "
                f"{TRINO_DRIVER_URL}; echo 'metabase trino driver ready.'"
            ],
            "volumes": ["metabase_plugins:/plugins"],
            "networks": ["platform"],
        },
        "metabase": {
            "image": METABASE_IMAGE,
            "container_name": "metabase",
            "ports": ["${METABASE_PORT:-3002}:3000"],
            "environment": {
                "MB_DB_TYPE":   "postgres",
                "MB_DB_DBNAME": "metabase",
                "MB_DB_PORT":   "5432",
                "MB_DB_USER":   "${POSTGRES_USER:-hive}",
                "MB_DB_PASS":   "${POSTGRES_PASSWORD:-hive}",
                "MB_DB_HOST":   "postgresql",
                "MB_PLUGINS_DIR": "/plugins",
            },
            "volumes": ["metabase_plugins:/plugins"],
            "depends_on": {
                "metabase-db-init":     {"condition": "service_completed_successfully"},
                "metabase-plugin-init": {"condition": "service_completed_successfully"},
            },
            "healthcheck": {
                "test": ["CMD-SHELL", "curl -sf http://localhost:3000/api/health || exit 1"],
                "interval": "30s",
                "timeout": "10s",
                "retries": 6,
                "start_period": "90s",
            },
            "networks": ["platform"],
        },
    }


def env_vars(selections: dict) -> dict:
    return {"METABASE_PORT": "3002"}


def named_volumes(selections: dict) -> list[str]:
    return ["metabase_plugins"]


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
