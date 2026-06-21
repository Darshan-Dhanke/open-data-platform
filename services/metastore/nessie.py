"""
Project Nessie metastore block — an alternative to the Hive Metastore.

Nessie is a transactional catalog for Iceberg with Git-like branching. It is a
single lightweight service (no separate Thrift server / schema bootstrap) and
persists its catalog in the platform's PostgreSQL via JDBC.

Note: when metastore=nessie, the processing (Spark) and query (Trino) layers
point their Iceberg catalogs at Nessie's REST endpoint instead of Thrift HMS —
that wiring already exists in services/processing/spark.py and
services/query_engine/trino.py (the nessie branches).

Exposes:
  - Nessie API : http://localhost:${NESSIE_PORT}  (default 19120)
"""

from __future__ import annotations

NESSIE_IMAGE = "darshandhanke07/odp-nessie:0.99.0"


def service_blocks(selections: dict) -> dict:
    return {
        # Create Nessie's database in the shared Postgres (idempotent), then exit.
        "nessie-db-init": {
            "image": "darshandhanke07/odp-postgres:15-alpine",
            "container_name": "nessie-db-init",
            "depends_on": {"postgresql": {"condition": "service_healthy"}},
            "entrypoint": ["/bin/sh", "-c"],
            "command": [
                "export PGPASSWORD=${POSTGRES_PASSWORD:-hive}; "
                "psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
                "-tc \"SELECT 1 FROM pg_database WHERE datname='nessie'\" | grep -q 1 "
                "|| psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
                "-c 'CREATE DATABASE nessie'; echo 'nessie database ready.'"
            ],
            "networks": ["platform"],
        },
        "nessie": {
            "image": NESSIE_IMAGE,
            "container_name": "nessie",
            "ports": ["${NESSIE_PORT:-19120}:19120"],
            "environment": {
                # Persist the catalog in Postgres via Nessie's JDBC version store.
                "nessie.version.store.type": "JDBC",
                "quarkus.datasource.jdbc.url":
                    "jdbc:postgresql://postgresql:5432/nessie",
                "quarkus.datasource.username": "${POSTGRES_USER:-hive}",
                "quarkus.datasource.password": "${POSTGRES_PASSWORD:-hive}",
            },
            "depends_on": {
                "nessie-db-init": {"condition": "service_completed_successfully"},
            },
            "healthcheck": {
                "test": ["CMD-SHELL", "bash -c 'echo > /dev/tcp/localhost/19120' 2>/dev/null || exit 1"],
                "interval": "15s",
                "timeout": "5s",
                "retries": 6,
                "start_period": "20s",
            },
            "networks": ["platform"],
        },
    }


def env_vars(selections: dict) -> dict:
    return {"NESSIE_PORT": "19120"}


def named_volumes(selections: dict) -> list[str]:
    return []


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
