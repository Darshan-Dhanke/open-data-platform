"""
Shared PostgreSQL base service.

Postgres is infrastructure that many layers depend on — the Hive Metastore,
Airflow, Marquez, Nessie, Prefect, etc. all store metadata in it. It therefore
lives here as a *base* service that the composer always includes, rather than
inside any one layer's module. (Previously it was defined in the Hive Metastore
module, which meant selecting metastore=nessie left the platform with no
Postgres and broke everything that needed it.)

Individual modules create their own database inside this instance via a small
*-db-init step (see marquez.py, prefect.py, nessie.py).
"""

from __future__ import annotations


def service_blocks(selections: dict) -> dict:
    postgres = {
        "image": "darshandhanke07/odp-postgres:15-alpine",
        "container_name": "postgresql",
        "environment": {
            "POSTGRES_USER":     "${POSTGRES_USER:-hive}",
            "POSTGRES_PASSWORD": "${POSTGRES_PASSWORD:-hive}",
            "POSTGRES_DB":       "${POSTGRES_DB:-metastore}",
        },
        "ports": ["${POSTGRES_PORT:-5432}:5432"],
        "volumes": ["postgresql_data:/var/lib/postgresql/data"],
        "healthcheck": {
            # -d names the database so pg_isready doesn't default to a DB named
            # after the user ("hive"), which doesn't exist.
            "test": ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore}"],
            "interval": "10s",
            "timeout": "5s",
            "retries": 5,
        },
        "networks": ["platform"],
    }

    # Debezium CDC reads the Postgres WAL via logical decoding, which the stock
    # image does not enable (it ships wal_level=replica). Enable it only when a
    # CDC ingestion stack is selected so other stacks keep the image defaults.
    if selections.get("ingestion") == "kafka_debezium":
        postgres["command"] = [
            "postgres",
            "-c", "wal_level=logical",
            "-c", "max_wal_senders=10",
            "-c", "max_replication_slots=10",
        ]

    return {"postgresql": postgres}


def env_vars(selections: dict) -> dict:
    return {
        "POSTGRES_USER":     "hive",
        "POSTGRES_PASSWORD": "hive",
        "POSTGRES_DB":       "metastore",
        "POSTGRES_PORT":     "5432",
    }


def named_volumes(selections: dict) -> list[str]:
    return ["postgresql_data"]


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
