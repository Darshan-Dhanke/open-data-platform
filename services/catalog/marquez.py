"""
Marquez catalog/lineage block.

Marquez is the reference OpenLineage backend — a lightweight alternative to
DataHub/OpenMetadata for data lineage (it does not need Elasticsearch, so its
footprint is ~1 GiB vs ~6-8 GiB). It reuses the platform's existing PostgreSQL
rather than running its own database.

Three services:
  - marquez-db-init : one-shot; creates the `marquez` database in the shared
                      Postgres (idempotent), then exits.
  - marquez         : the OpenLineage API + metadata server (runs its own
                      Flyway migrations on startup).
  - marquez-web     : the lineage UI.

Lineage is fed in by the Airflow OpenLineage provider, which is pointed at this
server when catalog=marquez (see services/orchestration/airflow.py). Triggering
any DAG then produces lineage visible in the Marquez UI.

Exposes:
  - Marquez API : http://localhost:${MARQUEZ_PORT}      (default 5000)
  - Marquez UI  : http://localhost:${MARQUEZ_WEB_PORT}  (default 3001)
"""

from __future__ import annotations

from pathlib import Path

# Upstream official images (pulled directly, like the observability stack).
MARQUEZ_IMAGE = "darshandhanke07/odp-marquez:0.50.0"
MARQUEZ_WEB_IMAGE = "darshandhanke07/odp-marquez-web:0.50.0"
# Reuse the platform's Postgres image for the throwaway DB-init step so nothing
# extra is pulled.
POSTGRES_IMAGE = "darshandhanke07/odp-postgres:15-alpine"


def service_blocks(selections: dict) -> dict:
    _write_marquez_config()

    blocks: dict = {}

    # Marquez needs its own database to exist before it runs migrations. The
    # stock Postgres image only creates POSTGRES_DB (metastore), so create the
    # marquez DB here. Idempotent: skip if it already exists.
    blocks["marquez-db-init"] = {
        "image": POSTGRES_IMAGE,
        "container_name": "marquez-db-init",
        "depends_on": {"postgresql": {"condition": "service_healthy"}},
        "entrypoint": ["/bin/sh", "-c"],
        "command": [
            "export PGPASSWORD=${POSTGRES_PASSWORD:-hive}; "
            "psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
            "-tc \"SELECT 1 FROM pg_database WHERE datname='marquez'\" | grep -q 1 "
            "|| psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
            "-c 'CREATE DATABASE marquez'; echo 'marquez database ready.'"
        ],
        "networks": ["platform"],
    }

    blocks["marquez"] = {
        "image": MARQUEZ_IMAGE,
        "container_name": "marquez",
        "depends_on": {
            "marquez-db-init": {"condition": "service_completed_successfully"},
            "postgresql": {"condition": "service_healthy"},
        },
        "ports": [
            "${MARQUEZ_PORT:-5000}:5000",
            "${MARQUEZ_ADMIN_PORT:-5001}:5001",
        ],
        "environment": {
            # Without MARQUEZ_CONFIG the image falls back to a baked dev config
            # (db user "marquez") and ignores these vars. Point it at our config
            # file, which references these vars via env substitution.
            "MARQUEZ_CONFIG":     "/opt/marquez/marquez.yml",
            "MARQUEZ_PORT":       "5000",
            "MARQUEZ_ADMIN_PORT": "5001",
            "POSTGRES_HOST":      "postgresql",
            "POSTGRES_PORT":      "5432",
            "POSTGRES_DB":        "marquez",
            "POSTGRES_USER":      "${POSTGRES_USER:-hive}",
            "POSTGRES_PASSWORD":  "${POSTGRES_PASSWORD:-hive}",
        },
        "volumes": [
            "./configs/marquez/marquez.yml:/opt/marquez/marquez.yml:ro",
        ],
        "healthcheck": {
            "test": ["CMD-SHELL", "wget -q -O /dev/null http://localhost:5001/healthcheck || exit 1"],
            "interval": "30s",
            "timeout": "10s",
            "retries": 6,
            "start_period": "60s",
        },
        "networks": ["platform"],
    }

    blocks["marquez-web"] = {
        "image": MARQUEZ_WEB_IMAGE,
        "container_name": "marquez-web",
        "depends_on": {"marquez": {"condition": "service_healthy"}},
        "ports": ["${MARQUEZ_WEB_PORT:-3001}:3000"],
        "environment": {
            "MARQUEZ_HOST": "marquez",
            "MARQUEZ_PORT": "5000",
            # The web container reads WEB_PORT for the port its node server
            # listens on; without it the app logs "listening on port undefined"
            # and nothing is reachable.
            "WEB_PORT": "3000",
        },
        "networks": ["platform"],
    }

    return blocks


def _write_marquez_config() -> None:
    d = Path("configs/marquez")
    d.mkdir(parents=True, exist_ok=True)
    # Marquez (Dropwizard) performs ${ENV:-default} substitution on this file,
    # so DB connection details come from the container's env vars. migrateOn-
    # Startup runs the Flyway schema migrations inside the (already-created)
    # marquez database.
    (d / "marquez.yml").write_text(
        "server:\n"
        "  applicationConnectors:\n"
        "    - type: http\n"
        "      port: ${MARQUEZ_PORT:-5000}\n"
        "  adminConnectors:\n"
        "    - type: http\n"
        "      port: ${MARQUEZ_ADMIN_PORT:-5001}\n"
        "\n"
        "db:\n"
        "  driverClass: org.postgresql.Driver\n"
        "  url: jdbc:postgresql://${POSTGRES_HOST:-postgresql}:${POSTGRES_PORT:-5432}/${POSTGRES_DB:-marquez}\n"
        "  user: ${POSTGRES_USER:-hive}\n"
        "  password: ${POSTGRES_PASSWORD:-hive}\n"
        "\n"
        "migrateOnStartup: true\n",
        encoding="utf-8", newline="\n",
    )


def env_vars(selections: dict) -> dict:
    return {
        "MARQUEZ_PORT":       "5000",
        "MARQUEZ_ADMIN_PORT": "5001",
        "MARQUEZ_WEB_PORT":   "3001",
    }


def named_volumes(selections: dict) -> list[str]:
    return []


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
