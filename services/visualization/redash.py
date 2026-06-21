"""
Redash visualization block — an alternative to Metabase/Superset.

Redash is a multi-process app: a web server, a Celery scheduler and a worker,
backed by Redis (its own, to stay decoupled) and Postgres (a dedicated `redash`
database in the shared instance). A one-shot create-tables step bootstraps the
schema.

Exposes:
  - Redash : http://localhost:${REDASH_PORT}  (default 5010)
"""

from __future__ import annotations

REDASH_IMAGE = "darshandhanke07/odp-redash:10.1.0.b50633"
# Reuse the platform's existing odp-redis image (already on Docker Hub).
REDIS_IMAGE = "darshandhanke07/odp-redis:7-alpine"

# Shared environment for all Redash processes.
_REDASH_ENV = {
    "PYTHONUNBUFFERED": "0",
    "REDASH_LOG_LEVEL": "INFO",
    "REDASH_REDIS_URL": "redis://redash-redis:6379/0",
    "REDASH_DATABASE_URL":
        "postgresql://${POSTGRES_USER:-hive}:${POSTGRES_PASSWORD:-hive}@postgresql/redash",
    "REDASH_COOKIE_SECRET": "${REDASH_COOKIE_SECRET:-odp-redash-cookie-secret}",
    "REDASH_SECRET_KEY":    "${REDASH_SECRET_KEY:-odp-redash-secret-key}",
}


def service_blocks(selections: dict) -> dict:
    return {
        "redash-redis": {
            "image": REDIS_IMAGE,
            "container_name": "redash-redis",
            "healthcheck": {
                "test": ["CMD", "redis-cli", "ping"],
                "interval": "10s", "timeout": "5s", "retries": 5,
            },
            "networks": ["platform"],
        },
        "redash-db-init": {
            "image": "darshandhanke07/odp-postgres:15-alpine",
            "container_name": "redash-db-init",
            "depends_on": {"postgresql": {"condition": "service_healthy"}},
            "entrypoint": ["/bin/sh", "-c"],
            "command": [
                "export PGPASSWORD=${POSTGRES_PASSWORD:-hive}; "
                "psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
                "-tc \"SELECT 1 FROM pg_database WHERE datname='redash'\" | grep -q 1 "
                "|| psql -h postgresql -U ${POSTGRES_USER:-hive} -d ${POSTGRES_DB:-metastore} "
                "-c 'CREATE DATABASE redash'; echo 'redash database ready.'"
            ],
            "networks": ["platform"],
        },
        # One-shot: create Redash's tables, then exit.
        "redash-create-tables": {
            "image": REDASH_IMAGE,
            "container_name": "redash-create-tables",
            "command": ["create_db"],
            "environment": dict(_REDASH_ENV),
            "depends_on": {
                "redash-db-init": {"condition": "service_completed_successfully"},
                "redash-redis":   {"condition": "service_healthy"},
            },
            "networks": ["platform"],
        },
        "redash-server": {
            "image": REDASH_IMAGE,
            "container_name": "redash-server",
            "command": ["server"],
            "ports": ["${REDASH_PORT:-5010}:5000"],
            "environment": {**_REDASH_ENV, "REDASH_WEB_WORKERS": "2"},
            "depends_on": {
                "redash-create-tables": {"condition": "service_completed_successfully"},
            },
            "healthcheck": {
                "test": ["CMD-SHELL", "wget -q -O /dev/null http://localhost:5000/ping || exit 1"],
                "interval": "30s", "timeout": "10s", "retries": 6, "start_period": "60s",
            },
            "networks": ["platform"],
        },
        "redash-scheduler": {
            "image": REDASH_IMAGE,
            "container_name": "redash-scheduler",
            "command": ["scheduler"],
            "environment": dict(_REDASH_ENV),
            "depends_on": {"redash-server": {"condition": "service_healthy"}},
            "networks": ["platform"],
        },
        "redash-worker": {
            "image": REDASH_IMAGE,
            "container_name": "redash-worker",
            "command": ["worker"],
            "environment": {**_REDASH_ENV, "WORKERS_COUNT": "2"},
            "depends_on": {"redash-server": {"condition": "service_healthy"}},
            "networks": ["platform"],
        },
    }


def env_vars(selections: dict) -> dict:
    return {
        "REDASH_PORT": "5010",
        "REDASH_COOKIE_SECRET": "odp-redash-cookie-secret",
        "REDASH_SECRET_KEY": "odp-redash-secret-key",
    }


def named_volumes(selections: dict) -> list[str]:
    return []


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
