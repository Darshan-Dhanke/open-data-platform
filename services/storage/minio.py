"""
MinIO service block.

Uses a separate minio-init container to create default buckets after
MinIO starts. The MinIO server image is distroless (scratch-based) and
contains only the minio binary — no shell, wget, curl, or Python —
so bucket creation must happen in a separate container.

Exposes:
  - S3 API     : http://localhost:${MINIO_API_PORT}     (default 9000)
  - Console UI : http://localhost:${MINIO_CONSOLE_PORT} (default 9001)

Upstream: https://hub.docker.com/r/minio/minio
"""

from __future__ import annotations


def service_blocks(selections: dict) -> dict:
    return {
        "minio": {
            "image": "darshandhanke07/odp-minio:RELEASE.2024-01-16T16-07-38Z",
            "container_name": "minio",
            "command": "server /data --console-address \":${MINIO_CONSOLE_PORT:-9001}\"",
            "ports": [
                "${MINIO_API_PORT:-9000}:9000",
                "${MINIO_CONSOLE_PORT:-9001}:9001",
            ],
            "environment": {
                "MINIO_ROOT_USER":     "${MINIO_ROOT_USER:-admin}",
                "MINIO_ROOT_PASSWORD": "${MINIO_ROOT_PASSWORD:-password123}",
            },
            "volumes": ["minio_data:/data"],
            # MinIO server image is distroless — no shell, wget, or curl available.
            # minio-init uses mc ready local to wait for MinIO before creating buckets.
            "healthcheck": {
                "test": ["CMD-SHELL", "mc ready local || exit 1"],
                "interval": "15s",
                "timeout": "10s",
                "retries": 5,
                "start_period": "15s",
            },
            "networks": ["platform"],
        },

        # minio-init: creates default buckets using the mc client image.
        # Runs once and exits. Kept separate because the MinIO server image
        # is distroless and cannot run shell scripts or any additional tools.
        "minio-init": {
            "image": "darshandhanke07/odp-minio-mc:latest",
            "container_name": "minio-init",
            "depends_on": {
                "minio": {"condition": "service_healthy"},
            },
            "entrypoint": ["/bin/sh", "-c"],
            "command": [
                "mc alias set local http://minio:9000 "
                "${MINIO_ROOT_USER:-admin} ${MINIO_ROOT_PASSWORD:-password123} && "
                "mc mb --ignore-existing local/${MINIO_WAREHOUSE_BUCKET:-warehouse} && "
                "mc mb --ignore-existing local/${MINIO_RAW_BUCKET:-raw} && "
                "mc mb --ignore-existing local/${MINIO_STAGING_BUCKET:-staging} && "
                "echo 'Buckets ready.'"
            ],
            "networks": ["platform"],
        },
    }


def env_vars(selections: dict) -> dict:
    return {
        "MINIO_ROOT_USER":        "admin",
        "MINIO_ROOT_PASSWORD":    "password123",
        "MINIO_API_PORT":         "9000",
        "MINIO_CONSOLE_PORT":     "9001",
        "MINIO_WAREHOUSE_BUCKET": "warehouse",
        "MINIO_RAW_BUCKET":       "raw",
        "MINIO_STAGING_BUCKET":   "staging",
        "MINIO_ENDPOINT":         "http://minio:9000",
        "AWS_ACCESS_KEY_ID":      "admin",
        "AWS_SECRET_ACCESS_KEY":  "password123",
        "AWS_REGION":             "us-east-1",
    }


def named_volumes(selections: dict) -> list[str]:
    return ["minio_data"]


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
