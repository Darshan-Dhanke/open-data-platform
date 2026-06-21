"""
Apache NiFi ingestion block — an alternative to Kafka/Debezium.

NiFi is a visual dataflow tool for moving and transforming data between systems.
This runs a single standalone NiFi node in HTTP mode (no TLS/single-user login,
which keeps the local demo simple).

Exposes:
  - NiFi UI : http://localhost:${NIFI_PORT}/nifi  (default 8086)
"""

from __future__ import annotations

NIFI_IMAGE = "apache/nifi:1.27.0"


def service_blocks(selections: dict) -> dict:
    return {
        "nifi": {
            "image": NIFI_IMAGE,
            "container_name": "nifi",
            "ports": ["${NIFI_PORT:-8086}:8080"],
            "environment": {
                # Setting an HTTP port makes NiFi 1.x serve unsecured HTTP
                # instead of the default HTTPS + generated single-user login,
                # which is fine for a local dataflow sandbox.
                "NIFI_WEB_HTTP_PORT": "8080",
                "NIFI_WEB_HTTP_HOST": "0.0.0.0",
            },
            "volumes": [
                "nifi_database:/opt/nifi/nifi-current/database_repository",
                "nifi_flowfile:/opt/nifi/nifi-current/flowfile_repository",
                "nifi_content:/opt/nifi/nifi-current/content_repository",
                "nifi_provenance:/opt/nifi/nifi-current/provenance_repository",
                "nifi_state:/opt/nifi/nifi-current/state",
            ],
            "healthcheck": {
                # NiFi is slow to boot; probe the web port with bash /dev/tcp.
                "test": ["CMD-SHELL", "bash -c 'echo > /dev/tcp/localhost/8080' 2>/dev/null || exit 1"],
                "interval": "30s",
                "timeout": "10s",
                "retries": 10,
                "start_period": "120s",
            },
            "networks": ["platform"],
        }
    }


def env_vars(selections: dict) -> dict:
    return {"NIFI_PORT": "8086"}


def named_volumes(selections: dict) -> list[str]:
    return ["nifi_database", "nifi_flowfile", "nifi_content", "nifi_provenance", "nifi_state"]


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
