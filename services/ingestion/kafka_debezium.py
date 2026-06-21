"""
Kafka + Debezium ingestion service block.

KRaft mode — no Zookeeper.

Self-contained kafka-connect: the entrypoint starts Connect, waits
for the REST API, then registers the Debezium connector automatically.
No debezium-init container needed.

Exposes:
  - Kafka broker       : localhost:${KAFKA_EXTERNAL_PORT}  (default 9092)
  - Kafka Connect REST : http://localhost:${KAFKA_CONNECT_PORT} (default 8083)
  - Kafka UI           : http://localhost:${KAFKA_UI_PORT} (default 9094)

Upstream:
  confluentinc/cp-kafka:7.6.0  https://hub.docker.com/r/confluentinc/cp-kafka
  debezium/connect:2.6         https://hub.docker.com/r/debezium/connect
  provectuslabs/kafka-ui       https://hub.docker.com/r/provectuslabs/kafka-ui
"""

from __future__ import annotations
from pathlib import Path

DOCKERHUB_USER = "darshandhanke07"
PREFIX = "odp"


def service_blocks(selections: dict) -> dict:
    _write_connect_entrypoint()
    blocks = {}

    blocks["kafka"] = {
        "image": f"{DOCKERHUB_USER}/{PREFIX}-cp-kafka:7.6.0",
        "container_name": "kafka",
        "ports": ["${KAFKA_EXTERNAL_PORT:-9092}:9092"],
        "environment": {
            "KAFKA_NODE_ID": "1",
            "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@kafka:9093",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_LISTENERS":
                "PLAINTEXT://kafka:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://kafka:9093",
            "KAFKA_ADVERTISED_LISTENERS":
                "PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:${KAFKA_EXTERNAL_PORT:-9092}",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":
                "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
            "CLUSTER_ID": "MkU3OEVBNTcwNTJENDM2Qk",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
            "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
            "KAFKA_LOG_RETENTION_HOURS": "${KAFKA_LOG_RETENTION_HOURS:-168}",
            "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
        },
        "volumes": ["kafka_data:/var/lib/kafka/data"],
        "healthcheck": {
            # bash /dev/tcp is a shell built-in available in every bash
            # container. The original "nc -z" check failed because netcat
            # is not installed in confluentinc/cp-kafka:7.6.0, causing the
            # healthcheck to always report unhealthy even after Kafka started
            # successfully. That in turn blocked kafka-connect, kafka-ui,
            # and (via cascade) hive-metastore from starting.
            #
            # We MUST probe 9092, not 29092: the PLAINTEXT listener
            # (PLAINTEXT://kafka:29092) binds to the IP that the hostname
            # "kafka" resolves to (eth0), NOT to 127.0.0.1, so a /dev/tcp
            # probe against localhost:29092 always fails and the container
            # never reports healthy. Only PLAINTEXT_HOST://0.0.0.0:9092 is
            # bound on all interfaces, so localhost:9092 is reachable.
            "test": ["CMD-SHELL", "bash -c 'echo > /dev/tcp/localhost/9092' 2>/dev/null || exit 1"],
            "interval": "10s",
            "timeout": "5s",
            "retries": 15,
            "start_period": "30s",
        },
        "networks": ["platform"],
    }

    blocks["kafka-connect"] = {
        "image": f"{DOCKERHUB_USER}/{PREFIX}-debezium-connect:2.6",
        "container_name": "kafka-connect",
        "depends_on": {
            "kafka":      {"condition": "service_healthy"},
            "postgresql": {"condition": "service_healthy"},
        },
        "ports": ["${KAFKA_CONNECT_PORT:-8083}:8083"],
        "environment": {
            "BOOTSTRAP_SERVERS": "kafka:29092",
            "GROUP_ID": "kafka-connect-cluster",
            "CONFIG_STORAGE_TOPIC": "_connect-configs",
            "OFFSET_STORAGE_TOPIC": "_connect-offsets",
            "STATUS_STORAGE_TOPIC": "_connect-status",
            "CONFIG_STORAGE_REPLICATION_FACTOR": "1",
            "OFFSET_STORAGE_REPLICATION_FACTOR": "1",
            "STATUS_STORAGE_REPLICATION_FACTOR": "1",
            "KEY_CONVERTER": "org.apache.kafka.connect.json.JsonConverter",
            "VALUE_CONVERTER": "org.apache.kafka.connect.json.JsonConverter",
            "KEY_CONVERTER_SCHEMAS_ENABLE": "false",
            "VALUE_CONVERTER_SCHEMAS_ENABLE": "false",
            # Passed to entrypoint for connector registration
            "POSTGRES_USER":     "${POSTGRES_USER:-hive}",
            "POSTGRES_PASSWORD": "${POSTGRES_PASSWORD:-hive}",
            "POSTGRES_DB":       "${POSTGRES_DB:-metastore}",
        },
        "entrypoint": ["/bin/bash", "/entrypoint-custom.sh"],
        "volumes": [
            "./configs/kafka-connect/entrypoint-custom.sh:/entrypoint-custom.sh:ro",
        ],
        "healthcheck": {
            "test": ["CMD", "curl", "-f", "http://localhost:8083/connectors"],
            "interval": "30s",
            "timeout": "10s",
            "retries": 5,
            "start_period": "60s",
        },
        "networks": ["platform"],
    }

    blocks["kafka-ui"] = {
        "image": f"{DOCKERHUB_USER}/{PREFIX}-kafka-ui:latest",
        "container_name": "kafka-ui",
        "depends_on": {"kafka": {"condition": "service_healthy"}},
        "ports": ["${KAFKA_UI_PORT:-9094}:8080"],
        "environment": {
            "KAFKA_CLUSTERS_0_NAME":             "platform",
            "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS": "kafka:29092",
            "KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME":    "debezium",
            "KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS": "http://kafka-connect:8083",
        },
        "networks": ["platform"],
    }

    return blocks


def _write_connect_entrypoint() -> None:
    d = Path("configs/kafka-connect")
    d.mkdir(parents=True, exist_ok=True)
    # The connector JSON is sent via a heredoc with an UNQUOTED delimiter so
    # the shell expands ${POSTGRES_USER} etc. from the container environment
    # (a single-quoted curl -d payload would forward the literal placeholder
    # text to Debezium and break the connection).
    #
    # Registration is best-effort: a failure must NOT terminate the
    # container. Connect runs in the foreground via `wait`, so the REST API
    # stays available even if the connector cannot be created.
    lines = [
        "#!/bin/bash",
        "set -e",
        "",
        "# Start Kafka Connect in background using the original entrypoint",
        "/docker-entrypoint.sh start &",
        "CONNECT_PID=$!",
        "",
        'echo "Waiting for Kafka Connect REST API..."',
        "until curl -sf http://localhost:8083/connectors > /dev/null 2>&1; do",
        "    sleep 5",
        "done",
        'echo "Kafka Connect is ready."',
        "",
        "# Register Debezium PostgreSQL connector (idempotent, best-effort).",
        'EXISTING=$(curl -sf http://localhost:8083/connectors/postgres-cdc-connector 2>/dev/null || echo "")',
        'if [ -z "$EXISTING" ]; then',
        '    echo "Registering Debezium connector..."',
        '    curl -s -o /dev/null -w "Connector registration HTTP %{http_code}\\n" \\',
        "        -X POST http://localhost:8083/connectors \\",
        "        -H 'Content-Type: application/json' \\",
        '        -d @- <<JSON || echo "Connector registration failed (continuing)."',
        "{",
        '  "name": "postgres-cdc-connector",',
        '  "config": {',
        '    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",',
        '    "database.hostname": "postgresql",',
        '    "database.port": "5432",',
        '    "database.user": "${POSTGRES_USER:-hive}",',
        '    "database.password": "${POSTGRES_PASSWORD:-hive}",',
        '    "database.dbname": "${POSTGRES_DB:-metastore}",',
        '    "database.server.name": "platform",',
        '    "topic.prefix": "cdc",',
        '    "schema.include.list": "public",',
        '    "plugin.name": "pgoutput",',
        '    "slot.name": "debezium_slot",',
        '    "publication.name": "debezium_publication",',
        '    "table.include.list": "public.*",',
        '    "decimal.handling.mode": "string",',
        '    "snapshot.mode": "initial"',
        "  }",
        "}",
        "JSON",
        '    echo "Debezium connector registration attempted."',
        "else",
        '    echo "Debezium connector already exists, skipping."',
        "fi",
        "",
        "# Hand control back to Connect (keeps the container in the foreground)",
        "wait $CONNECT_PID",
        "",
    ]
    # Write explicit LF bytes so Git/Windows CRLF conversion can't corrupt
    # the script (a CRLF shebang line breaks execution under bash).
    (d / "entrypoint-custom.sh").write_bytes("\n".join(lines).encode("utf-8"))


def env_vars(selections: dict) -> dict:
    return {
        "KAFKA_EXTERNAL_PORT":       "9092",
        "KAFKA_CONNECT_PORT":        "8083",
        "KAFKA_UI_PORT":             "9094",
        "KAFKA_LOG_RETENTION_HOURS": "168",
    }


def named_volumes(selections: dict) -> list[str]:
    return ["kafka_data"]


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
