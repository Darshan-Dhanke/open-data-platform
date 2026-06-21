#!/bin/bash
set -e

# Start Kafka Connect in background using the original entrypoint
/docker-entrypoint.sh start &
CONNECT_PID=$!

echo "Waiting for Kafka Connect REST API..."
until curl -sf http://localhost:8083/connectors > /dev/null 2>&1; do
    sleep 5
done
echo "Kafka Connect is ready."

# Register Debezium PostgreSQL connector (idempotent, best-effort).
EXISTING=$(curl -sf http://localhost:8083/connectors/postgres-cdc-connector 2>/dev/null || echo "")
if [ -z "$EXISTING" ]; then
    echo "Registering Debezium connector..."
    curl -s -o /dev/null -w "Connector registration HTTP %{http_code}\n" \
        -X POST http://localhost:8083/connectors \
        -H 'Content-Type: application/json' \
        -d @- <<JSON || echo "Connector registration failed (continuing)."
{
  "name": "postgres-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgresql",
    "database.port": "5432",
    "database.user": "${POSTGRES_USER:-hive}",
    "database.password": "${POSTGRES_PASSWORD:-hive}",
    "database.dbname": "${POSTGRES_DB:-metastore}",
    "database.server.name": "platform",
    "topic.prefix": "cdc",
    "schema.include.list": "public",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "debezium_publication",
    "table.include.list": "public.*",
    "decimal.handling.mode": "string",
    "snapshot.mode": "initial"
  }
}
JSON
    echo "Debezium connector registration attempted."
else
    echo "Debezium connector already exists, skipping."
fi

# Hand control back to Connect (keeps the container in the foreground)
wait $CONNECT_PID
