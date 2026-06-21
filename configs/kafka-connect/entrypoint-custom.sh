#!/bin/bash
set -e

# Start Kafka Connect in the background using the original entrypoint.
/docker-entrypoint.sh start &
CONNECT_PID=$!

echo "Waiting for Kafka Connect REST API..."
until curl -sf http://localhost:8083/connectors > /dev/null 2>&1; do
    sleep 5
done
echo "Kafka Connect is ready. Register CDC connectors via the REST API"
echo "(http://localhost:8083/connectors) against your own database."

# Hand control back to Connect (keeps the container in the foreground)
wait $CONNECT_PID
