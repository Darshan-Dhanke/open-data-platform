#!/bin/sh
set -e

# Start MinIO server in background
minio server /data --console-address ":${MINIO_CONSOLE_PORT:-9001}" &
MINIO_PID=$!

# Wait until MinIO is ready
echo "Waiting for MinIO to be ready..."
until mc ready local 2>/dev/null; do
    sleep 2
done

# Configure mc client
mc alias set local http://localhost:9000     "${MINIO_ROOT_USER:-admin}"     "${MINIO_ROOT_PASSWORD:-password123}"

# Create default buckets (idempotent)
mc mb --ignore-existing "local/${MINIO_WAREHOUSE_BUCKET:-warehouse}"
mc mb --ignore-existing "local/${MINIO_RAW_BUCKET:-raw}"
mc mb --ignore-existing "local/${MINIO_STAGING_BUCKET:-staging}"

echo "MinIO buckets ready."

# Hand control back to MinIO (wait for it)
wait $MINIO_PID
