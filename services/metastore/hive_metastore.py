"""
Hive Metastore service block.

Self-contained entrypoint:
  1. Downloads PostgreSQL JDBC driver (not bundled in apache/hive:4.0.0)
     Uses wget (preferred) or curl — both present in the apache/hive image.
     The original java-based downloader was fragile on Windows/WSL2 DNS.
  2. Validates the jar is not a truncated HTML error page.
  3. Calls the original /entrypoint.sh hivemetastore

Exposes:
  - Thrift API : localhost:${HMS_THRIFT_PORT} (default 9083, internal)

Upstream: https://hub.docker.com/r/apache/hive
"""

from __future__ import annotations
from pathlib import Path


def service_blocks(selections: dict) -> dict:
    storage = selections.get("storage", "minio")
    warehouse_path = _warehouse_path(storage)

    _write_entrypoint()

    blocks = {}

    # NOTE: the postgresql service is now provided by services/base/postgres.py
    # (always included by the composer), so the Hive Metastore just depends on
    # it rather than defining it. This lets metastore=nessie etc. still get a
    # Postgres.
    depends: dict = {"postgresql": {"condition": "service_healthy"}}
    if storage == "minio":
        depends["minio-init"] = {"condition": "service_completed_successfully"}

    blocks["hive-metastore"] = {
        "image": "darshandhanke07/odp-hive:4.0.0",
        "container_name": "hive-metastore",
        "depends_on": depends,
        "environment": _hms_environment(storage, warehouse_path),
        "ports": ["${HMS_THRIFT_PORT:-9083}:9083"],
        "entrypoint": ["/bin/bash", "/entrypoint-custom.sh"],
        "volumes": [
            # NOTE: do NOT mount postgresql_data here. That volume belongs
            # to the postgresql container. Mounting it in HMS too caused
            # HMS to boot with a stale/unexpected PG data directory visible
            # inside the container, which confused the Hive schematool.
            "./configs/hive-metastore/hive-site.xml:/opt/hive/conf/hive-site.xml:ro",
            "./configs/hive-metastore/entrypoint-custom.sh:/entrypoint-custom.sh:ro",
            # PostgreSQL JDBC driver, bundled in the repo and mounted directly.
            # apache/hive:4.0.0 has no wget/curl, so the previous runtime
            # download approach failed repeatedly. The jar lives at
            # configs/hive-metastore/postgresql.jar and is carried into
            # generated/configs by setup.py's copytree.
            "./configs/hive-metastore/postgresql.jar:/opt/hive/lib/postgresql.jar:ro",
        ],
        "healthcheck": {
            # bash /dev/tcp is a shell built-in — no nc required.
            # nc is not guaranteed to be present in the apache/hive image.
            "test": ["CMD-SHELL", "bash -c 'echo > /dev/tcp/localhost/9083' 2>/dev/null || exit 1"],
            "interval": "30s",
            "timeout": "10s",
            "retries": 5,
            "start_period": "120s",
        },
        "networks": ["platform"],
    }

    return blocks


def _write_entrypoint() -> None:
    d = Path("configs/hive-metastore")
    d.mkdir(parents=True, exist_ok=True)
    # Write entrypoint as binary from base64 — immune to CRLF conversion by
    # Git autocrlf / Windows zip tools. Shell scripts with CRLF line endings
    # fail with "bad interpreter" on Linux containers.
    #
    # This entrypoint does NOT download anything. The PostgreSQL JDBC driver
    # is bundled at configs/hive-metastore/postgresql.jar and mounted into the
    # container by docker-compose. apache/hive:4.0.0 ships without wget or
    # curl, so every runtime-download strategy (java downloader, wget/curl,
    # java stdin) failed in a loop. Bundling the jar removes the failure mode
    # entirely. The script just verifies the jar is present, then execs Hive.
    lines = [
        "#!/bin/bash",
        "set -e",
        "",
        "# The PostgreSQL JDBC driver is bundled in the repo and mounted read-only",
        "# at /opt/hive/lib/postgresql.jar by docker-compose. No network download",
        "# happens here — apache/hive:4.0.0 ships without wget/curl, and runtime",
        "# downloads were the source of repeated metastore startup failures.",
        "",
        "JDBC_JAR=/opt/hive/lib/postgresql.jar",
        "",
        'if [ ! -f "$JDBC_JAR" ]; then',
        '    echo "FATAL: $JDBC_JAR is not present."',
        '    echo "It should be bind-mounted from configs/hive-metastore/postgresql.jar."',
        '    echo "Re-run setup.py (which restores the jar) and recreate this container."',
        "    exit 1",
        "fi",
        "",
        'JAR_SIZE=$(stat -c%s "$JDBC_JAR" 2>/dev/null || echo 0)',
        'if [ "$JAR_SIZE" -lt 100000 ]; then',
        '    echo "FATAL: $JDBC_JAR is only ${JAR_SIZE} bytes — looks corrupt."',
        "    exit 1",
        "fi",
        "",
        "# Put the Hadoop S3A connector on the metastore classpath so the metastore",
        "# can create and manage table/namespace locations on s3a:// (MinIO). The",
        "# hadoop-aws and aws-java-sdk-bundle jars ship in the image under hadoop's",
        "# tools/lib, which is NOT on the Hive classpath by default — without this,",
        "# any CREATE SCHEMA/TABLE with an s3a:// location fails with",
        '# "ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem".',
        'S3A_JARS=$(ls /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-*.jar '
        '/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar 2>/dev/null | tr "\\n" ":")',
        'export HADOOP_CLASSPATH="${S3A_JARS}${HADOOP_CLASSPATH}"',
        "export HADOOP_OPTIONAL_TOOLS=hadoop-aws",
        "",
        'echo "JDBC driver present: $JDBC_JAR (${JAR_SIZE} bytes). Starting Hive Metastore..."',
        'echo "S3A on metastore classpath: ${S3A_JARS:-<none found>}"',
        "exec /entrypoint.sh hivemetastore",
        "",
    ]
    (d / "entrypoint-custom.sh").write_bytes("\n".join(lines).encode("utf-8"))

def _warehouse_path(storage: str) -> str:
    if storage == "hdfs":
        return "hdfs://namenode:8020/user/hive/warehouse"
    return "s3a://${MINIO_WAREHOUSE_BUCKET:-warehouse}/hive"


def _hms_environment(storage: str, warehouse_path: str) -> dict:
    env = {
        "SERVICE_NAME": "metastore",
        "DB_DRIVER":    "postgres",
        # Use literal "metastore" for the DB name — the apache/hive:4.0.0
        # entrypoint does not always expand shell variables inside SERVICE_OPTS
        # before passing them to schematool, causing it to fall back to a DB
        # named "hive" (the username) which does not exist. Hardcoding avoids
        # that ambiguity. Username/password remain variable-interpolated since
        # those are handled correctly by the image.
        "SERVICE_OPTS": (
            "-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver "
            "-Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgresql:5432/metastore "
            "-Djavax.jdo.option.ConnectionUserName=${POSTGRES_USER:-hive} "
            "-Djavax.jdo.option.ConnectionPassword=${POSTGRES_PASSWORD:-hive}"
        ),
        "HIVE_METASTORE_WAREHOUSE_DIR": warehouse_path,
    }
    if storage == "minio":
        env.update({
            "AWS_ACCESS_KEY_ID":     "${AWS_ACCESS_KEY_ID:-admin}",
            "AWS_SECRET_ACCESS_KEY": "${AWS_SECRET_ACCESS_KEY:-password123}",
            # Credentials come from the AWS_* env vars above via the SDK's
            # EnvironmentVariableCredentialsProvider. SimpleAWSCredentialsProvider
            # would instead require fs.s3a.access.key/secret.key (which are not
            # set), so it fails to authenticate to MinIO and CREATE SCHEMA/TABLE
            # at an s3a:// location errors with "Failed to create external path".
            # ssl.enabled=false because the MinIO endpoint is plain HTTP.
            "HIVE_METASTORE_HADOOP_OPTS": (
                "-Dfs.s3a.endpoint=${MINIO_ENDPOINT:-http://minio:9000} "
                "-Dfs.s3a.path.style.access=true "
                "-Dfs.s3a.connection.ssl.enabled=false "
                "-Dfs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
                "-Dfs.s3a.aws.credentials.provider="
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
            ),
        })
    return env


def env_vars(selections: dict) -> dict:
    # POSTGRES_* now come from the base postgres module.
    return {
        "HMS_THRIFT_PORT":   "9083",
    }


def named_volumes(selections: dict) -> list[str]:
    # postgresql_data is owned by the base postgres module now.
    return []


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
