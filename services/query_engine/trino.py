"""
Trino service block.

Runs a single Trino coordinator. For production-like setups, additional
worker nodes can be added by adjusting TRINO_WORKER_REPLICAS in .env —
the coordinator discovery URI is pre-configured for this.

Catalog properties files are written to configs/trino/etc/catalog/ at
generation time and mounted read-only into the container as part of the
single /etc/trino tree.

Exposes:
  - Trino UI / JDBC : http://localhost:${TRINO_PORT}  (default 8080)
"""

from __future__ import annotations

from pathlib import Path


# ---------------------------------------------------------------------------
# Catalog property templates per table format + metastore
# ---------------------------------------------------------------------------

def _iceberg_hms_catalog() -> str:
    return """connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=${ENV:MINIO_ENDPOINT}
hive.s3.path-style-access=true
hive.s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
hive.s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
hive.s3.ssl.enabled=false
iceberg.file-format=PARQUET
"""


def _iceberg_nessie_catalog() -> str:
    return """connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://nessie:19120/iceberg
hive.s3.endpoint=${ENV:MINIO_ENDPOINT}
hive.s3.path-style-access=true
hive.s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
hive.s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
hive.s3.ssl.enabled=false
iceberg.file-format=PARQUET
"""


def _delta_hms_catalog() -> str:
    return """connector.name=delta_lake
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=${ENV:MINIO_ENDPOINT}
hive.s3.path-style-access=true
hive.s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
hive.s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
hive.s3.ssl.enabled=false
delta.metadata.cache-size=1000
"""


def _hudi_hms_catalog() -> str:
    return """connector.name=hudi
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=${ENV:MINIO_ENDPOINT}
hive.s3.path-style-access=true
hive.s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
hive.s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
hive.s3.ssl.enabled=false
"""


def _tpch_catalog() -> str:
    """Always included — useful for testing Trino is alive without real data."""
    return "connector.name=tpch\n"


def _postgresql_catalog() -> str:
    """Always included — provides direct SQL access to the HMS backend Postgres."""
    return """connector.name=postgresql
connection-url=jdbc:postgresql://postgresql:5432/${ENV:POSTGRES_DB}
connection-user=${ENV:POSTGRES_USER}
connection-password=${ENV:POSTGRES_PASSWORD}
"""


CATALOG_CONTENT = {
    ("iceberg", "hive_metastore"): ("iceberg",    _iceberg_hms_catalog),
    ("iceberg", "nessie"):         ("iceberg",    _iceberg_nessie_catalog),
    ("delta",   "hive_metastore"): ("delta_lake", _delta_hms_catalog),
    ("hudi",    "hive_metastore"): ("hudi",       _hudi_hms_catalog),
}


def service_blocks(selections: dict) -> dict:
    table_format = selections.get("table_format", "iceberg")
    metastore    = selections.get("metastore",    "hive_metastore")
    storage      = selections.get("storage",      "minio")

    depends: dict = {}
    if metastore == "hive_metastore":
        depends["hive-metastore"] = {"condition": "service_healthy"}
    elif metastore == "nessie":
        depends["nessie"] = {"condition": "service_healthy"}
    if storage == "minio":
        depends["minio-init"] = {"condition": "service_completed_successfully"}

    _write_catalogs(table_format, metastore)
    _write_trino_config()

    return {
        "trino": {
            "image": "darshandhanke07/odp-trino:435",
            "container_name": "trino",
            "ports": [
                "${TRINO_PORT:-8080}:8080",
            ],
            # Single read-only mount of the whole etc/ tree. The catalog
            # directory lives at configs/trino/etc/catalog so it is covered
            # by this one mount. A separate nested mount of
            # /etc/trino/catalog fails on Docker Desktop because the parent
            # /etc/trino is already mounted read-only, so the daemon cannot
            # create the nested mountpoint ("read-only file system").
            "volumes": [
                "./configs/trino/etc:/etc/trino:ro",
            ],
            "environment": {
                "AWS_ACCESS_KEY_ID":     "${AWS_ACCESS_KEY_ID:-admin}",
                "AWS_SECRET_ACCESS_KEY": "${AWS_SECRET_ACCESS_KEY:-password123}",
                "MINIO_ENDPOINT":        "${MINIO_ENDPOINT:-http://minio:9000}",
                "POSTGRES_USER":         "${POSTGRES_USER:-hive}",
                "POSTGRES_PASSWORD":     "${POSTGRES_PASSWORD:-hive}",
                "POSTGRES_DB":           "${POSTGRES_DB:-metastore}",
            },
            "depends_on": depends,
            "healthcheck": {
                "test": [
                    "CMD", "curl", "-f",
                    "http://localhost:8080/v1/info",
                ],
                "interval": "30s",
                "timeout": "10s",
                "retries": 5,
                "start_period": "60s",
            },
            "networks": ["platform"],
        }
    }


def _write_catalogs(table_format: str, metastore: str) -> None:
    # Catalog files live under etc/ so they are covered by the single
    # ./configs/trino/etc:/etc/trino mount (see service_blocks).
    catalog_dir = Path("configs/trino/etc/catalog")
    catalog_dir.mkdir(parents=True, exist_ok=True)

    # Prune any lakehouse catalog left over from a previous generation with a
    # different table_format. Without this, switching presets (e.g. Iceberg ->
    # Delta) leaves a stale iceberg.properties behind and Trino loads both
    # catalogs. The always-on utility catalogs (tpch, postgresql) are rewritten
    # below and are intentionally not pruned.
    for catalog_name, _ in CATALOG_CONTENT.values():
        stale = catalog_dir / f"{catalog_name}.properties"
        if stale.exists():
            stale.unlink()

    # Main data lakehouse catalog
    key = (table_format, metastore)
    if key in CATALOG_CONTENT:
        catalog_name, content_fn = CATALOG_CONTENT[key]
        (catalog_dir / f"{catalog_name}.properties").write_text(
            content_fn(), encoding="utf-8"
        )

    # Always-on utility catalogs
    (catalog_dir / "tpch.properties").write_text(_tpch_catalog(), encoding="utf-8")
    (catalog_dir / "postgresql.properties").write_text(
        _postgresql_catalog(), encoding="utf-8"
    )


def _write_trino_config() -> None:
    etc_dir = Path("configs/trino/etc")
    etc_dir.mkdir(parents=True, exist_ok=True)

    config = etc_dir / "config.properties"
    if not config.exists():
        config.write_text(
            "coordinator=true\n"
            "node-scheduler.include-coordinator=true\n"
            "http-server.http.port=8080\n"
            "discovery.uri=http://trino:8080\n"
            # query.max-total-memory-per-node was removed in Trino; setting
            # it makes the server refuse to start ("Defunct property").
            "query.max-memory=2GB\n"
            "query.max-memory-per-node=1GB\n",
            encoding="utf-8",
        )

    jvm_config = etc_dir / "jvm.config"
    if not jvm_config.exists():
        # NOTE: jvm.config is read verbatim by the launcher — it is NOT
        # shell-expanded, so an env placeholder like ${TRINO_MAX_HEAP}
        # would be passed to the JVM literally ("Invalid maximum heap
        # size"). The heap must be a concrete value baked in at generation
        # time. -Xmx must exceed query.max-total-memory-per-node (2GB).
        jvm_config.write_text(
            "-server\n"
            "-Xmx3G\n"
            "-XX:InitialRAMPercentage=80\n"
            "-XX:MaxRAMPercentage=80\n"
            "-XX:G1HeapRegionSize=32M\n"
            "-XX:+ExplicitGCInvokesConcurrent\n"
            "-XX:+HeapDumpOnOutOfMemoryError\n"
            "-XX:+ExitOnOutOfMemoryError\n"
            "-XX:ReservedCodeCacheSize=512M\n"
            "-Djdk.attach.allowAttachSelf=true\n"
            "-Dfile.encoding=UTF-8\n",
            encoding="utf-8",
        )

    node_props = etc_dir / "node.properties"
    if not node_props.exists():
        node_props.write_text(
            "node.environment=production\n"
            "node.id=trino-coordinator-1\n"
            "node.data-dir=/data/trino\n",
            encoding="utf-8",
        )

    log_props = etc_dir / "log.properties"
    if not log_props.exists():
        log_props.write_text(
            "io.trino=INFO\n",
            encoding="utf-8",
        )


def env_vars(selections: dict) -> dict:
    return {
        "TRINO_VERSION":           "435",
        "TRINO_PORT":              "8080",
        "TRINO_MAX_HEAP":          "3G",
        "TRINO_MAX_MEMORY":        "2GB",
        "TRINO_MAX_MEMORY_PER_NODE": "1GB",
    }


def named_volumes(selections: dict) -> list[str]:
    return []


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
