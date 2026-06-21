"""
Spark service block.

Runs a Spark standalone cluster: one master + configurable number of workers.
Worker count is controlled via SPARK_WORKER_REPLICAS in .env.

Handles three processing selections:
  - spark            : master + workers only
  - spark_and_flink  : same, Flink handles its own block separately

Table format and metastore selections are read to inject the correct
JARs and catalog config into SPARK_EXTRA_CLASSPATH and spark-defaults.conf.

Exposes:
  - Spark Master UI  : http://localhost:${SPARK_MASTER_UI_PORT}  (default 8081)
  - Spark Master     : spark://spark-master:7077  (internal)
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# JAR coordinates per table format
# Injected into spark-defaults.conf so jobs pick them up without --jars
# ---------------------------------------------------------------------------

FORMAT_JARS = {
    "iceberg": (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
    ),
    "delta": (
        "io.delta:delta-spark_2.12:3.1.0"
    ),
    "hudi": (
        "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"
    ),
}

# Catalog extension class per table format
CATALOG_EXTENSIONS = {
    "iceberg": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "delta":   "io.delta.sql.DeltaSparkSessionExtension",
    "hudi":    "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
}

# Spark catalog *plugin* class per table format + metastore combination.
# For Iceberg this must be org.apache.iceberg.spark.SparkCatalog (a Spark
# CatalogPlugin); the underlying catalog (Hive vs Nessie) is then selected via
# the `type`/`catalog-impl` property in spark-defaults. Using HiveCatalog here
# directly fails at runtime: "does not implement CatalogPlugin".
CATALOG_IMPL = {
    ("iceberg", "hive_metastore"): "org.apache.iceberg.spark.SparkCatalog",
    ("iceberg", "nessie"):         "org.apache.iceberg.spark.SparkCatalog",
    ("delta",   "hive_metastore"): "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    ("hudi",    "hive_metastore"): "org.apache.hudi.hadoop.HoodieParquetInputFormat",
}

# Hadoop S3A connector — required for Spark to read/write s3a:// (MinIO).
# Not bundled in the base Spark distribution; pulled via ivy alongside the
# table-format runtime. Version must match the Hadoop libs in Spark 3.5.x.
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


# Official Apache Spark image — stable, publicly available, no vendor registry required
# Uses the spark:3.5.x line which has the widest connector compatibility
SPARK_IMAGE = "darshandhanke07/odp-spark:3.5.1"

def service_blocks(selections: dict) -> dict:
    table_format = selections.get("table_format", "iceberg")
    metastore    = selections.get("metastore",    "hive_metastore")
    storage      = selections.get("storage",      "minio")

    spark_env = _spark_environment(table_format, metastore, storage)
    spark_defaults = _spark_defaults_content(table_format, metastore, storage)

    depends: dict = {}
    if metastore == "hive_metastore":
        depends["hive-metastore"] = {"condition": "service_healthy"}
    elif metastore == "nessie":
        depends["nessie"] = {"condition": "service_healthy"}
    if storage == "minio":
        depends["minio-init"] = {"condition": "service_completed_successfully"}

    blocks = {}

    blocks["spark-master"] = {
        "image": SPARK_IMAGE,
        "container_name": "spark-master",
        "environment": {
            **spark_env,
            "SPARK_NO_DAEMONIZE": "true",
        },
        "command": ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master",
                    "--host", "spark-master", "--port", "7077", "--webui-port", "8080"],
        "ports": [
            "${SPARK_MASTER_UI_PORT:-8081}:8080",
            "7077:7077",
        ],
        "volumes": _spark_volumes(),
        "depends_on": depends,
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -sf http://localhost:8080 || exit 1"],
            "interval": "30s",
            "timeout": "10s",
            "retries": 3,
            "start_period": "30s",
        },
        "networks": ["platform"],
        "user": "root",
    }

    blocks["spark-worker"] = {
        "image": SPARK_IMAGE,
        "environment": {
            **spark_env,
            "SPARK_NO_DAEMONIZE": "true",
            "SPARK_WORKER_MEMORY": "${SPARK_WORKER_MEMORY:-2g}",
            "SPARK_WORKER_CORES":  "${SPARK_WORKER_CORES:-2}",
        },
        "command": ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker",
                    "spark://spark-master:7077",
                    "--webui-port", "8081"],
        "volumes": _spark_volumes(),
        "depends_on": {
            "spark-master": {"condition": "service_healthy"},
        },
        "deploy": {
            "replicas": "${SPARK_WORKER_REPLICAS:-1}",
        },
        "networks": ["platform"],
        "user": "root",
    }

    # Write spark-defaults.conf into the configs directory so the volume
    # mount picks it up. This is done at compose generation time.
    _write_spark_defaults(spark_defaults)

    return blocks


def _spark_environment(table_format: str, metastore: str, storage: str) -> dict:
    env: dict = {
        "SPARK_RPC_AUTHENTICATION_ENABLED": "no",
        "SPARK_RPC_ENCRYPTION_ENABLED":     "no",
        "SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED": "no",
        "SPARK_SSL_ENABLED": "no",
        "SPARK_DRIVER_MEMORY":   "${SPARK_DRIVER_MEMORY:-1g}",
        "SPARK_EXECUTOR_MEMORY": "${SPARK_EXECUTOR_MEMORY:-1g}",
    }

    if storage == "minio":
        env.update({
            "AWS_ACCESS_KEY_ID":     "${AWS_ACCESS_KEY_ID:-admin}",
            "AWS_SECRET_ACCESS_KEY": "${AWS_SECRET_ACCESS_KEY:-password123}",
        })

    if metastore == "hive_metastore":
        env["SPARK_EXTRA_CLASSPATH"] = (
            "/opt/spark/jars/postgresql.jar"
        )

    return env


def _spark_volumes() -> list:
    return [
        "./configs/spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro",
        "./configs/spark/hive-site.xml:/opt/spark/conf/hive-site.xml:ro",
        # PostgreSQL JDBC driver, bundled in the repo (configs/spark/postgresql.jar)
        # and referenced by SPARK_EXTRA_CLASSPATH. Mounted rather than downloaded
        # so startup needs no network and no wget/curl inside the image.
        "./configs/spark/postgresql.jar:/opt/spark/jars/postgresql.jar:ro",
        # Spark job scripts (e.g. jobs/iceberg_demo.py) so they can be
        # submitted with: docker exec spark-master spark-submit /opt/jobs/<job>.py
        "./jobs:/opt/jobs:ro",
    ]


def _spark_defaults_content(
    table_format: str,
    metastore: str,
    storage: str,
) -> str:
    lines = [
        "# spark-defaults.conf",
        "# Auto-generated by open-data-platform setup.py",
        "# Edit values here to tune Spark behaviour without rebuilding.",
        "",
    ]

    # Table format JARs (+ hadoop-aws for S3A/MinIO) — downloaded by Spark at
    # startup via ivy. hadoop-aws is what provides S3AFileSystem; without it
    # any s3a:// access fails with ClassNotFoundException.
    packages = [p for p in (FORMAT_JARS.get(table_format, ""),) if p]
    if storage == "minio":
        packages.append(HADOOP_AWS_PACKAGE)
    if packages:
        lines += [
            f"spark.jars.packages                    {','.join(packages)}",
            "spark.jars.ivy                         /tmp/ivy",
            "",
        ]

    # SQL extensions
    ext = CATALOG_EXTENSIONS.get(table_format, "")
    if ext:
        lines += [
            f"spark.sql.extensions                   {ext}",
            "",
        ]

    # Catalog configuration
    catalog_impl = CATALOG_IMPL.get((table_format, metastore), "")

    if table_format == "iceberg":
        lines += [
            "spark.sql.catalog.spark_catalog        "
            "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.spark_catalog.type   hive",
            f"spark.sql.catalog.iceberg              {catalog_impl}",
        ]
        if metastore == "hive_metastore":
            lines += [
                "spark.sql.catalog.iceberg.type         hive",
                "spark.sql.catalog.iceberg.uri          "
                "thrift://hive-metastore:9083",
                "spark.sql.catalog.iceberg.warehouse    "
                "s3a://warehouse/iceberg",
                # The standalone Hive Metastore (apache/hive:4.0.0) does not
                # implement the table-lock API, so Iceberg's HiveCatalog fails
                # with "Internal error processing lock". Disabling Hive locks
                # makes Iceberg rely on the metastore's atomic alter-table for
                # commit isolation, which the standalone HMS does support.
                "spark.hadoop.iceberg.engine.hive.lock-enabled false",
            ]
        elif metastore == "nessie":
            lines += [
                "spark.sql.catalog.iceberg.catalog-impl "
                "org.apache.iceberg.nessie.NessieCatalog",
                "spark.sql.catalog.iceberg.uri          "
                "http://nessie:19120/api/v1",
                "spark.sql.catalog.iceberg.ref          main",
                "spark.sql.catalog.iceberg.warehouse    "
                "s3a://warehouse/iceberg",
            ]

    elif table_format == "delta":
        lines += [
            f"spark.sql.catalog.spark_catalog        {catalog_impl}",
            "spark.sql.catalog.spark_catalog.warehouse "
            "s3a://warehouse/delta",
            "spark.databricks.delta.retentionDurationCheck.enabled false",
        ]

    elif table_format == "hudi":
        lines += [
            "spark.serializer                       "
            "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.catalog.spark_catalog        "
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            "spark.kryo.registrator                 "
            "org.apache.spark.HoodieSparkKryoRegistrar",
        ]

    lines.append("")

    # S3A / MinIO settings.
    #
    # NOTE: spark-defaults.conf is read verbatim — it is NOT shell-expanded,
    # so an env placeholder like ${MINIO_ENDPOINT} would be passed to Spark
    # literally and break S3A. Concrete values are baked in here.
    #
    # Credentials come from EnvironmentVariableCredentialsProvider, which
    # reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from the container
    # environment (set on the spark services from .env). This keeps secrets
    # out of the config file and honours .env overrides. SimpleAWS-
    # CredentialsProvider would instead require fs.s3a.access.key/secret.key
    # in this file, which are not set — so any S3A write would fail to auth.
    if storage == "minio":
        lines += [
            "# S3A / MinIO",
            "spark.hadoop.fs.s3a.endpoint           http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access  true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled false",
            "spark.hadoop.fs.s3a.impl               "
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider "
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
            "",
        ]

    # Hive Metastore connectivity
    if metastore == "hive_metastore":
        lines += [
            "# Hive Metastore",
            "spark.hadoop.hive.metastore.uris       thrift://hive-metastore:9083",
            "spark.sql.warehouse.dir                "
            "s3a://warehouse/spark-warehouse",
            "",
        ]

    return "\n".join(lines)


def _write_spark_defaults(content: str) -> None:
    """
    Write spark-defaults.conf and a minimal hive-site.xml into configs/spark/
    so they are copied into generated/configs/spark/ by the composer.
    """
    from pathlib import Path
    spark_conf_dir = Path("configs/spark")
    spark_conf_dir.mkdir(parents=True, exist_ok=True)

    defaults_path = spark_conf_dir / "spark-defaults.conf"
    defaults_path.write_text(content, encoding="utf-8", newline="\n")

    hive_site_path = spark_conf_dir / "hive-site.xml"
    if not hive_site_path.exists():
        hive_site_path.write_text(_hive_site_xml(), encoding="utf-8", newline="\n")


def _hive_site_xml() -> str:
    return """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  hive-site.xml for Spark
  Tells Spark where to find the Hive Metastore thrift server.
  Also mounted read-only into spark-master and spark-worker.
-->
<configuration>

  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://hive-metastore:9083</value>
  </property>

  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>s3a://warehouse/hive</value>
  </property>

  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>

</configuration>
"""


def env_vars(selections: dict) -> dict:
    return {
        "SPARK_DRIVER_MEMORY":    "1g",
        "SPARK_EXECUTOR_MEMORY":  "1g",
        "SPARK_WORKER_MEMORY":    "2g",
        "SPARK_WORKER_CORES":     "2",
        "SPARK_WORKER_REPLICAS":  "1",
        "SPARK_MASTER_UI_PORT":   "8081",
    }


def named_volumes(selections: dict) -> list[str]:
    return []


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
