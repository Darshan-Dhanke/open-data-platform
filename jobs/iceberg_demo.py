"""
iceberg_demo.py — end-to-end "real data" round-trip for the platform.

Run inside the Spark cluster:

    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/jobs/iceberg_demo.py

It writes a small Iceberg table through the Hive Metastore into MinIO. The
same table is then readable from Trino's `iceberg` catalog
(`SELECT * FROM iceberg.demo.events`), proving Spark (write) and Trino (read)
share one lakehouse. Catalog / S3A settings come from the mounted
spark-defaults.conf, so this script stays configuration-free.

Idempotent: it drops and recreates the demo table on each run.
"""

from datetime import datetime

from pyspark.sql import SparkSession


CATALOG = "iceberg"
SCHEMA = "demo"
TABLE = f"{CATALOG}.{SCHEMA}.events"


def main() -> None:
    spark = (
        SparkSession.builder.appName("iceberg-demo")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[iceberg_demo] Creating namespace {CATALOG}.{SCHEMA} ...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SCHEMA}")

    print(f"[iceberg_demo] (Re)creating table {TABLE} ...")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(
        f"""
        CREATE TABLE {TABLE} (
            id     BIGINT,
            name   STRING,
            amount DOUBLE,
            ts     TIMESTAMP
        ) USING iceberg
        """
    )

    now = datetime.utcnow()
    rows = [
        (1, "alpha",   100.0, now),
        (2, "bravo",   250.5, now),
        (3, "charlie",  75.25, now),
        (4, "delta",   500.0, now),
    ]
    df = spark.createDataFrame(rows, schema="id BIGINT, name STRING, amount DOUBLE, ts TIMESTAMP")
    df.writeTo(TABLE).append()

    count = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE}").collect()[0]["c"]
    total = spark.sql(f"SELECT SUM(amount) AS s FROM {TABLE}").collect()[0]["s"]
    print(f"[iceberg_demo] Wrote {count} rows, amount sum = {total}")
    print("[iceberg_demo] Sample:")
    spark.sql(f"SELECT * FROM {TABLE} ORDER BY id").show(truncate=False)

    print("[iceberg_demo] DONE — table is now queryable from Trino as "
          f"{TABLE}")
    spark.stop()


if __name__ == "__main__":
    main()
