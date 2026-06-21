"""
CDC consumer — Kafka (Debezium) -> bronze Iceberg, as an always-on Spark
Structured Streaming job.

It subscribes to every Debezium topic for the e-commerce source
(ecom.public.*), and appends each change event into a single generic bronze
table as a raw CDC log:

    iceberg.bronze.cdc_events(table_name, op, ts_ms, after, ingest_ts)

Keeping bronze generic (one streaming query, the row stored as a JSON string in
`after`) keeps the consumer simple and schema-agnostic; the silver layer (Trino
SQL) projects typed columns and reduces to latest-state-per-key. Append-only +
Iceberg snapshot isolation means Trino can read bronze safely while this writes.

Submitted by the spark-cdc-consumer service (see compose.override.yml). Catalog
/ S3A settings come from the mounted spark-defaults.conf.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, get_json_object, current_timestamp, regexp_extract, coalesce,
)

BRONZE_TABLE = "iceberg.bronze.cdc_events"
CHECKPOINT = "s3a://warehouse/checkpoints/ecom_cdc_v2"
KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC_PATTERN = "ecom\\.public\\..*"


def main() -> None:
    spark = SparkSession.builder.appName("ecommerce-cdc-consumer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Bronze table must exist before the streaming sink writes to it.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            table_name STRING,
            op         STRING,
            ts_ms      BIGINT,
            after      STRING,
            ingest_ts  TIMESTAMP
        ) USING iceberg
        """
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Debezium with JsonConverter + schemas.enable=false emits the change
    # fields at the top level ($.op/$.after); with schemas.enable=true they sit
    # under $.payload. Coalesce both so the consumer works either way.
    v = col("value").cast("string")
    events = raw.select(
        regexp_extract(col("topic"), r"ecom\.public\.(.*)", 1).alias("table_name"),
        coalesce(get_json_object(v, "$.op"), get_json_object(v, "$.payload.op")).alias("op"),
        coalesce(get_json_object(v, "$.ts_ms"), get_json_object(v, "$.payload.ts_ms")).cast("long").alias("ts_ms"),
        coalesce(get_json_object(v, "$.after"), get_json_object(v, "$.payload.after")).alias("after"),
        current_timestamp().alias("ingest_ts"),
    ).where(col("table_name") != "")

    query = (
        events.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .option("fanout-enabled", "true")
        .trigger(processingTime="10 seconds")
        .toTable(BRONZE_TABLE)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
