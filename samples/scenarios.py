"""
Sample stack scenarios.

Each entry is a complete, tested selection of one component per layer. They are
deliberately varied to cover all three table formats (Iceberg, Delta, Hudi) and
a spread of the alternative components, while staying small enough to run one at
a time on a typical laptop (~8-10 GiB to Docker).

Used by run_sample.py (boot one) and validate_all.py (generate-check all).
"""

SCENARIOS: dict[str, dict] = {
    # 1) The flagship Iceberg lakehouse with CDC, lineage and monitoring.
    "iceberg-lakehouse": {
        "description": "Iceberg + Hive Metastore + Kafka/Debezium CDC + Spark + "
                       "Trino + Airflow + Marquez lineage + Prometheus/Grafana.",
        "selections": {
            "storage": "minio",
            "table_format": "iceberg",
            "metastore": "hive_metastore",
            "ingestion": "kafka_debezium",
            "processing": "spark",
            "query_engine": "trino",
            "orchestration": "airflow",
            "catalog": "marquez",
            "observability": "prometheus_grafana",
        },
    },

    # 2) Delta lakehouse with Soda data-quality checks. NOTE: Soda (like Great
    #    Expectations) runs as an Airflow DAG on the Airflow worker, so it
    #    requires orchestration=airflow.
    "delta-lakehouse": {
        "description": "Delta Lake + Hive Metastore + Spark + Trino + Airflow + "
                       "Soda data-quality checks.",
        "selections": {
            "storage": "minio",
            "table_format": "delta",
            "metastore": "hive_metastore",
            "processing": "spark",
            "query_engine": "trino",
            "orchestration": "airflow",
            "quality": "soda",
        },
    },

    # 3) The lightest possible Hudi lakehouse (no orchestration).
    "hudi-lakehouse": {
        "description": "Hudi + Hive Metastore + Spark + Trino. Minimal footprint.",
        "selections": {
            "storage": "minio",
            "table_format": "hudi",
            "metastore": "hive_metastore",
            "processing": "spark",
            "query_engine": "trino",
        },
    },

    # 4) Alternatives showcase — NiFi, Flink, Dagster, Metabase, Netdata.
    "alternatives-showcase": {
        "description": "Iceberg + NiFi ingestion + Flink processing + Trino + "
                       "Dagster orchestration + Metabase BI + Netdata monitoring.",
        "selections": {
            "storage": "minio",
            "table_format": "iceberg",
            "metastore": "hive_metastore",
            "ingestion": "nifi",
            "processing": "flink",
            "query_engine": "trino",
            "orchestration": "dagster",
            "visualization": "metabase",
            "observability": "netdata",
        },
    },

    # 5) Lean Delta with lightweight alternatives (Prefect + Metabase).
    "lean-delta-alt": {
        "description": "Delta + Spark + Trino + Prefect orchestration + "
                       "Metabase BI. Lightweight alternative stack.",
        "selections": {
            "storage": "minio",
            "table_format": "delta",
            "metastore": "hive_metastore",
            "processing": "spark",
            "query_engine": "trino",
            "orchestration": "prefect",
            "visualization": "metabase",
        },
    },
}
