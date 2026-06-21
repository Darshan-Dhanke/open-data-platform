"""
iceberg_etl_dag.py

A real (small) ETL pipeline orchestrated by Airflow:

  build_table  -> creates an Iceberg table on MinIO from the built-in TPCH
                  dataset, via Trino (CREATE TABLE AS SELECT).
  verify_table -> queries the new table back and asserts it has rows.

This exercises the full lakehouse path — Airflow (orchestration) -> Trino
(query/write) -> Iceberg + Hive Metastore (table format/catalog) -> MinIO
(storage) — using only the Python standard library, so it runs on the stock
Airflow worker with no extra providers or drivers.
"""

import json
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


TRINO_STATEMENT_URL = "http://trino:8080/v1/statement"
TRINO_HEADERS = {"X-Trino-User": "airflow", "Content-Type": "text/plain"}

SCHEMA = "iceberg.etl"
TABLE = f"{SCHEMA}.top_customers"


def _trino_query(sql: str) -> list:
    """Run a SQL statement against Trino's REST API and return result rows.

    Trino streams results across a chain of nextUri pages; we follow them to
    completion and surface any server-side error as an exception so the task
    fails loudly.
    """
    req = urllib.request.Request(
        TRINO_STATEMENT_URL, data=sql.encode("utf-8"),
        headers=TRINO_HEADERS, method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.load(resp)

    rows: list = []
    while True:
        if payload.get("data"):
            rows.extend(payload["data"])
        if payload.get("error"):
            msg = payload["error"].get("message", "unknown error")
            raise RuntimeError(f"Trino query failed: {msg}\nSQL: {sql}")
        next_uri = payload.get("nextUri")
        if not next_uri:
            break
        nreq = urllib.request.Request(next_uri, headers=TRINO_HEADERS)
        with urllib.request.urlopen(nreq, timeout=30) as resp:
            payload = json.load(resp)
    return rows


def build_table():
    _trino_query(
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} "
        "WITH (location = 's3a://warehouse/iceberg/etl')"
    )
    _trino_query(f"DROP TABLE IF EXISTS {TABLE}")
    _trino_query(
        f"CREATE TABLE {TABLE} AS "
        "SELECT custkey, name, acctbal, nationkey "
        "FROM tpch.tiny.customer WHERE acctbal > 5000"
    )
    print(f"Built {TABLE} from tpch.tiny.customer (acctbal > 5000)")


def verify_table():
    count_rows = _trino_query(f"SELECT count(*) FROM {TABLE}")
    n = int(count_rows[0][0]) if count_rows else 0
    print(f"{TABLE} row count = {n}")
    if n <= 0:
        raise ValueError(f"Expected rows in {TABLE}, found none")
    sample = _trino_query(
        f"SELECT custkey, name, acctbal FROM {TABLE} ORDER BY acctbal DESC LIMIT 5"
    )
    print("Top 5 customers by account balance:")
    for row in sample:
        print("  ", row)


default_args = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="iceberg_etl_demo",
    default_args=default_args,
    description="Build and verify an Iceberg table from TPCH data via Trino",
    schedule=None,  # trigger manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["platform", "etl", "iceberg"],
) as dag:

    build = PythonOperator(task_id="build_table", python_callable=build_table)
    verify = PythonOperator(task_id="verify_table", python_callable=verify_table)

    build >> verify
