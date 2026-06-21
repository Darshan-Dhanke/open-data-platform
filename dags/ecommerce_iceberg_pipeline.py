"""
ecommerce_iceberg_pipeline  (combo-1 sample)

Orchestrates the batch transforms of the e-commerce CDC lakehouse:
  build_silver -> build_gold -> report

Bronze is fed continuously by the spark-cdc-consumer service (Kafka/Debezium ->
Iceberg). This DAG turns bronze into typed silver tables and business gold marts
by executing dags/ecommerce_iceberg_sql/{silver,gold}.sql against Trino over its
REST API (stdlib only — no Spark submission needed). With catalog=marquez the
Airflow OpenLineage provider emits this run's lineage to Marquez automatically.
"""

import json
import os
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

TRINO_URL = "http://trino:8080/v1/statement"
HEADERS = {"X-Trino-User": "airflow", "Content-Type": "text/plain"}
SQL_DIR = os.path.join(os.path.dirname(__file__), "ecommerce_iceberg_sql")

# --- OpenLineage: emit explicit dataset lineage to Marquez --------------------
# The PythonOperator is opaque to OpenLineage's auto-extraction, so we post
# RunEvents with explicit inputs/outputs. This makes Marquez draw the real
# bronze -> silver -> gold dataset graph (otherwise Datasets=0).
MARQUEZ_LINEAGE = "http://marquez:5000/api/v1/lineage"
JOB_NS = "open-data-platform"
DS_NS = "iceberg"  # dataset namespace = the Trino/Iceberg catalog


def _ol_post(event_type, run_id, job, inputs, outputs):
    ev = {
        "eventType": event_type,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": "https://github.com/Darshan-Dhanke/open-data-platform",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "run": {"runId": run_id},
        "job": {"namespace": JOB_NS, "name": job},
        "inputs": [{"namespace": DS_NS, "name": n} for n in inputs],
        "outputs": [{"namespace": DS_NS, "name": n} for n in outputs],
    }
    req = urllib.request.Request(MARQUEZ_LINEAGE, data=json.dumps(ev).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        urllib.request.urlopen(req, timeout=20)
    except Exception as e:  # lineage is best-effort, never fail the pipeline
        print(f"  (lineage emit warning: {e})")


def emit_lineage(job, inputs, outputs):
    rid = str(uuid.uuid4())
    _ol_post("START", rid, job, inputs, outputs)
    _ol_post("COMPLETE", rid, job, inputs, outputs)
    print(f"  emitted lineage: {inputs} -> {job} -> {outputs}")


def _trino(sql: str) -> list:
    req = urllib.request.Request(TRINO_URL, data=sql.encode(), headers=HEADERS, method="POST")
    with urllib.request.urlopen(req, timeout=60) as r:
        payload = json.load(r)
    rows: list = []
    while True:
        rows += payload.get("data", []) or []
        if payload.get("error"):
            raise RuntimeError(payload["error"].get("message", "trino error") + "\nSQL: " + sql[:200])
        nxt = payload.get("nextUri")
        if not nxt:
            return rows
        with urllib.request.urlopen(urllib.request.Request(nxt, headers=HEADERS), timeout=60) as r:
            payload = json.load(r)


def _strip_comments(chunk: str) -> str:
    # Drop full-line SQL comments so a comment before a statement doesn't cause
    # the whole statement to be skipped.
    return "\n".join(
        ln for ln in chunk.splitlines() if not ln.strip().startswith("--")
    ).strip()


def _run_sql_file(name: str):
    sql = open(os.path.join(SQL_DIR, name), encoding="utf-8").read()
    stmts = [c for c in (_strip_comments(p) for p in sql.split(";")) if c]
    for i, stmt in enumerate(stmts, 1):
        print(f"[{name}] statement {i}/{len(stmts)}")
        _trino(stmt)
    print(f"[{name}] done ({len(stmts)} statements)")


SILVER_TABLES = ["silver.customers", "silver.products", "silver.orders",
                 "silver.order_items", "silver.payments"]
GOLD_TABLES = ["gold.daily_revenue", "gold.top_products", "gold.customer_ltv"]


def build_silver():
    _run_sql_file("silver.sql")
    # Distinct job names (not the Airflow task names) so the Airflow auto-
    # OpenLineage events — which carry no datasets — don't clobber these edges.
    emit_lineage("ecommerce_iceberg.silver_transform",
                 inputs=["bronze.cdc_events"], outputs=SILVER_TABLES)


def build_gold():
    _run_sql_file("gold.sql")
    emit_lineage("ecommerce_iceberg.gold_transform",
                 inputs=SILVER_TABLES, outputs=GOLD_TABLES)


def data_quality():
    """Gate the pipeline: assert the gold marts are sane. Fails the run (and
    thus blocks downstream) if any expectation is violated."""
    checks = [
        ("daily_revenue has rows", "SELECT count(*) FROM iceberg.gold.daily_revenue", lambda v: v > 0),
        ("no null product in top_products",
         "SELECT count(*) FROM iceberg.gold.top_products WHERE product_id IS NULL", lambda v: v == 0),
        ("all revenue non-negative",
         "SELECT count(*) FROM iceberg.gold.daily_revenue WHERE revenue < 0", lambda v: v == 0),
        ("customer_ltv covers all customers",
         "SELECT (SELECT count(*) FROM iceberg.gold.customer_ltv) "
         "- (SELECT count(*) FROM iceberg.silver.customers)", lambda v: v == 0),
    ]
    failures = []
    for desc, sql, ok in checks:
        val = float(_trino(sql)[0][0])
        passed = ok(val)
        print(f"  [{'PASS' if passed else 'FAIL'}] {desc} (={val:g})")
        if not passed:
            failures.append(desc)
    if failures:
        raise ValueError(f"Data quality gate failed: {failures}")
    print("  all data-quality checks passed.")


def report():
    for tbl in ("daily_revenue", "top_products", "customer_ltv"):
        n = _trino(f"SELECT count(*) FROM iceberg.gold.{tbl}")
        print(f"  gold.{tbl}: {n[0][0]} rows")
    top = _trino("SELECT name, revenue FROM iceberg.gold.top_products ORDER BY revenue DESC LIMIT 3")
    print("  top products:", top)


default_args = {"owner": "platform", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="ecommerce_iceberg_pipeline",
    default_args=default_args,
    description="CDC e-commerce lakehouse: bronze -> silver -> gold (Trino)",
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sample", "ecommerce", "iceberg", "cdc"],
) as dag:
    s = PythonOperator(task_id="build_silver", python_callable=build_silver)
    g = PythonOperator(task_id="build_gold", python_callable=build_gold)
    q = PythonOperator(task_id="data_quality", python_callable=data_quality)
    r = PythonOperator(task_id="report", python_callable=report)
    s >> g >> q >> r
