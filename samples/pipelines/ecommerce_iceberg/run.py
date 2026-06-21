"""
Run the e-commerce Iceberg CDC lakehouse demo end to end.

    python samples/pipelines/ecommerce_iceberg/run.py          # build + run everything
    python samples/pipelines/ecommerce_iceberg/run.py --down   # tear it all down

Flow:
  1. generate the platform (Iceberg + HMS + Kafka/Debezium + Spark + Trino +
     Airflow + Marquez + Metabase + Prometheus/Grafana) and overlay the
     always-on Spark CDC consumer
  2. seed an `ecommerce` source DB and start Debezium CDC
  3. CDC streams into bronze Iceberg; Airflow runs the Trino bronze->silver->gold
     transforms; Marquez captures lineage
  4. auto-provision a Metabase dashboard over the gold marts
  5. (optional) push extra orders to show CDC velocity

Run from the repo root. One combo at a time (shares the platform compose
project). First run downloads ~280 MB of Spark jars (cached afterwards).
"""

import os
import shutil
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, REPO)
os.chdir(REPO)

import composer  # noqa: E402
from samples.pipelines.ecommerce_iceberg import seed_source, provision_metabase  # noqa: E402

HERE = "samples/pipelines/ecommerce_iceberg"
COMPOSE = ["docker", "compose", "-f", "generated/docker-compose.yml",
           "-f", f"{HERE}/compose.override.yml"]
SELECTION = {
    "storage": "minio", "table_format": "iceberg", "metastore": "hive_metastore",
    "ingestion": "kafka_debezium", "processing": "spark", "query_engine": "trino",
    "orchestration": "airflow", "catalog": "marquez",
    "observability": "prometheus_grafana", "visualization": "metabase",
}
DAG_ID = "ecommerce_iceberg_pipeline"


def _wait_healthy(svc: str, timeout: int = 240) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = subprocess.run(["docker", "inspect", "-f", "{{.State.Health.Status}}", svc],
                             capture_output=True, text=True).stdout.strip()
        if out == "healthy":
            return
        time.sleep(8)
    raise SystemExit(f"{svc} did not become healthy")


def _trino_count(sql: str) -> int:
    out = subprocess.run(["docker", "exec", "trino", "trino", "--execute", sql],
                         capture_output=True, text=True).stdout.strip().strip('"')
    try:
        return int(out)
    except ValueError:
        return -1


def up() -> int:
    print("[1/6] Generating platform + CDC overlay...")
    composer.assemble(SELECTION, stack_name="ecommerce-iceberg")
    shutil.copy(f"{HERE}/jobs/cdc_consumer.py", "generated/jobs/cdc_consumer.py")
    os.makedirs(".ivy_cache", exist_ok=True)

    print("[2/6] Starting the stack (down -v first for a clean slate)...")
    subprocess.run([*COMPOSE, "down", "-v"], check=False)
    if subprocess.run([*COMPOSE, "up", "-d"]).returncode != 0:
        return 1
    for svc in ("kafka-connect", "hive-metastore", "trino", "airflow-scheduler"):
        _wait_healthy(svc)

    print("[3/6] Seeding ecommerce source + starting Debezium CDC...")
    seed_source.seed()

    print("[4/6] Waiting for CDC to land in bronze Iceberg...")
    deadline = time.time() + 600  # first run includes the jar download
    while time.time() < deadline:
        n = _trino_count("SELECT count(*) FROM iceberg.bronze.cdc_events")
        if n > 0:
            print(f"      bronze.cdc_events: {n} rows")
            break
        time.sleep(15)
    else:
        return 1

    print("[5/6] Running the Airflow transform pipeline (bronze->silver->gold)...")
    # Restart the scheduler/worker first: the CeleryExecutor can wedge ("task
    # queued but never dispatched") after a long, busy startup; a restart
    # guarantees a clean executor before we trigger.
    subprocess.run(["docker", "restart", "airflow-scheduler",
                    "open-data-platform-airflow-worker-1"], capture_output=True)
    _wait_healthy("airflow-scheduler", timeout=120)
    time.sleep(10)
    subprocess.run(["docker", "exec", "airflow-scheduler", "airflow", "dags",
                    "unpause", DAG_ID], capture_output=True)
    subprocess.run(["docker", "exec", "airflow-scheduler", "airflow", "dags",
                    "trigger", DAG_ID], capture_output=True)
    deadline = time.time() + 300
    while time.time() < deadline:
        st = subprocess.run(["docker", "exec", "airflow-scheduler", "airflow", "dags",
                             "list-runs", "-d", DAG_ID, "-o", "plain"],
                            capture_output=True, text=True).stdout
        if "success" in st:
            break
        if "failed" in st:
            print("      pipeline failed — see Airflow logs")
            return 1
        time.sleep(15)
    for t in ("daily_revenue", "top_products", "customer_ltv"):
        print(f"      gold.{t}: {_trino_count(f'SELECT count(*) FROM iceberg.gold.{t}')} rows")

    print("[6/6] Provisioning Metabase dashboard...")
    provision_metabase.main()

    print("\nDone. Explore:")
    print("  Metabase   http://localhost:3002   (E-commerce Overview dashboard)")
    print("  Trino      http://localhost:8080")
    print("  Airflow    http://localhost:8082   (admin / see generated/.env)")
    print("  Marquez    http://localhost:3001   (lineage)")
    print("  Grafana    http://localhost:3000")
    print(f"\nShow CDC velocity:  python {HERE}/seed_source.py bump")
    print(f"  then re-run the pipeline:  docker exec airflow-scheduler airflow dags trigger {DAG_ID}")
    return 0


def down() -> int:
    composer.assemble(SELECTION, stack_name="ecommerce-iceberg")
    return subprocess.run([*COMPOSE, "down", "-v"]).returncode


if __name__ == "__main__":
    sys.exit(down() if "--down" in sys.argv else up())
