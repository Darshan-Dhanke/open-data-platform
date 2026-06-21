"""
Seed the e-commerce source database and start CDC.

Steps:
  1. generate the e-commerce CSVs (samples/datasets/ecommerce.py)
  2. create an `ecommerce` database in the platform Postgres, load the CSVs,
     and set REPLICA IDENTITY FULL (so Debezium captures updates/deletes)
  3. register a Debezium connector that streams ecommerce.public.* to Kafka
     topics prefixed `ecom.`

Source columns are typed as bigint / double precision / text (timestamps stored
as ISO strings) so Debezium emits plain JSON scalars that the silver SQL can
cast cleanly — avoiding Debezium's epoch-micros temporal encoding.

Run from the repo root after the platform is up:  python <this> seed
Insert extra orders later (CDC velocity demo):     python <this> bump
"""

import json
import os
import subprocess
import sys
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, REPO)
os.chdir(REPO)
from samples.datasets import ecommerce  # noqa: E402

DATA_DIR = ".sample_data/ecommerce"
PG = "postgresql"
CONNECT = "http://localhost:8083"

DDL = """
CREATE TABLE IF NOT EXISTS customers(customer_id BIGINT PRIMARY KEY, name TEXT, email TEXT, country TEXT, segment TEXT, signup_date TEXT);
CREATE TABLE IF NOT EXISTS products(product_id BIGINT PRIMARY KEY, name TEXT, category TEXT, unit_price DOUBLE PRECISION, unit_cost DOUBLE PRECISION);
CREATE TABLE IF NOT EXISTS orders(order_id BIGINT PRIMARY KEY, customer_id BIGINT, order_ts TEXT, channel TEXT, status TEXT);
CREATE TABLE IF NOT EXISTS order_items(item_id BIGINT PRIMARY KEY, order_id BIGINT, product_id BIGINT, quantity BIGINT, unit_price DOUBLE PRECISION);
CREATE TABLE IF NOT EXISTS payments(order_id BIGINT PRIMARY KEY, method TEXT, amount DOUBLE PRECISION, status TEXT, paid_ts TEXT);
CREATE TABLE IF NOT EXISTS events(event_id BIGINT PRIMARY KEY, customer_id BIGINT, event_type TEXT, event_ts TEXT, payload TEXT);
"""

TABLES = ["customers", "products", "orders", "order_items", "payments", "events"]


def _env(key: str, default: str) -> str:
    path = os.path.join("generated", ".env")
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            if line.strip().startswith(f"{key}="):
                return line.split("=", 1)[1].strip()
    return default


def _psql(db: str, sql: str, pw: str, user: str) -> None:
    subprocess.run(
        ["docker", "exec", "-e", f"PGPASSWORD={pw}", PG,
         "psql", "-v", "ON_ERROR_STOP=1", "-U", user, "-d", db, "-c", sql],
        check=True,
    )


def seed() -> None:
    user = _env("POSTGRES_USER", "hive")
    pw = _env("POSTGRES_PASSWORD", "hive")

    print("[seed] generating CSVs...")
    ecommerce.generate(DATA_DIR)

    print("[seed] creating ecommerce database...")
    # CREATE DATABASE can't run if it exists; ignore failure.
    subprocess.run(["docker", "exec", "-e", f"PGPASSWORD={pw}", PG, "psql", "-U", user,
                    "-d", "metastore", "-c", "CREATE DATABASE ecommerce"],
                   capture_output=True)

    print("[seed] copying CSVs into the container...")
    subprocess.run(["docker", "exec", PG, "rm", "-rf", "/tmp/ecom"], capture_output=True)
    subprocess.run(["docker", "exec", PG, "mkdir", "-p", "/tmp/ecom"], check=True)
    for t in TABLES:
        subprocess.run(["docker", "cp", f"{DATA_DIR}/{t}.csv", f"{PG}:/tmp/ecom/{t}.csv"], check=True)

    print("[seed] creating tables + loading data...")
    _psql("ecommerce", DDL, pw, user)
    for t in TABLES:
        _psql("ecommerce", f"TRUNCATE {t};", pw, user)
        _psql("ecommerce",
              f"COPY {t} FROM '/tmp/ecom/{t}.csv' WITH (FORMAT csv, HEADER true)",
              pw, user)
        _psql("ecommerce", f"ALTER TABLE {t} REPLICA IDENTITY FULL;", pw, user)

    print("[seed] registering Debezium connector...")
    _register_connector(user, pw)
    print("[seed] done.")


def _register_connector(user: str, pw: str) -> None:
    cfg = {
        "name": "ecommerce-cdc",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "postgresql", "database.port": "5432",
            "database.user": user, "database.password": pw,
            "database.dbname": "ecommerce", "topic.prefix": "ecom",
            "schema.include.list": "public",
            "table.include.list": ",".join(f"public.{t}" for t in TABLES),
            "plugin.name": "pgoutput", "slot.name": "ecom_slot",
            "publication.name": "ecom_pub",
            "decimal.handling.mode": "double", "snapshot.mode": "initial",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
        },
    }
    # delete any prior instance, then create
    try:
        req = urllib.request.Request(f"{CONNECT}/connectors/ecommerce-cdc", method="DELETE")
        urllib.request.urlopen(req, timeout=15)
    except Exception:
        pass
    req = urllib.request.Request(
        f"{CONNECT}/connectors", data=json.dumps(cfg).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        print("   connector HTTP", r.status)


def bump(n: int = 500) -> None:
    """CDC velocity demo: insert N new orders (+items+payments) into the source."""
    import random
    user, pw = _env("POSTGRES_USER", "hive"), _env("POSTGRES_PASSWORD", "hive")
    rows = []
    base = f"(SELECT COALESCE(MAX(order_id),0) FROM orders)"
    sql = f"""
    INSERT INTO orders(order_id, customer_id, order_ts, channel, status)
    SELECT {base}+g, (random()*1999)::int+1,
           to_char(now(),'YYYY-MM-DD HH24:MI:SS'),
           (ARRAY['web','mobile','store'])[(random()*2)::int+1], 'placed'
    FROM generate_series(1,{n}) g;
    """
    _psql("ecommerce", sql, pw, user)
    print(f"[bump] inserted {n} new orders into source -> CDC will stream them.")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "seed"
    (bump if cmd == "bump" else seed)()
