# Pipeline 1 — E-commerce CDC Lakehouse (Iceberg)

A complete, real data platform assembled from the project's components, running
an end-to-end pipeline on a realistic e-commerce dataset.

```
Postgres (source)  ──Debezium CDC──▶  Kafka  ──Spark Structured Streaming──▶  bronze (Iceberg)
                                                                                   │
                                                        Airflow ── Trino SQL ──────┤
                                                                                   ▼
                                                                  silver  ──▶  gold marts
                                                                                   │
                                          Metabase dashboard ◀── Trino ───────────┤
                                          Marquez lineage  ◀── Airflow OpenLineage ┘
                                          Prometheus/Grafana ◀── platform metrics
```

| Layer | Component |
|---|---|
| Storage | MinIO |
| Table format | **Apache Iceberg** |
| Metastore | Hive Metastore |
| Ingestion | Kafka + Debezium (**CDC**) |
| Processing | Spark (Structured Streaming consumer) |
| Query engine | Trino |
| Orchestration | **Airflow** |
| Catalog/lineage | Marquez |
| Visualization | Metabase |
| Observability | Prometheus + Grafana |

## What it does

1. **Generate + seed** ~50k rows of e-commerce data (customers, products,
   orders, order_items, payments, events) into a `ecommerce` source Postgres DB.
2. **CDC**: Debezium streams every change to Kafka; an always-on Spark job lands
   them in **bronze** Iceberg (`iceberg.bronze.cdc_events`), append-only.
3. **Transform** (Airflow → Trino SQL): bronze → **silver** (typed,
   latest-state-per-key) → **gold** marts (`daily_revenue`, `top_products` with
   margin, `customer_ltv`), then a **data-quality gate** (row counts, no nulls,
   non-negative revenue, LTV covers all customers) that fails the run on a
   violation.
4. **Lineage**: the DAG emits OpenLineage events so Marquez shows the full
   **table-level graph** `bronze.cdc_events → silver.* → gold.*`.
5. **Visualize**: an auto-provisioned **"E-commerce Overview"** Metabase
   dashboard — KPI tiles (total revenue, orders, customers, AOV), a daily
   revenue trend, and revenue-by-channel / top-products / LTV-by-segment.
6. **Monitor**: a provisioned Grafana **"Platform Overview"** dashboard (Trino
   query activity + per-container CPU/memory).
7. **CDC velocity**: push new orders into the source and watch them flow through
   to bronze (and to gold on the next pipeline run).

## Run it

From the repo root (one combo at a time; needs ~8–10 GiB free for Docker):

```bash
python samples/pipelines/ecommerce_iceberg/run.py          # build + run everything
python samples/pipelines/ecommerce_iceberg/run.py --down   # tear down + clean up
```

> First run downloads ~280 MB of Spark/Kafka/Iceberg jars into `.ivy_cache/`
> (git-ignored); later runs reuse it. The script prints all the URLs at the end.

### Then explore
- **Metabase** http://localhost:3002 — *E-commerce Overview* dashboard (login printed by the run)
- **Trino** http://localhost:8080 — query `iceberg.gold.*`
- **Airflow** http://localhost:8082 — `ecommerce_iceberg_pipeline` DAG, incl. the `data_quality` gate (admin pw in `generated/.env`)
- **Marquez** http://localhost:3001 — search dataset `gold.daily_revenue` to see the `bronze → silver → gold` lineage graph
- **Grafana** http://localhost:3000 — the *Platform Overview* dashboard

### Show CDC velocity
```bash
python samples/pipelines/ecommerce_iceberg/seed_source.py bump   # +500 orders into source
docker exec airflow-scheduler airflow dags trigger ecommerce_iceberg_pipeline
```

## Files
```
run.py                 orchestrates the whole demo (build → seed → CDC → transform → BI)
seed_source.py         generates + loads the source DB, registers Debezium ('bump' adds orders)
provision_metabase.py  creates the Trino datasource + dashboard via the Metabase API
jobs/cdc_consumer.py   Spark Structured Streaming: Kafka/Debezium → bronze Iceberg
compose.override.yml   adds the always-on spark-cdc-consumer service
../../datasets/ecommerce.py   the dataset generator
../../../dags/ecommerce_iceberg_pipeline.py + ecommerce_iceberg_sql/   the Airflow DAG + SQL
```
