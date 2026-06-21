# open-data-platform

A composable, open-source data platform you assemble from interchangeable parts.
Pick one component per layer (or start from a preset) and the tool generates a
ready-to-run Docker Compose stack. Incompatible combinations are blocked with a
reason.

## Two ways to use it

| | Command | What you get |
|---|---|---|
| **A. Build a platform** | `python setup.py` | Choose a preset or custom stack → it generates and launches the services. **No sample data** — bring your own. |
| **B. Run a full working example** | `python samples/pipelines/ecommerce_iceberg/run.py` | A complete, opinionated stack **plus** real data flowing end to end: CDC → lakehouse → transforms → BI dashboard → lineage → monitoring. One command. |

Option B is self-contained — it assembles its own stack internally, so you do
**not** need to run `setup.py` first.

## Prerequisites

- **Docker** + **Compose v2** (`docker compose`, not `docker-compose`)
- **Python 3.10+** and **git**
- ~8–10 GiB free for Docker (the example stack is ~18 containers)
- **x86-64 (amd64)** host — the published images are amd64 only (ARM would need multi-arch images)

### Install — Windows
```powershell
# Install Docker Desktop (with Compose v2) and Python 3.10+, then:
git clone https://github.com/Darshan-Dhanke/open-data-platform
cd open-data-platform
pip install -r requirements.txt
```

### Install — Linux
```bash
# Docker Engine + the compose plugin, Python 3.10+, git
sudo usermod -aG docker $USER     # run docker without sudo, then re-login
git clone https://github.com/Darshan-Dhanke/open-data-platform
cd open-data-platform
pip install -r requirements.txt
```
> Everything runs through `docker compose`, so the steps are identical on
> Windows, Linux and macOS. On native Linux it's actually smoother — no Docker
> Desktop memory cap, and cAdvisor/Netdata see all containers.

---

## Option A — build a platform (`setup.py`)

```bash
python setup.py
```
1. Pick a preset (**A** Iceberg or **B** Delta) or build a **custom** stack.
2. Confirm — it writes everything to `generated/` and offers to launch.
3. The clickable service endpoints (and generated passwords) print at the end.

```
generated/
  docker-compose.yml   # the assembled stack
  .env                 # ports + generated credentials (created once, then preserved)
  configs/  dags/  jobs/
  STACK.md             # summary of your selection
```

Control the stack yourself:
```bash
docker compose -f generated/docker-compose.yml up -d        # start
docker compose -f generated/docker-compose.yml ps           # health
docker compose -f generated/docker-compose.yml up -d --scale spark-worker=3
docker compose -f generated/docker-compose.yml down -v      # stop + wipe volumes
```
HMS, Airflow and Trino take 60–90s to go healthy on first boot.

---

## Option B — run the end-to-end example (e-commerce CDC lakehouse)

```bash
python samples/pipelines/ecommerce_iceberg/run.py          # build + run everything
python samples/pipelines/ecommerce_iceberg/run.py --down   # tear down + clean up
```

It seeds a realistic e-commerce dataset into a source Postgres, then:
**Debezium CDC → Kafka → Spark Structured Streaming → bronze Iceberg → Airflow
runs Trino transforms → silver + gold marts → a data-quality gate**, and
auto-provisions a **Metabase dashboard**, **Marquez lineage**, and a **Grafana**
dashboard. A `bump` step shows CDC velocity (new orders flow straight through).

When it finishes, explore:

| Service | URL | Notes |
|---|---|---|
| **Metabase** | http://localhost:3002 | *E-commerce Overview* dashboard — KPIs, revenue trend, breakdowns. Login `admin@example.com` / `MetabasePlatform1!` |
| **Marquez** | http://localhost:3001 | search dataset `gold.daily_revenue` → `bronze → silver → gold` lineage graph |
| **Airflow** | http://localhost:8082 | `ecommerce_iceberg_pipeline` DAG (incl. `data_quality` gate) |
| **Grafana** | http://localhost:3000 | *Platform Overview* dashboard |
| **Trino** | http://localhost:8080 | query `iceberg.gold.*` |

> First run downloads ~280 MB of Spark jars into `.ivy_cache/` (cached after).
> Run one example at a time (they share the `open-data-platform` Compose project).
> More combos (NYC taxi · Hudi · Dagster, IoT · Delta · Prefect, …) are coming.
> See [the pipeline README](samples/pipelines/ecommerce_iceberg/README.md).

---

## Presets

| Preset | Stack |
|---|---|
| **A — Iceberg Lakehouse** | MinIO · Iceberg · Hive Metastore · Kafka+Debezium · Spark · Trino · Airflow · Marquez · Prometheus/Grafana |
| **B — Delta Lakehouse** | MinIO · Delta · Hive Metastore · Kafka+Debezium · Spark · Trino · Airflow · Marquez · Prometheus/Grafana |

## Components by layer

Pick one per layer. ✅ = implemented & validated.

| Layer | Options |
|---|---|
| Storage | `minio` ✅ · `hdfs` ✅ |
| Table format | `iceberg` ✅ · `delta` ✅ · `hudi` ✅ |
| Metastore | `hive_metastore` ✅ · `nessie` ✅ |
| Ingestion | `kafka_debezium` ✅ · `kafka_only` ✅ · `nifi` ✅ |
| Processing | `spark` ✅ · `flink` ✅ |
| Query engine | `trino` ✅ · `spark_sql` *(reuses Spark)* |
| Orchestration | `airflow` ✅ · `dagster` ✅ · `prefect` ✅ |
| Quality | `great_expectations` ✅ · `soda` ✅ *(need orchestration=airflow)* |
| Catalog / lineage | `marquez` ✅ · `datahub`, `openmetadata` *(heavy, not bundled)* |
| Governance | `trino_rules` *(config-level)* · `ranger` *(heavy, not bundled)* |
| Visualization | `metabase` ✅ · `redash` ✅ · `superset` *(not bundled)* |
| Observability | `prometheus_grafana` ✅ · `netdata` ✅ |

## More sample stacks

Beyond the full pipeline demo, [`samples/`](samples/) has five lighter reference
stacks (all three table formats + alternative components) and a validator:
```bash
python samples/run_sample.py --list             # boot a scenario
python samples/validate_all.py                  # generate + compose-validate every combo
```

## Notes

- **Credentials are generated** randomly on first run and preserved in
  `generated/.env` (so they're stable across restarts). The endpoint summary and
  `generated/.env` show them.
- Images pull from Docker Hub (`darshandhanke07/odp-*`) on first run.
- `generated/`, `.ivy_cache/` and `.sample_data/` are git-ignored build artifacts.
- Heavy options (DataHub, OpenMetadata, Ranger, Superset) are selectable but not
  bundled — they need significantly more RAM.

## Project layout

```
setup.py                 interactive stack builder
composer.py              assembles selections -> generated/ Compose stack
compatibility/           which component combinations are allowed
services/                one module per component
samples/pipelines/       full end-to-end example pipelines (Option B)
samples/                 lighter reference stacks + validators
```

## License

See [LICENSE](LICENSE).
