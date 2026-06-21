# open-data-platform

A composable, open-source data platform you assemble from interchangeable parts.
Pick one component per layer (or start from a preset), and the tool generates a
ready-to-run Docker Compose stack. Incompatible combinations are blocked with a
reason.

## How it works

`setup.py` is an interactive CLI. You choose a stack, it writes everything into
`generated/`, and (optionally) starts it:

```
generated/
  docker-compose.yml   # the assembled stack
  .env                 # ports, credentials, tunables
  configs/             # per-service config (Trino, Spark, Hive, etc.)
  dags/  jobs/         # sample Airflow DAGs and Spark jobs
  STACK.md             # summary of your selection
```

## Prerequisites

- **Docker Desktop** with **Compose v2** (`docker compose`, not `docker-compose`)
- **Python 3.10+**
- Enough RAM for your selection. The default presets run comfortably in ~8–10 GiB
  allocated to Docker; adding several heavy services needs more.

## Quick start

```bash
pip install pyyaml rich
python setup.py
```

Then:
1. Pick a preset (**A** Iceberg or **B** Delta) or build a **custom** stack.
2. Confirm — it generates the files and offers to launch.
3. When it finishes, the clickable service endpoints are printed.

## Running it manually

The generator only writes files unless you let it launch. To control the stack
yourself:

```bash
# from the repo root
docker compose -f generated/docker-compose.yml up -d        # start
docker compose -f generated/docker-compose.yml ps           # health
docker compose -f generated/docker-compose.yml logs -f svc  # tail a service
docker compose -f generated/docker-compose.yml up -d --scale spark-worker=3
docker compose -f generated/docker-compose.yml down         # stop (keep data)
docker compose -f generated/docker-compose.yml down -v      # stop + wipe volumes
```

Services start in dependency order; HMS, Airflow and Trino take 60–90s to go
healthy on first boot. Re-running `python setup.py` overwrites `generated/`.

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
| Table format | `iceberg` ✅ · `delta` ✅ · `hudi` ✅ *(config-level)* |
| Metastore | `hive_metastore` ✅ · `nessie` ✅ |
| Ingestion | `kafka_debezium` ✅ · `kafka_only` ✅ · `nifi` ✅ |
| Processing | `spark` ✅ · `flink` ✅ |
| Query engine | `trino` ✅ · `spark_sql` *(reuses Spark)* |
| Orchestration | `airflow` ✅ · `dagster` ✅ · `prefect` ✅ |
| Quality | `great_expectations` ✅ · `soda` ✅ |
| Catalog / lineage | `marquez` ✅ · `datahub`, `openmetadata` *(heavy, not bundled)* |
| Governance | `trino_rules` *(config-level)* · `ranger` *(heavy, not bundled)* |
| Visualization | `metabase` ✅ · `redash` ✅ · `superset` *(not bundled)* |
| Observability | `prometheus_grafana` ✅ · `netdata` ✅ |

## Default endpoints

Printed after launch; ports come from `generated/.env`. Common ones:

| Service | URL | Login |
|---|---|---|
| Trino | http://localhost:8080 | — |
| Spark master | http://localhost:8081 | — |
| Airflow | http://localhost:8082 | admin / admin |
| MinIO console | http://localhost:9001 | admin / password123 |
| Kafka UI | http://localhost:9094 | — |
| Marquez (lineage) | http://localhost:3001 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |

(Alternatives expose their own ports, e.g. Metabase 3002, Dagster 3003, Prefect
4200, NiFi 8086, HDFS NameNode 9870, Nessie 19120, Netdata 19999.)

## Project layout

```
setup.py            # interactive CLI entry point
composer.py         # assembles selections -> generated/ Compose stack
compatibility/      # which component combinations are allowed
services/           # one module per component (service definition + config)
configs/ dags/ jobs/ # source config and samples copied into generated/
```

## Notes

- Images are pulled from Docker Hub (`darshandhanke07/odp-*` plus upstream
  official images) on first run.
- `generated/` is git-ignored — it's build output; regenerate it any time.
- Heavy catalog/governance options (DataHub, OpenMetadata, Ranger, Superset) are
  selectable in the matrix but not bundled, as they need significantly more RAM.

## License

See [LICENSE](LICENSE).
