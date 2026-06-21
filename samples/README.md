# Sample scenarios

Five reference stacks that show the platform working across all three table
formats and a spread of the alternative components. Each is a complete
selection (one component per layer) defined in [`scenarios.py`](scenarios.py).

| Scenario | Table format | Highlights |
|---|---|---|
| `iceberg-lakehouse` | Iceberg | Kafka/Debezium CDC, Spark, Trino, Airflow, Marquez lineage, Prometheus/Grafana |
| `delta-lakehouse` | Delta | Spark, Trino, Airflow + **Soda** data-quality checks |
| `hudi-lakehouse` | Hudi | Minimal: Spark + Trino + Hive Metastore |
| `alternatives-showcase` | Iceberg | **NiFi** + **Flink** + **Dagster** + **Metabase** + **Netdata** |
| `lean-delta-alt` | Delta | **Prefect** + **Metabase**, lightweight |

## Run a scenario

From the **repo root** (works the same on Windows, Linux, macOS):

```bash
python samples/run_sample.py --list                 # list scenarios
python samples/run_sample.py hudi-lakehouse         # generate + start + show status
python samples/run_sample.py hudi-lakehouse --down  # stop and remove volumes
```

`run_sample.py` performs, in order:
1. `composer.assemble(...)` → writes `generated/docker-compose.yml`
2. resets any previous platform stack (`down -v`), then `docker compose up -d`
3. polls until services are healthy
4. prints the service endpoints

> Run **one scenario at a time** — they share the `open-data-platform` Compose
> project (and so the same container names/volumes), and the runner resets it on
> each start. Give Docker ~8–10 GiB; `iceberg-lakehouse` and
> `alternatives-showcase` are the heaviest.

## Validate every combination

`validate_all.py` swaps each implemented component into a baseline stack and
runs `docker compose config` on it, then does the same for all five scenarios.
It's a fast structural check (no containers started) and doubles as a CI smoke
test:

```bash
python samples/validate_all.py     # exit 0 if all combinations generate valid Compose
```

## What's been tested

- **All implemented per-layer options** + all five scenarios pass
  `validate_all.py` (generate → valid Compose).
- The three **table-format core stacks** (`iceberg-lakehouse`, `delta-lakehouse`,
  `hudi-lakehouse`) and `lean-delta-alt` were **booted and smoke-tested live**
  (services healthy; Trino shows the expected catalog).

## Add your own scenario

**Fork the repo** and adapt it to your needs:

1. Add an entry to `SCENARIOS` in [`scenarios.py`](scenarios.py) — one component
   per layer (see the keys in `composer.MODULE_MAP`).
2. Validate it: `python samples/validate_all.py`.
3. Boot it: `python samples/run_sample.py <your-name>`.

Cross-layer rules to keep in mind (the compatibility matrix enforces these in
the interactive picker): `quality` (`soda`, `great_expectations`) runs as
Airflow DAGs, so it needs `orchestration=airflow`; `nessie` only works with
`iceberg`; `spark_sql` reuses the Spark service rather than adding a container.
