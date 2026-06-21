"""
Composer for open-data-platform.

Takes the final selections dict and assembles:
  - generated/docker-compose.yml
  - generated/.env
  - generated/STACK.md

Each service module lives under services/<layer>/<stack>.py and exposes:
  - service_blocks(config: dict) -> dict   : one or more compose service dicts
  - env_vars(config: dict) -> dict         : env var name -> default value
  - depends_on(selections: dict) -> list   : service names this one waits for

Service names within the compose file follow the pattern: <stack_name>
e.g. minio, hive_metastore, trino, spark_master, airflow_webserver
"""

from __future__ import annotations

import base64
import importlib
import os
import secrets
import shutil
import yaml
from pathlib import Path
from typing import Any

OUTPUT_DIR = Path("generated")
SERVICES_ROOT = Path("services")

# Env keys whose value is a credential/secret. On first generation these get a
# random value; on later runs the existing value is kept (so they don't change
# under a running stack whose volumes were initialised with the old value).
SECRET_KEYS = {
    "POSTGRES_PASSWORD",
    "MINIO_ROOT_PASSWORD",
    "AIRFLOW_ADMIN_PASSWORD",
    "GRAFANA_ADMIN_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "REDASH_COOKIE_SECRET",
    "REDASH_SECRET_KEY",
}


def _generate_secret(key: str) -> str:
    # Airflow's Fernet key must be 32 url-safe base64 bytes.
    if key == "AIRFLOW_FERNET_KEY":
        return base64.urlsafe_b64encode(os.urandom(32)).decode()
    # token_urlsafe -> only [A-Za-z0-9_-], safe inside connection URLs and YAML.
    return secrets.token_urlsafe(18)


def _read_existing_env(path: Path) -> dict[str, str]:
    existing: dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                existing[k.strip()] = v.strip()
    return existing


# ---------------------------------------------------------------------------
# Layer -> module path mapping
# Maps each layer name and stack name to its Python module path
# ---------------------------------------------------------------------------

# Always-included base services (not tied to any selectable layer).
BASE_MODULES: list[str] = [
    "services.base.postgres",
]

MODULE_MAP: dict[str, dict[str, str]] = {
    "storage": {
        "minio":           "services.storage.minio",
        "hdfs":            "services.storage.hdfs",
    },
    "table_format": {
        # Table formats are handled at the processing/query layer config level
        # No standalone service for Iceberg/Delta/Hudi themselves
    },
    "metastore": {
        "hive_metastore":  "services.metastore.hive_metastore",
        "nessie":          "services.metastore.nessie",
    },
    "ingestion": {
        "kafka_debezium":  "services.ingestion.kafka_debezium",
        "kafka_only":      "services.ingestion.kafka_only",
        "nifi":            "services.ingestion.nifi",
    },
    "processing": {
        "spark":           "services.processing.spark",
        "flink":           "services.processing.flink",
        "spark_and_flink": "services.processing.spark_and_flink",
    },
    "query_engine": {
        "trino":           "services.query_engine.trino",
        "spark_sql":       None,  # Spark SQL reuses the spark service, no extra container
    },
    "orchestration": {
        "airflow":         "services.orchestration.airflow",
        "dagster":         "services.orchestration.dagster",
        "prefect":         "services.orchestration.prefect",
    },
    "quality": {
        "great_expectations": "services.quality.great_expectations",
        "soda":               "services.quality.soda",
    },
    "catalog": {
        "datahub":         "services.catalog.datahub",
        "openmetadata":    "services.catalog.openmetadata",
        "marquez":         "services.catalog.marquez",
    },
    "governance": {
        "ranger":          "services.governance.ranger",
        "trino_rules":     None,  # Config-only, no extra container
    },
    "visualization": {
        "superset":        "services.visualization.superset",
        "metabase":        "services.visualization.metabase",
        "redash":          "services.visualization.redash",
    },
    "observability": {
        "prometheus_grafana": "services.observability.prometheus_grafana",
        "netdata":            "services.observability.netdata",
    },
}


# ---------------------------------------------------------------------------
# Compose assembler
# ---------------------------------------------------------------------------

def _load_module(module_path: str):
    return importlib.import_module(module_path)


def assemble(selections: dict[str, str], stack_name: str = "custom") -> None:
    """
    Main entry point. Given selections {layer -> stack}, produces the
    generated/ output directory with compose file, env, and stack doc.
    """
    OUTPUT_DIR.mkdir(exist_ok=True)

    all_services: dict[str, Any] = {}
    all_env: dict[str, str] = {}
    all_volumes: set[str] = set()
    all_networks: set[str] = set()

    skipped: list[str] = []

    # Base infrastructure that is always present regardless of layer choices
    # (e.g. Postgres, which many layers depend on for metadata). Loaded before
    # the selected layers so their depends_on: postgresql always resolves.
    for module_path in BASE_MODULES:
        mod = _load_module(module_path)
        all_services.update(mod.service_blocks(selections))
        all_env.update(mod.env_vars(selections))
        all_volumes.update(getattr(mod, "named_volumes", lambda s: [])(selections))
        all_networks.update(getattr(mod, "named_networks", lambda s: [])(selections))

    for layer_name, stack_name_sel in selections.items():
        if stack_name_sel is None:
            continue

        layer_map = MODULE_MAP.get(layer_name, {})
        module_path = layer_map.get(stack_name_sel)

        if module_path is None:
            # Stack is config-only (e.g. spark_sql, trino_rules)
            skipped.append(f"{layer_name}/{stack_name_sel} — config-only, no container")
            continue

        try:
            mod = _load_module(module_path)
        except ModuleNotFoundError:
            # Service module not yet implemented
            skipped.append(f"{layer_name}/{stack_name_sel} — service module not yet implemented")
            continue

        blocks = mod.service_blocks(selections)
        env = mod.env_vars(selections)
        volumes = getattr(mod, "named_volumes", lambda s: [])(selections)
        networks = getattr(mod, "named_networks", lambda s: [])(selections)

        all_services.update(blocks)
        all_env.update(env)
        all_volumes.update(volumes)
        all_networks.update(networks)

    # Copy the configs/ directory into generated/ so that bind mounts in
    # docker-compose.yml resolve correctly when run as:
    #   docker compose -f generated/docker-compose.yml up
    # Docker resolves relative paths from the directory containing the compose
    # file, so configs must live under generated/.
    #
    # This must run AFTER the service loop above: each module's
    # service_blocks() writes its config files into configs/ as a side
    # effect, so copying earlier would ship a stale (one-generation-old)
    # configs tree to generated/.
    src_configs = Path("configs")
    dst_configs = OUTPUT_DIR / "configs"
    if src_configs.exists():
        if dst_configs.exists():
            shutil.rmtree(dst_configs)
        shutil.copytree(src_configs, dst_configs)

    # Copy the dags/ and jobs/ directories too. Airflow bind-mounts ./dags
    # and the Spark services bind-mount ./jobs (both resolved under
    # generated/), so without this the seed DAGs and Spark job scripts never
    # reach the containers and those generated/ dirs would be empty.
    for sub in ("dags", "jobs"):
        src = Path(sub)
        dst = OUTPUT_DIR / sub
        if src.exists():
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)

    compose = _build_compose(all_services, all_volumes, all_networks)
    _write_compose(compose)
    _write_env(all_env)
    _write_stack_md(selections, skipped, stack_name)

    print()
    print("  Generated files:")
    print("    generated/docker-compose.yml")
    print("    generated/.env")
    print("    generated/STACK.md")

    if skipped:
        print()
        print("  Notes:")
        for s in skipped:
            print(f"    - {s}")


def _build_compose(
    services: dict[str, Any],
    volumes: set[str],
    networks: set[str],
) -> dict:
    compose: dict[str, Any] = {
        "name": "open-data-platform",
        "services": services,
    }
    if volumes:
        compose["volumes"] = {v: None for v in sorted(volumes)}
    if networks:
        compose["networks"] = {n: None for n in sorted(networks)}
    return compose


def _write_compose(compose: dict) -> None:
    path = OUTPUT_DIR / "docker-compose.yml"
    with open(path, "w") as f:
        yaml.dump(
            compose,
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )


def _write_env(env: dict[str, str]) -> None:
    path = OUTPUT_DIR / ".env"

    # Persist across runs: keep any value already in generated/.env, generate a
    # random one for first-seen secrets, and fall back to the module default
    # otherwise. This means passwords are created once and stay stable for the
    # life of the stack's volumes (changing them would lock you out of an
    # already-initialised Postgres/MinIO).
    existing = _read_existing_env(path)
    resolved: dict[str, str] = {}
    for key, default in env.items():
        if key in existing:
            resolved[key] = existing[key]
        elif key in SECRET_KEYS:
            resolved[key] = _generate_secret(key)
        else:
            resolved[key] = default

    # MinIO credentials and the AWS_* credentials that S3A uses must match —
    # keep them in lockstep so a randomised MinIO password still authenticates.
    if "MINIO_ROOT_PASSWORD" in resolved:
        resolved["AWS_SECRET_ACCESS_KEY"] = resolved["MINIO_ROOT_PASSWORD"]
    if "MINIO_ROOT_USER" in resolved:
        resolved["AWS_ACCESS_KEY_ID"] = resolved["MINIO_ROOT_USER"]

    lines = [
        "# Auto-generated by open-data-platform setup.py",
        "# Ports/tunables are regenerated each run; secrets (passwords, keys)",
        "# are generated once and then preserved across runs. Delete this file",
        "# to rotate them (you must also wipe volumes: docker compose down -v).",
        "",
    ]

    # Group by prefix (e.g. SPARK_, TRINO_, MINIO_)
    groups: dict[str, list[str]] = {}
    for key, val in sorted(resolved.items()):
        prefix = key.split("_")[0]
        groups.setdefault(prefix, []).append(f"{key}={val}")

    for prefix, group_lines in sorted(groups.items()):
        lines.append(f"# --- {prefix} ---")
        lines.extend(group_lines)
        lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))


def _write_stack_md(
    selections: dict[str, str],
    skipped: list[str],
    preset_name: str,
) -> None:
    path = OUTPUT_DIR / "STACK.md"
    lines = [
        "# Your Data Platform Stack",
        "",
        f"Preset: **{preset_name}**",
        "",
        "## Selected Components",
        "",
        "| Layer | Stack |",
        "|---|---|",
    ]
    for layer, stack in selections.items():
        display = stack if stack else "skipped"
        lines.append(f"| {layer} | {display} |")

    lines += [
        "",
        "## Getting Started",
        "",
        "```bash",
        "# From the generated/ directory:",
        "docker compose up -d",
        "",
        "# Scale Spark workers:",
        "docker compose up -d --scale spark-worker=3",
        "",
        "# Tear down:",
        "docker compose down -v",
        "```",
        "",
        "## Endpoints",
        "",
        "Endpoints are printed after `docker compose up` completes.",
        "See the .env file for port configuration.",
        "",
    ]

    if skipped:
        lines += [
            "## Notes",
            "",
        ]
        for s in skipped:
            lines.append(f"- {s}")
        lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))
