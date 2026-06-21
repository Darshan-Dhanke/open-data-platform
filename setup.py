"""
open-data-platform setup.py

Entry point. Run with:
  python setup.py

Prerequisites:
  - Docker Desktop installed and running
  - Docker Compose v2 (docker compose, not docker-compose)
  - Python 3.9+
  - pip install pyyaml rich
"""

from __future__ import annotations

import subprocess
import sys
import os

# Force UTF-8 output on Windows — prevents encoding artifacts in the terminal
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich import box
except ImportError:
    print("Missing dependency: run  pip install pyyaml rich  then try again.")
    sys.exit(1)

try:
    import yaml
except ImportError:
    print("Missing dependency: run  pip install pyyaml rich  then try again.")
    sys.exit(1)

from compatibility import load_matrix, evaluate_layer, get_layer_order
from composer import assemble

console = Console()


# ---------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------

PRESET_ICEBERG = {
    "name": "Iceberg Lakehouse",
    "description": "MinIO + Iceberg + Hive Metastore + Kafka + Spark + Trino + Airflow + DataHub + Superset",
    "selections": {
        "storage":       "minio",
        "table_format":  "iceberg",
        "metastore":     "hive_metastore",
        "ingestion":     "kafka_debezium",
        "processing":    "spark",
        "query_engine":  "trino",
        "orchestration": "airflow",
        "quality":       "great_expectations",
        "catalog":       "datahub",
        "governance":    "trino_rules",
        "visualization": "superset",
        "observability": "prometheus_grafana",
    },
}

PRESET_DELTA = {
    "name": "Delta Lakehouse",
    "description": "MinIO + Delta Lake + Hive Metastore + Kafka + Spark + Trino + Dagster + OpenMetadata + Metabase",
    "selections": {
        "storage":       "minio",
        "table_format":  "delta",
        "metastore":     "hive_metastore",
        "ingestion":     "kafka_debezium",
        "processing":    "spark",
        "query_engine":  "trino",
        "orchestration": "dagster",
        "quality":       "great_expectations",
        "catalog":       "openmetadata",
        "governance":    "trino_rules",
        "visualization": "metabase",
        "observability": "prometheus_grafana",
    },
}

PRESETS = [PRESET_ICEBERG, PRESET_DELTA]


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

BANNER = """
  ██████╗ ██████╗ ███████╗███╗   ██╗    ██████╗  █████╗ ████████╗ █████╗
 ██╔═══██╗██╔══██╗██╔════╝████╗  ██║    ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗
 ██║   ██║██████╔╝█████╗  ██╔██╗ ██║    ██║  ██║███████║   ██║   ███████║
 ██║   ██║██╔═══╝ ██╔══╝  ██║╚██╗██║    ██║  ██║██╔══██║   ██║   ██╔══██║
 ╚██████╔╝██║     ███████╗██║ ╚████║    ██████╔╝██║  ██║   ██║   ██║  ██║
  ╚═════╝ ╚═╝     ╚══════╝╚═╝  ╚═══╝    ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝

 ██████╗ ██╗      █████╗ ████████╗███████╗ ██████╗ ██████╗ ███╗   ███╗
 ██╔══██╗██║     ██╔══██╗╚══██╔══╝██╔════╝██╔═══██╗██╔══██╗████╗ ████║
 ██████╔╝██║     ███████║   ██║   █████╗  ██║   ██║██████╔╝██╔████╔██║
 ██╔═══╝ ██║     ██╔══██║   ██║   ██╔══╝  ██║   ██║██╔══██╗██║╚██╔╝██║
 ██║     ███████╗██║  ██║   ██║   ██║     ╚██████╔╝██║  ██║██║ ╚═╝ ██║
 ╚═╝     ╚══════╝╚═╝  ╚═╝   ╚═╝   ╚═╝      ╚═════╝ ╚═╝  ╚═╝╚═╝     ╚═╝
"""


def print_banner() -> None:
    console.print(BANNER, style="bold cyan")
    console.print(Panel(
        "[bold white]Composable open source data platform — built on Docker Compose.[/bold white]\n"
        "Select one component per layer. Incompatible combinations are blocked with a reason.\n"
        "Two ready-made presets are available, or build any combination you need.\n\n"
        "[dim]Requires: Docker Desktop + Docker Compose v2[/dim]",
        border_style="cyan",
        expand=False,
    ))
    console.print()


# ---------------------------------------------------------------------------
# Prerequisite check
# ---------------------------------------------------------------------------

def _run_check(cmd: str) -> bool:
    """Run a shell command, return True if it exits with code 0."""
    try:
        result = subprocess.run(
            cmd.split(),
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False
    except subprocess.TimeoutExpired:
        return False


def check_prerequisites() -> None:
    console.print("[bold]Checking prerequisites...[/bold]")
    console.print()

    # Step 1: Docker CLI
    # `docker version` only needs the binary, not the daemon.
    if not _run_check("docker version"):
        console.print("  [red]FAIL[/red]  Docker CLI not found.")
        console.print()
        console.print("  Docker Desktop is not installed or not on your PATH.")
        console.print("  Install it from: https://www.docker.com/products/docker-desktop/")
        console.print("  After installing, re-run this script.")
        sys.exit(1)

    console.print("  [green]OK[/green]    Docker CLI")

    # Step 2: Docker Compose v2
    # Compose v2 ships as a plugin: `docker compose` not `docker-compose`.
    if not _run_check("docker compose version"):
        console.print("  [red]FAIL[/red]  Docker Compose v2 not found.")
        console.print()
        console.print("  This project uses Compose v2 (`docker compose`, not `docker-compose`).")
        console.print("  Docker Desktop 4.x and later includes it automatically.")
        console.print("  Update Docker Desktop and re-run this script.")
        sys.exit(1)

    console.print("  [green]OK[/green]    Docker Compose v2")

    # Step 3: Docker daemon reachable
    # `docker info` talks to the daemon. On Windows this goes via a named pipe
    # to Docker Desktop. We do NOT attempt to launch Docker Desktop ourselves
    # -- install path varies, it requires user interaction, and UAC can block it.
    # Instead we tell the user exactly what to do and ask them to re-run.
    if not _run_check("docker info"):
        console.print("  [red]FAIL[/red]  Docker daemon is not reachable.")
        console.print()
        # OS-aware guidance — the checks and deployment are identical across
        # platforms (all via `docker compose`); only how you start the daemon
        # differs.
        if sys.platform == "win32":
            how = (
                "  1. Open Docker Desktop from the Start menu or system tray.\n"
                "  2. Wait until the whale icon stops animating (30-60s on first start)."
            )
        elif sys.platform == "darwin":
            how = (
                "  1. Open Docker Desktop from Applications (or the menu-bar whale).\n"
                "  2. Wait until it reports 'Docker Desktop is running'."
            )
        else:  # linux and others
            how = (
                "  1. Start the daemon:  sudo systemctl start docker\n"
                "     (or launch Docker Desktop if that's what you use).\n"
                "  2. Ensure your user can reach it:  docker info"
            )
        console.print(Panel(
            "[bold]Docker is not running.[/bold]\n\n"
            f"{how}\n"
            "  3. Re-run this script:  [bold]python setup.py[/bold]",
            border_style="yellow",
            title="Action required",
            expand=False,
        ))
        sys.exit(1)

    console.print("  [green]OK[/green]    Docker daemon")
    console.print()


# ---------------------------------------------------------------------------
# Preset selection
# ---------------------------------------------------------------------------

def pick_preset_or_custom() -> str:
    """Returns 'preset_0', 'preset_1', ..., or 'custom'."""
    labels = [chr(ord("A") + i) for i in range(len(PRESETS))]

    console.print("[bold]Quick start presets[/bold]")
    console.print()

    for label, preset in zip(labels, PRESETS):
        console.print(f"  [{label}] [bold]{preset['name']}[/bold]")
        console.print(f"      {preset['description']}")
        console.print()

    console.print("  [C] Custom — choose layer by layer")
    console.print()

    valid = {label.lower(): i for i, label in enumerate(labels)}

    while True:
        raw = console.input("  Selection: ").strip().lower()
        if raw == "c":
            return "custom"
        if raw in valid:
            return f"preset_{valid[raw]}"
        options = "/".join(labels) + "/C"
        console.print(f"  Enter {options}.")


# ---------------------------------------------------------------------------
# Layer-by-layer custom selection
# ---------------------------------------------------------------------------

# Layers where Skip is not offered. A platform cannot function without these.
MANDATORY_LAYERS = {
    "storage", "table_format", "metastore", "processing", "query_engine"
}

# Layers where Skip is a valid choice, shown as the last row in the table.
OPTIONAL_LAYERS = {
    "ingestion", "orchestration", "quality", "catalog",
    "governance", "visualization", "observability"
}


def run_custom_selection() -> dict[str, str | None]:
    matrix = load_matrix()
    selections: dict[str, str | None] = {}

    console.print()
    console.print("[bold]Layer-by-layer selection[/bold]")
    console.print(
        "[dim]Mandatory layers must have a selection. "
        "Optional layers show Skip as an explicit option.[/dim]"
    )
    console.print()

    for layer in matrix:
        is_optional = layer.name in OPTIONAL_LAYERS

        tag = "[dim](optional)[/dim]" if is_optional else "[red](required)[/red]"
        console.print(f"[bold cyan]Layer: {layer.display}[/bold cyan]  {tag}")
        console.print(f"[dim]{layer.description}[/dim]")
        console.print()

        statuses = evaluate_layer(layer, {k: v for k, v in selections.items() if v})
        available = [s for s in statuses if s.available]
        blocked = [s for s in statuses if not s.available]

        # Edge case: all options blocked on an optional layer — auto-skip
        if not available:
            if is_optional:
                console.print("  No compatible options remain for this layer given your selections.")
                console.print("  [dim]Skipping automatically.[/dim]")
                console.print()
                selections[layer.name] = None
                continue
            else:
                console.print(
                    "[red]  No compatible stacks available for this required layer.[/red]\n"
                    "  This is likely a bug in the compatibility matrix. "
                    "Please open an issue on the repo."
                )
                sys.exit(1)

        # Build the options table
        table = Table(box=box.SIMPLE, show_header=True, header_style="bold")
        table.add_column("#", style="bold", width=4)
        table.add_column("Stack", style="bold white", min_width=20)
        table.add_column("Description")

        for i, status in enumerate(available, start=1):
            warning_text = ""
            if status.stack.warning:
                warning_text = f"\n  [yellow]Warning:[/yellow] {status.stack.warning}"
            table.add_row(
                str(i),
                status.stack.display,
                status.stack.description + warning_text,
            )

        # Skip row — only for optional layers
        skip_number = len(available) + 1
        if is_optional:
            table.add_row(
                str(skip_number),
                "[dim]Skip[/dim]",
                "[dim]Do not include this layer in the platform[/dim]",
            )

        console.print(table)

        # Blocked options — shown below the table, dimmed
        if blocked:
            console.print("  [dim]Unavailable given your current selections:[/dim]")
            for status in blocked:
                console.print(
                    f"    [dim]- {status.stack.display}: "
                    f"{status.blocked_reason} "
                    f"(blocked by {status.blocked_by})[/dim]"
                )
            console.print()

        max_choice = skip_number if is_optional else len(available)

        while True:
            raw = console.input(f"  Select [1-{max_choice}]: ").strip()

            if raw.isdigit():
                idx = int(raw) - 1
                # Skip row selected
                if is_optional and int(raw) == skip_number:
                    selections[layer.name] = None
                    console.print("  [dim]Skipped.[/dim]")
                    break
                # Valid stack selected
                if 0 <= idx < len(available):
                    chosen = available[idx].stack
                    selections[layer.name] = chosen.name
                    console.print(f"  [green]Selected:[/green] {chosen.display}")
                    if chosen.warning:
                        console.print(f"  [yellow]Note:[/yellow] {chosen.warning}")
                    break

            console.print(f"  Enter a number between 1 and {max_choice}.")

        console.print()

    return selections


# ---------------------------------------------------------------------------
# Confirm and generate
# ---------------------------------------------------------------------------

def print_summary(selections: dict, preset_name: str) -> None:
    console.print()
    console.print(Panel(
        f"[bold]Stack summary — {preset_name}[/bold]",
        border_style="cyan",
        expand=False,
    ))

    table = Table(box=box.SIMPLE, show_header=True, header_style="bold")
    table.add_column("Layer", min_width=18)
    table.add_column("Selected Stack", style="bold white")

    for layer, stack in selections.items():
        display = stack if stack else "[dim]skipped[/dim]"
        table.add_row(layer, display)

    console.print(table)
    console.print()


def confirm() -> bool:
    raw = console.input(
        "  Generate docker-compose.yml and start the platform? [Y/n]: "
    ).strip().lower()
    return raw in ("", "y", "yes")


# ---------------------------------------------------------------------------
# Endpoint reporting
# ---------------------------------------------------------------------------

# UI / API endpoints worth surfacing, keyed by the compose service name so we
# only print ones that are actually in the generated stack. Each entry:
#   (compose_service, label, host_port_env, default_port, path, creds_env)
# host_port_env is read from generated/.env so custom port overrides are
# reflected; creds_env is an optional (user_env, password_env) pair.
UI_ENDPOINTS = [
    ("trino",             "Trino — SQL & Web UI",   "TRINO_PORT",           "8080", "",            None),
    ("airflow-webserver", "Airflow",                "AIRFLOW_PORT",         "8082", "",            ("AIRFLOW_ADMIN_USER", "AIRFLOW_ADMIN_PASSWORD")),
    ("spark-master",      "Spark master UI",        "SPARK_MASTER_UI_PORT", "8081", "",            None),
    ("minio",             "MinIO console",          "MINIO_CONSOLE_PORT",   "9001", "",            ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")),
    ("kafka-ui",          "Kafka UI",               "KAFKA_UI_PORT",        "9094", "",            None),
    ("kafka-connect",     "Kafka Connect REST API", "KAFKA_CONNECT_PORT",   "8083", "/connectors", None),
    ("grafana",           "Grafana",                "GRAFANA_PORT",         "3000", "",            ("GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD")),
    ("prometheus",        "Prometheus",             "PROMETHEUS_PORT",      "9090", "",            None),
    ("marquez-web",       "Marquez (lineage UI)",   "MARQUEZ_WEB_PORT",     "3001", "",            None),
    ("marquez",           "Marquez API",            "MARQUEZ_PORT",         "5000", "",            None),
    ("netdata",           "Netdata dashboard",      "NETDATA_PORT",         "19999","",            None),
    ("flink-jobmanager",  "Flink JobManager UI",    "FLINK_UI_PORT",        "8085", "",            None),
    ("prefect",           "Prefect UI",             "PREFECT_PORT",         "4200", "",            None),
    ("nessie",            "Nessie API",             "NESSIE_PORT",          "19120","",            None),
    ("metabase",          "Metabase",               "METABASE_PORT",        "3002", "",            None),
    ("dagster",           "Dagster UI",             "DAGSTER_PORT",         "3003", "",            None),
    ("nifi",              "Apache NiFi",            "NIFI_PORT",            "8086", "/nifi",       None),
    ("namenode",          "HDFS NameNode UI",       "HDFS_NAMENODE_UI_PORT","9870", "",            None),
    ("redash-server",     "Redash",                 "REDASH_PORT",          "5010", "",            None),
]


def _load_env_file(path: str) -> dict[str, str]:
    env: dict[str, str] = {}
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                env[key.strip()] = val.strip()
    except OSError:
        pass
    return env


def _compose_services(path: str) -> set[str]:
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return set((data.get("services") or {}).keys())
    except (OSError, yaml.YAMLError):
        return set()


def print_endpoints(output_dir: str = "generated") -> None:
    """Print clickable UI/API endpoints for the services that were generated."""
    services = _compose_services(os.path.join(output_dir, "docker-compose.yml"))
    if not services:
        return
    env = _load_env_file(os.path.join(output_dir, ".env"))

    rows = []
    for svc, label, port_env, default_port, path, creds in UI_ENDPOINTS:
        if svc not in services:
            continue
        port = env.get(port_env, default_port)
        url = f"http://localhost:{port}{path}"
        login = ""
        if creds:
            user = env.get(creds[0], "")
            pwd = env.get(creds[1], "")
            if user or pwd:
                login = f"{user} / {pwd}"
        rows.append((label, url, login))

    if not rows:
        return

    console.print()
    console.print("[bold]Service endpoints[/bold] [dim](Ctrl/Cmd-click to open)[/dim]")
    table = Table(box=box.SIMPLE, show_header=True, header_style="bold")
    table.add_column("Service")
    table.add_column("URL", style="cyan")
    table.add_column("Login")
    for label, url, login in rows:
        table.add_row(label, f"[link={url}]{url}[/link]", login or "[dim]—[/dim]")
    console.print(table)


def launch(output_dir: str = "generated") -> None:
    console.print()
    console.print("[bold]Starting platform...[/bold]")
    console.print(f"  Running: docker compose -f {output_dir}/docker-compose.yml up -d")
    console.print()

    try:
        result = subprocess.run(
            ["docker", "compose", "-f", f"{output_dir}/docker-compose.yml", "up", "-d"],
            cwd=os.getcwd(),
        )
    except KeyboardInterrupt:
        console.print()
        console.print("[yellow]Interrupted.[/yellow]")
        console.print(
            "Docker may still be pulling images in the background. "
            "Check with:"
        )
        console.print(f"  [bold]docker compose -f {output_dir}/docker-compose.yml ps[/bold]")
        console.print()
        console.print("To start manually when ready:")
        console.print(f"  [bold]docker compose -f {output_dir}/docker-compose.yml up -d[/bold]")
        sys.exit(0)

    if result.returncode != 0:
        console.print()
        console.print("[red]docker compose exited with an error.[/red]")
        console.print(
            "Review the output above. "
            "Common causes: port conflicts, insufficient Docker memory allocation."
        )
        sys.exit(1)

    console.print()
    console.print("[bold green]Platform is starting.[/bold green]")
    console.print(
        "Services start in dependency order. "
        "Some (HMS, Airflow) take 60-90 seconds to become ready."
    )
    console.print()
    console.print("Run this to watch service health:")
    console.print(f"  [bold]docker compose -f {output_dir}/docker-compose.yml ps[/bold]")

    print_endpoints(output_dir)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print_banner()
    check_prerequisites()

    choice = pick_preset_or_custom()

    if choice == "custom":
        selections = run_custom_selection()
        preset_name = "custom"
    else:
        idx = int(choice.split("_")[1])
        preset = PRESETS[idx]
        selections = preset["selections"]
        preset_name = preset["name"]
        console.print(f"  Using preset: [bold]{preset_name}[/bold]")

    print_summary(selections, preset_name)

    # Filter out None selections before passing to assembler
    final = {k: v for k, v in selections.items() if v is not None}

    console.print("[bold]Generating configuration...[/bold]")
    assemble(final, stack_name=preset_name)

    if confirm():
        launch()
    else:
        console.print()
        console.print(
            "Configuration saved to [bold]generated/[/bold]. "
            "Start manually when ready:"
        )
        console.print(
            "  [bold]docker compose -f generated/docker-compose.yml up -d[/bold]"
        )


if __name__ == "__main__":
    main()
