"""
Netdata observability block.

A lightweight, single-container alternative to the Prometheus + Grafana +
cAdvisor stack. Netdata auto-discovers system and per-container metrics with
zero scrape config and ships its own real-time dashboard, so it gives the
observability layer a genuine low-footprint alternative (~150-250 MiB vs the
~400 MiB+ of the Prometheus stack).

Exposes:
  - Netdata dashboard : http://localhost:${NETDATA_PORT}  (default 19999)
"""

from __future__ import annotations

NETDATA_IMAGE = "darshandhanke07/odp-netdata:v1.47.0"


def service_blocks(selections: dict) -> dict:
    return {
        "netdata": {
            "image": NETDATA_IMAGE,
            "container_name": "netdata",
            "ports": ["${NETDATA_PORT:-19999}:19999"],
            # Netdata needs to read host process/cgroup info and the Docker
            # socket (to label per-container metrics). These caps/mounts are the
            # standard, documented Netdata container setup.
            "cap_add": ["SYS_PTRACE", "SYS_ADMIN"],
            "security_opt": ["apparmor:unconfined"],
            "volumes": [
                "netdata_config:/etc/netdata",
                "netdata_lib:/var/lib/netdata",
                "netdata_cache:/var/cache/netdata",
                "/etc/passwd:/host/etc/passwd:ro",
                "/etc/group:/host/etc/group:ro",
                "/proc:/host/proc:ro",
                "/sys:/host/sys:ro",
                "/etc/os-release:/host/etc/os-release:ro",
                "/var/run/docker.sock:/var/run/docker.sock:ro",
            ],
            "healthcheck": {
                "test": ["CMD-SHELL", "wget -q -O /dev/null http://localhost:19999/api/v1/info || exit 1"],
                "interval": "30s",
                "timeout": "10s",
                "retries": 5,
                "start_period": "30s",
            },
            "networks": ["platform"],
        }
    }


def env_vars(selections: dict) -> dict:
    return {"NETDATA_PORT": "19999"}


def named_volumes(selections: dict) -> list[str]:
    return ["netdata_config", "netdata_lib", "netdata_cache"]


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
