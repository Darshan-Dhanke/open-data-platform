"""
Apache Flink processing block — an alternative to Spark.

Runs a standalone Flink session cluster (one JobManager + one TaskManager) that
accepts batch and streaming jobs. This is the compute-engine alternative in the
processing layer; the JobManager web UI lets you submit and monitor jobs.

Exposes:
  - Flink JobManager UI : http://localhost:${FLINK_UI_PORT}  (default 8085)
"""

from __future__ import annotations

FLINK_IMAGE = "flink:1.18.1-scala_2.12-java11"

# Shared cluster settings. jobmanager.rpc.address must resolve to the JobManager
# service so the TaskManager can register with it.
_FLINK_PROPERTIES = (
    "jobmanager.rpc.address: flink-jobmanager\n"
    "taskmanager.numberOfTaskSlots: ${FLINK_TASK_SLOTS:-2}\n"
    "parallelism.default: 1\n"
)


def service_blocks(selections: dict) -> dict:
    blocks: dict = {}

    blocks["flink-jobmanager"] = {
        "image": FLINK_IMAGE,
        "container_name": "flink-jobmanager",
        "command": ["jobmanager"],
        "ports": ["${FLINK_UI_PORT:-8085}:8081"],
        "environment": {"FLINK_PROPERTIES": _FLINK_PROPERTIES},
        "healthcheck": {
            # The Flink image ships neither curl nor wget reliably, so probe the
            # web port with bash's /dev/tcp built-in.
            "test": ["CMD-SHELL", "bash -c 'echo > /dev/tcp/localhost/8081' 2>/dev/null || exit 1"],
            "interval": "15s",
            "timeout": "5s",
            "retries": 5,
            "start_period": "20s",
        },
        "networks": ["platform"],
    }

    blocks["flink-taskmanager"] = {
        "image": FLINK_IMAGE,
        "command": ["taskmanager"],
        "environment": {"FLINK_PROPERTIES": _FLINK_PROPERTIES},
        "depends_on": {"flink-jobmanager": {"condition": "service_healthy"}},
        "deploy": {"replicas": "${FLINK_TASKMANAGER_REPLICAS:-1}"},
        "networks": ["platform"],
    }

    return blocks


def env_vars(selections: dict) -> dict:
    return {
        "FLINK_UI_PORT":              "8085",
        "FLINK_TASK_SLOTS":           "2",
        "FLINK_TASKMANAGER_REPLICAS": "1",
    }


def named_volumes(selections: dict) -> list[str]:
    return []


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
