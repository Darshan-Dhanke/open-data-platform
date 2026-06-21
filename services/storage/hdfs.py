"""
HDFS storage block — an alternative to MinIO/S3.

Runs a single-node HDFS cluster (one NameNode + one DataNode) using the
well-known bde2020 Hadoop images, which are configured entirely through
CORE_CONF_/HDFS_CONF_ environment variables. Other layers reference HDFS via
hdfs://namenode:8020 (see _warehouse_path in the metastore module).

Exposes:
  - NameNode UI : http://localhost:${HDFS_NAMENODE_UI_PORT}  (default 9870)
"""

from __future__ import annotations

NAMENODE_IMAGE = "darshandhanke07/odp-hadoop-namenode:2.0.0-hadoop3.2.1-java8"
DATANODE_IMAGE = "darshandhanke07/odp-hadoop-datanode:2.0.0-hadoop3.2.1-java8"

_CORE_CONF = {
    "CORE_CONF_fs_defaultFS": "hdfs://namenode:8020",
    "CORE_CONF_hadoop_http_staticuser_user": "root",
}


def service_blocks(selections: dict) -> dict:
    return {
        "namenode": {
            "image": NAMENODE_IMAGE,
            "container_name": "namenode",
            "ports": [
                "${HDFS_NAMENODE_UI_PORT:-9870}:9870",
                "${HDFS_NAMENODE_RPC_PORT:-8020}:8020",
            ],
            "environment": {
                "CLUSTER_NAME": "open-data-platform",
                **_CORE_CONF,
            },
            "volumes": ["hdfs_namenode:/hadoop/dfs/name"],
            "healthcheck": {
                "test": ["CMD-SHELL", "bash -c 'echo > /dev/tcp/localhost/9870' 2>/dev/null || exit 1"],
                "interval": "30s",
                "timeout": "10s",
                "retries": 6,
                "start_period": "30s",
            },
            "networks": ["platform"],
        },
        "datanode": {
            "image": DATANODE_IMAGE,
            "container_name": "datanode",
            "environment": {
                # Wait for the NameNode web UI before the DataNode registers.
                "SERVICE_PRECONDITION": "namenode:9870",
                **_CORE_CONF,
            },
            "volumes": ["hdfs_datanode:/hadoop/dfs/data"],
            "depends_on": {"namenode": {"condition": "service_healthy"}},
            "networks": ["platform"],
        },
    }


def env_vars(selections: dict) -> dict:
    return {
        "HDFS_NAMENODE_UI_PORT":  "9870",
        "HDFS_NAMENODE_RPC_PORT": "8020",
    }


def named_volumes(selections: dict) -> list[str]:
    return ["hdfs_namenode", "hdfs_datanode"]


def named_networks(selections: dict) -> list[str]:
    return ["platform"]
