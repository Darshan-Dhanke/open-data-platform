#!/bin/bash
set -e

# The PostgreSQL JDBC driver is bundled in the repo and mounted read-only
# at /opt/hive/lib/postgresql.jar by docker-compose. No network download
# happens here — apache/hive:4.0.0 ships without wget/curl, and runtime
# downloads were the source of repeated metastore startup failures.

JDBC_JAR=/opt/hive/lib/postgresql.jar

if [ ! -f "$JDBC_JAR" ]; then
    echo "FATAL: $JDBC_JAR is not present."
    echo "It should be bind-mounted from configs/hive-metastore/postgresql.jar."
    echo "Re-run setup.py (which restores the jar) and recreate this container."
    exit 1
fi

JAR_SIZE=$(stat -c%s "$JDBC_JAR" 2>/dev/null || echo 0)
if [ "$JAR_SIZE" -lt 100000 ]; then
    echo "FATAL: $JDBC_JAR is only ${JAR_SIZE} bytes — looks corrupt."
    exit 1
fi

# Put the Hadoop S3A connector on the metastore classpath so the metastore
# can create and manage table/namespace locations on s3a:// (MinIO). The
# hadoop-aws and aws-java-sdk-bundle jars ship in the image under hadoop's
# tools/lib, which is NOT on the Hive classpath by default — without this,
# any CREATE SCHEMA/TABLE with an s3a:// location fails with
# "ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem".
S3A_JARS=$(ls /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-*.jar /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar 2>/dev/null | tr "\n" ":")
export HADOOP_CLASSPATH="${S3A_JARS}${HADOOP_CLASSPATH}"
export HADOOP_OPTIONAL_TOOLS=hadoop-aws

echo "JDBC driver present: $JDBC_JAR (${JAR_SIZE} bytes). Starting Hive Metastore..."
echo "S3A on metastore classpath: ${S3A_JARS:-<none found>}"
exec /entrypoint.sh hivemetastore
