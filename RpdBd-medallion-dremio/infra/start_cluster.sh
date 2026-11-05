#!/usr/bin/env bash
# start calego klastra, uruchamiac na n1
set -euo pipefail

# ~/.bashrc w powloce nieinteraktywnej czesto konczy sie przed exportami, wiec domyslne sciezki
set +u; source ~/.bashrc 2>/dev/null || true; set -u
HADOOP_HOME="${HADOOP_HOME:-$HOME/cluster/hadoop}"
SPARK_HOME="${SPARK_HOME:-$HOME/cluster/spark}"
export HADOOP_HOME SPARK_HOME

echo "hdfs"
"$HADOOP_HOME/sbin/start-dfs.sh"

echo "kafka"
for n in n1 n2 n3; do
  ssh "$n" "~/cluster/kafka/bin/kafka-server-start.sh -daemon ~/cluster/kafka/config/cluster.properties"
done

echo "spark"
"$SPARK_HOME/sbin/start-master.sh"
"$SPARK_HOME/sbin/start-workers.sh"

echo "gotowe, kontrola: infra/status.sh"
