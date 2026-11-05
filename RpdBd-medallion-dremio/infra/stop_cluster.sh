#!/usr/bin/env bash
# zatrzymanie klastra w odwrotnej kolejnosci, uruchamiac na n1
# nie testowane na klastrze, przy pierwszym uzyciu obserwuj wynik
set -uo pipefail

# ~/.bashrc w powloce nieinteraktywnej czesto konczy sie przed exportami, wiec domyslne sciezki
set +u; source ~/.bashrc 2>/dev/null || true; set -u
HADOOP_HOME="${HADOOP_HOME:-$HOME/cluster/hadoop}"
SPARK_HOME="${SPARK_HOME:-$HOME/cluster/spark}"
export HADOOP_HOME SPARK_HOME

echo "spark"
"$SPARK_HOME/sbin/stop-workers.sh"
"$SPARK_HOME/sbin/stop-master.sh"

echo "kafka"
for n in n1 n2 n3; do
  ssh "$n" "~/cluster/kafka/bin/kafka-server-stop.sh" || true
done

echo "hdfs"
"$HADOOP_HOME/sbin/stop-dfs.sh"
