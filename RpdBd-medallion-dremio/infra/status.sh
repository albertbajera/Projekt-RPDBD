#!/usr/bin/env bash
# szybka kontrola, co dziala na ktorym wezle
HDFS="${HADOOP_HOME:-$HOME/cluster/hadoop}/bin/hdfs"
KAFKA_BIN="$HOME/cluster/kafka/bin"
for n in n1 n2 n3 n4 n5; do
  echo "== $n"
  ssh "$n" jps | grep -v Jps
done
echo "== hdfs"
"$HDFS" dfsadmin -report | grep -E "Live datanodes"
echo "== kafka"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server n1:9092 --list
echo "== sesje tmux (producent, detektor, dashboard)"
for n in n1 n4 n5; do
  echo "-- $n"; ssh "$n" "tmux ls 2>/dev/null" || true
done
