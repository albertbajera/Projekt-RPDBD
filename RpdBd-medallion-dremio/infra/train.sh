#!/bin/bash
# Trening modelu na klastrze (wsad). Uruchamiac na n1 z katalogu repo, gdy klaster stoi (infra/status.sh).
# Nie startuje HDFS, Kafki ani Sparka. Pełne ścieżki, bo SSH bez logowania ma puste PATH.
set -euo pipefail
cd "$(dirname "$0")/.."

HDFS="${HADOOP_HOME:-$HOME/cluster/hadoop}/bin/hdfs"
SPARK_SUBMIT="${SPARK_HOME:-$HOME/cluster/spark}/bin/spark-submit"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/cluster/hadoop}"
export SPARK_HOME="${SPARK_HOME:-$HOME/cluster/spark}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"

"$HDFS" dfs -mkdir -p /lake/bronze/flights /lake/bronze/weather /lake/silver /lake/gold /models /data/metrics

if [[ -f data/raw/flights/flights.csv ]]; then
  "$HDFS" dfs -put -f data/raw/flights/flights.csv /lake/bronze/flights/
elif [[ -f data/raw/flights/flights.parquet ]]; then
  "$HDFS" dfs -put -f data/raw/flights/flights.parquet /lake/bronze/flights/
else
  echo "Brak flights.csv i flights.parquet" >&2
  exit 1
fi
if [[ -f data/raw/flights/airports.csv ]]; then
  "$HDFS" dfs -put -f data/raw/flights/airports.csv /lake/bronze/flights/
elif [[ -f data/raw/flights/airports.parquet ]]; then
  "$HDFS" dfs -put -f data/raw/flights/airports.parquet /lake/bronze/flights/
fi
if [[ -f data/raw/flights/airlines.csv ]]; then
  "$HDFS" dfs -put -f data/raw/flights/airlines.csv /lake/bronze/flights/
elif [[ -f data/raw/flights/airlines.parquet ]]; then
  "$HDFS" dfs -put -f data/raw/flights/airlines.parquet /lake/bronze/flights/
fi
"$HDFS" dfs -put -f data/raw/weather/weather_*.json /lake/bronze/weather/
if [[ -f data/raw/weather/weather_hourly.csv ]]; then
  "$HDFS" dfs -put -f data/raw/weather/weather_hourly.csv /lake/bronze/weather/
fi
"$HDFS" dfsadmin -report | grep -E "Live datanodes"
VENV="$HOME/venv-train"
if [[ ! -x "$VENV/bin/python" ]]; then
  python3 -m venv "$VENV"
fi
"$VENV/bin/pip" install --disable-pip-version-check "numpy<2" setuptools
export PYSPARK_DRIVER_PYTHON="$VENV/bin/python"
export PYSPARK_PYTHON="${PYSPARK_PYTHON_WORKERS:-python3}"

"$SPARK_SUBMIT" --master spark://n1:7077 \
  --total-executor-cores 12 \
  --executor-cores 2 \
  --executor-memory 8g \
  --driver-memory 2g \
  batch/train.py
