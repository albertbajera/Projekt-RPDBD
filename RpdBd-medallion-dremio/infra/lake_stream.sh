#!/usr/bin/env bash
# Warstwy medallion na zywo: Kafka -> bronze -> silver -> gold (HDFS) + gold -> PostgreSQL. Na n1 (w tmux):
#   infra/lake_stream.sh [prefix]        # bez zapisu do PostgreSQL: NO_PG=1 infra/lake_stream.sh
# PostgreSQL: PG_DSN w .env na n1 (jak na n4 i n5). Obok dziala streaming/risk.py (infra/stream.sh), nie ruszamy go.
set -euo pipefail
cd "$(dirname "$0")/.."
P="${1:-}"
VENV="$HOME/venv-train"
[ -x "$VENV/bin/python" ] || { python3 -m venv "$VENV"; "$VENV/bin/pip" install -q "numpy<2" setuptools; }
"$VENV/bin/pip" install -q --disable-pip-version-check psycopg2-binary
export PYSPARK_DRIVER_PYTHON="$VENV/bin/python" PYSPARK_PYTHON=python3
~/cluster/hadoop/bin/hdfs dfs -mkdir -p /lake/bronze /lake/silver /lake/gold
~/cluster/spark/bin/spark-submit --master spark://n1:7077 \
  --total-executor-cores 4 --executor-cores 1 --executor-memory 2g --driver-memory 1g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  streaming/lake_stream.py --prefix "$P" ${NO_PG:+--no-pg}
