#!/usr/bin/env bash
# Strumien Spark: pogoda + samoloty -> alerts (ryzyko + szansa opoznienia z modelu). Na n1:  infra/stream.sh [prefix]
# Model: /models/delay_gbt (po infra/train.sh). Bez modelu: MODEL= infra/stream.sh
set -euo pipefail
cd "$(dirname "$0")/.."
P="${1:-}"
MODEL="${MODEL-hdfs://n1:9000/models/delay_gbt}"
VENV="$HOME/venv-train"
[ -x "$VENV/bin/python" ] || { python3 -m venv "$VENV"; "$VENV/bin/pip" install -q "numpy<2" setuptools; }
export PYSPARK_DRIVER_PYTHON="$VENV/bin/python" PYSPARK_PYTHON=python3
CKPT="hdfs://n1:9000/checkpoints/stream-${P:-prod}"
~/cluster/spark/bin/spark-submit --master spark://n1:7077 \
  --total-executor-cores 8 --executor-cores 2 --executor-memory 4g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  streaming/risk.py --prefix "$P" --checkpoint "$CKPT" ${MODEL:+--model "$MODEL"}
