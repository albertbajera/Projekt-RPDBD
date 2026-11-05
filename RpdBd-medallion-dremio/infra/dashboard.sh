#!/usr/bin/env bash
# Dashboard Streamlit na n5 (port 8501):  infra/dashboard.sh [prefix]
set -euo pipefail
cd "$(dirname "$0")/.."
[ -d .venv ] || python3 -m venv .venv
. .venv/bin/activate; pip install -q -r requirements.txt
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-n1:9092,n2:9092,n3:9092}" TOPIC_PREFIX="${1:-}"
exec streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port 8501 --server.headless true
