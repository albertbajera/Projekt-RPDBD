#!/usr/bin/env bash
# Producent (OpenSky + Open-Meteo -> Kafka) i detektor anomalii w tmux. Na n4:
#   infra/ingest.sh start [prefix]  |  stop  |  status
set -euo pipefail
cd "$(dirname "$0")/.."
CMD="${1:-status}"; P="${2:-}"
case "$CMD" in
start)
  command -v tmux >/dev/null || { echo "brak tmux"; exit 1; }
  [ -d .venv ] || python3 -m venv .venv
  . .venv/bin/activate; pip install -q -r requirements.txt; mkdir -p out
  [ -f credentials.json ] || [ -f .env ] || echo "UWAGA: brak danych OpenSky (credentials.json lub .env), poleci anonimowo"
  BASE=""
  if [ ! -s out/baseline.jsonl ] && [ -x "$HOME/cluster/hadoop/bin/hdfs" ]; then
    "$HOME/cluster/hadoop/bin/hdfs" dfs -getmerge /lake/gold/baseline out/baseline.jsonl 2>/dev/null || true
  fi
  [ -s out/baseline.jsonl ] && BASE="--baseline out/baseline.jsonl"
  export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-n1:9092,n2:9092,n3:9092}"
  for s in producer detector; do tmux has-session -t "$s" 2>/dev/null && { echo "$s juz dziala"; exit 1; }; done
  tmux new-session -d -s producer "bash -lc '. .venv/bin/activate; python producers/producer.py --sink kafka --prefix \"$P\" 2>&1 | tee -a out/producer.log'"
  tmux new-session -d -s detector "bash -lc '. .venv/bin/activate; python anomalies/detector.py --kafka --prefix \"$P\" $BASE 2>&1 | tee -a out/detector.log'"
  if [ -n "${PG_DSN:-}" ] || grep -q '^PG_DSN=.' .env 2>/dev/null; then
    tmux new-session -d -s pgsink "bash -lc '. .venv/bin/activate; python storage/to_postgres.py --prefix \"$P\" 2>&1 | tee -a out/pgsink.log'"
    echo "uruchomiono pgsink (zapis do PostgreSQL)"
  else
    echo "PG_DSN nie ustawione: bez zapisu do PostgreSQL"
  fi
  echo "uruchomiono producer i detector (prefiks: '${P}')"; tmux ls ;;
stop)
  for s in producer detector pgsink; do
    if tmux has-session -t "$s" 2>/dev/null; then
      tmux send-keys -t "$s" C-c; sleep 3; tmux kill-session -t "$s" 2>/dev/null || true; echo "zatrzymano $s"
    fi
  done ;;
status)
  tmux ls 2>&1 | grep -E "producer|detector|pgsink" || echo "nie dziala"
  for f in producer detector pgsink; do echo "== out/$f.log"; tail -n 6 "out/$f.log" 2>/dev/null; done
  echo "anomalie od startu: $(grep -c '^ANOMALIA' out/detector.log 2>/dev/null || echo 0)" ;;
*) echo "uzycie: $0 start [prefix] | stop | status"; exit 1 ;;
esac
