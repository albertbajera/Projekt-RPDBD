#!/usr/bin/env bash
# Jednorazowa migracja istniejacych danych HDFS do warstw medallion (/lake/bronze|silver|gold). Na n1.
# Nic nie kasuje: stare sciezki /data/raw i /data/curated zostaja, dopoki sami ich nie usuniemy.
#   infra/lake_migrate.sh          # kopiuje (hdfs dfs -cp)
set -euo pipefail
HDFS="${HADOOP_HOME:-$HOME/cluster/hadoop}/bin/hdfs"

"$HDFS" dfs -mkdir -p /lake/bronze/flights /lake/bronze/weather /lake/silver /lake/gold

copy() {  # copy ZRODLO CEL: pomija, gdy brak zrodla lub cel juz istnieje
  if ! "$HDFS" dfs -test -e "$1"; then echo "pomijam $1 (brak)"; return; fi
  if "$HDFS" dfs -test -e "$2"; then echo "pomijam $2 (juz jest)"; return; fi
  echo "$1 -> $2"
  "$HDFS" dfs -cp "$1" "$2"
}

copy /data/raw/flights/flights.csv        /lake/bronze/flights/flights.csv
copy /data/raw/flights/flights.parquet    /lake/bronze/flights/flights.parquet
copy /data/raw/flights/airports.csv       /lake/bronze/flights/airports.csv
copy /data/raw/flights/airlines.csv       /lake/bronze/flights/airlines.csv
"$HDFS" dfs -test -e /data/raw/weather && "$HDFS" dfs -cp -f '/data/raw/weather/*' /lake/bronze/weather/ || true
copy /data/curated/flights_weather        /lake/silver/flights_weather
copy /data/curated/baseline               /lake/gold/baseline

echo "== /lake"
"$HDFS" dfs -ls -R /lake | head -40
