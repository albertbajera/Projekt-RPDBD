#!/usr/bin/env bash
# Tworzy tematy Kafki (3 partycje, replikacja 3). Na n1:  infra/create_topics.sh [prefix]   np. dev-
P="${1:-}"
for t in opensky.states weather.current alerts anomalies; do
  ~/cluster/kafka/bin/kafka-topics.sh --bootstrap-server n1:9092 --create --if-not-exists \
    --topic "${P}${t}" --partitions 3 --replication-factor 3
done
