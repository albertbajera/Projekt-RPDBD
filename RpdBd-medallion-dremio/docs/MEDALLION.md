# Warstwy medallion (bronze / silver / gold)

Jezioro danych w HDFS ma trzy warstwy, osobno dla historii (wsad) i dla danych na zywo (strumien).
Sciezki sa w jednym miejscu: [common/lake.py](../common/lake.py).

## Na zywo: Kafka -> bronze -> silver -> gold -> PostgreSQL -> dashboard

Job Spark Structured Streaming [streaming/lake_stream.py](../streaming/lake_stream.py) (`infra/lake_stream.sh`, n1) dziala obok
`streaming/risk.py`, ktory dalej liczy alerty i nie jest ruszany.

    Kafka: opensky.states, weather.current, alerts, anomalies
      |-> BRONZE /lake/bronze/live/<temat>   surowe wiadomosci 1:1 (key, value, offset, kafka_time)
      |-> SILVER /lake/silver/live/<temat>   sparsowane wg schematow, tylko poprawne lotniska i czasy, bez duplikatow
      |-> GOLD   /lake/gold/live/alerts, anomalies, weather_current   wzbogacone (high_risk, airport_wide, severe)
                 /lake/gold/live/airport_activity                     samoloty na lotnisku w oknach 5 min
                   |-> PostgreSQL (alerts, anomalies, weather) -> dashboard Streamlit

Parquet, partycje po dacie (`ingest_date`). Zapis do PostgreSQL pomija duplikaty, wiec dziala rownolegle ze starym
`storage/to_postgres.py` (Kafka -> PostgreSQL); po sprawdzeniu mozna go zatrzymac (`infra/ingest.sh`, sesja `pgsink`).
Gold `airport_activity` pojawia sie z opoznieniem ok. 20 minut (okno zamykane watermarkiem).

## Historia (wsad): `infra/train.sh`

| Warstwa | Sciezka HDFS | Format | Kto pisze |
| --- | --- | --- | --- |
| bronze | `/lake/bronze/flights`, `/lake/bronze/weather` | csv/parquet, json | `infra/train.sh` (upload surowych plikow) |
| silver | `/lake/silver/flights_weather` | parquet | `batch/train.py` (join lotow z pogoda) |
| gold | `/lake/gold/baseline` (json), `/lake/gold/delay_by_airport_month` (parquet) | json, parquet | `batch/train.py` |
| gold (model) | `/models/delay_gbt` | Spark ML | `batch/train.py` |

Istniejace dane przeniesiesz bez ponownego treningu: `infra/lake_migrate.sh` (kopiuje `/data/raw`, `/data/curated`, nic nie kasuje).

## Uruchomienie warstw na zywo (n1)

1. Klaster stoi (`infra/status.sh`), strumien `risk.py` dziala (`infra/stream.sh`), producent na n4 (`infra/ingest.sh start`).
2. Opcjonalnie `PG_DSN=...` w `.env` na n1 (jak na n4/n5), inaczej job zapisze tylko do HDFS.
3. W tmux na n1: `infra/lake_stream.sh` (testowo: `infra/lake_stream.sh dev-`, tematy z prefiksem).

## Jak to obejrzec (Web UI)

- **Spark UI** `http://n1:8080` -> aplikacja `lake-stream` -> zakladka *Structured Streaming*: osobne zapytania `bronze_*`, `silver_gold_*`
  i `gold_airport_activity`, z liczba rekordow na sekunde.
- **HDFS NameNode** `http://n1:9870` -> *Utilities -> Browse the file system* -> `/lake/bronze/live`, `/lake/silver/live`, `/lake/gold/live`.
- Z laptopa przez tunel SSH, np. `ssh -L 8080:10.7.11.26:8080 -L 9870:10.7.11.26:9870 student@10.7.11.26`.

## Stan sprawdzenia

Job `lake_stream.py` napisany bez dostepu do klastra i **nie uruchomiony**. Pierwsze uruchomienie moze wymagac poprawek
(pamiec, prawa do `/lake`, brak psycopg2 w venv). Dremio rozwazalismy jako Web UI, ale zrezygnowano z niego (opis opcji: `MEDALLION_OPCJE.md`).
