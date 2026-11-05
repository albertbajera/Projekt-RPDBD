# System wczesnego ostrzegania o zakloceniach lotow przez pogode

Z historii lotow i pogody uczymy model, kiedy pogoda opoznia odloty. Na zywo liczymy ryzyko dla 5 lotnisk USA
(ATL, ORD, DFW, JFK, LAX), wykrywamy anomalie w ruchu i pokazujemy to na dashboardzie z asystentem AI.

## Przeplyw danych

    OpenSky + Open-Meteo --> [producers] --> Kafka: opensky.states, weather.current
                                                  |-> [anomalies] --> Kafka: anomalies
                                                  |-> [streaming + model] --> Kafka: alerts
    DOT 2015 + pogoda archiwalna --> [batch] --> HDFS /models/delay_gbt (model), /lake/gold/baseline (normy)
    Kafka: alerts, anomalies, weather.current --> [dashboard] + [agent]

## Moduly

| Katalog | Co robi |
| --- | --- |
| contracts/ | wspolne umowy: tematy Kafki, schematy wiadomosci, lotniska, cechy modelu |
| infra/ | instalacja i start klastra, tematy, uruchamianie kolejnych etapow (skrypty .sh) |
| producers/ | pobiera OpenSky (co 120 s) i Open-Meteo (co 4 min), wysyla do Kafki |
| batch/ | pobranie danych, laczenie lotow z pogoda, trening modelu GBT, normy ruchu |
| streaming/ | Spark Structured Streaming: okna 5 min, ryzyko pogodowe + szansa opoznienia z modelu |
| anomalies/ | reguly: krazenie, drugi krag, spadek odlotow |
| agent/ | asystent dyzurnego: narzedzia tylko do odczytu, LLM opcjonalny |
| dashboard/ | Streamlit: 4 wizualizacje i pole pytania do agenta |
| storage/ | zapis alertow, anomalii i pogody z Kafki do PostgreSQL (historia) |
| common/ | wspolny odczyt danych (PostgreSQL, Kafka lub demo) |
| docs/ | MODEL.md (wyniki modelu), ZARZAD.md (opis bez zargonu), MEDALLION.md (warstwy bronze/silver/gold) |

## Klaster (n1-n5, instalacja bezposrednia, bez Dockera)

- n1: HDFS NameNode, Spark Master, Kafka 1, tu uruchamiamy trening i strumien
- n2, n3: DataNode, Spark Worker, Kafka 2 i 3
- n4: DataNode, Spark Worker, producent i detektor anomalii
- n5: DataNode, Spark Worker, dashboard (port 8501)

## Uruchomienie (kolejnosc)

Repo sklonowane na kazdej maszynie, ktora cos uruchamia (n1, n4, n5). Opcjonalny prefiks `dev-` w kazdym kroku = tematy testowe.

| Krok | Maszyna | Komenda |
| --- | --- | --- |
| 1. klaster | n1 | `infra/start_cluster.sh`, kontrola: `infra/status.sh` |
| 2. tematy | n1 | `infra/create_topics.sh` |
| 3. model (wsad) | n1 | `infra/train.sh` (potrzebuje `data/raw/...`, patrz `batch/fetch_data.py`) |
| 4. strumien | n1 | `infra/stream.sh` (w tmux) |
| 5. dane na zywo | n4 | `infra/ingest.sh start` (stop: `stop`, kontrola: `status`) |
| 5b. baza (raz) | n5 | `infra/db_setup.sh` (sudo), potem wpisz `PG_DSN=...` do `.env` na n4 i n5 |
| 6. dashboard | n5 | `infra/dashboard.sh` -> http://n5:8501 |
| 7. warstwy na zywo | n1 | `infra/lake_stream.sh` (w tmux): Kafka -> bronze/silver/gold w HDFS -> PostgreSQL, patrz `docs/MEDALLION.md` |

Bez klastra (demo): `pip install -r requirements.txt && python -m common.demo_data && streamlit run dashboard/app.py`.
Asystent: `python -m agent "Dlaczego ORD ma opoznienia?"`.
Testy: `python -m unittest anomalies.test_detector producers.test_producer anomalies.test_real_data`.

Konfiguracja: `.env` (wzor `.env.example`): Kafka, prefiks tematow, klucze OpenSky i LLM. Nie commitujemy `.env`, danych ani kluczy.

## Uczciwe ograniczenia

- Model: AUC 0,63 (cel 0,70 niedostepny przy cechach znanych w strumieniu), szczegoly w `docs/MODEL.md`.
- Progi anomalii dobrane na krotkim zbiorze danych, wymagaja przegladu po dluzszej pracy na zywo.
- Asystent tylko czyta dane i doradza, decyzje podejmuje czlowiek.
