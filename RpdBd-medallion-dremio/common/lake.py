"""Sciezki warstw medallion (bronze / silver / gold) na HDFS.

Jedno zrodlo prawdy dla skryptow wsadowych i dokumentacji. Kafka, PostgreSQL i dashboard
to sciezka operacyjna (serving) i nie naleza do jeziora. Model zostaje w /models/delay_gbt,
bo czyta go strumien (infra/stream.sh).
"""

LAKE = "/lake"

BRONZE = f"{LAKE}/bronze"
SILVER = f"{LAKE}/silver"
GOLD = f"{LAKE}/gold"

BRONZE_FLIGHTS = f"{BRONZE}/flights"        # loty DOT 2015 (csv/parquet), airports, airlines
BRONZE_WEATHER = f"{BRONZE}/weather"        # pogoda archiwalna Open-Meteo (json + weather_hourly.csv)

SILVER_FLIGHTS_WEATHER = f"{SILVER}/flights_weather"   # loty polaczone z pogoda, oczyszczone (parquet)

GOLD_BASELINE = f"{GOLD}/baseline"                       # normy ruchu godzina x dzien tygodnia (json)
GOLD_DELAY_BY_AIRPORT_MONTH = f"{GOLD}/delay_by_airport_month"   # agregat pod raport (parquet)

MODEL = "/models/delay_gbt"
METRICS = "/data/metrics/metrics.json"

LAYERS = {"bronze": BRONZE, "silver": SILVER, "gold": GOLD}

# warstwy na zywo (infra/lake_stream.sh): Kafka -> bronze -> silver -> gold; osobny katalog "live" obok wsadu
BRONZE_LIVE = f"{BRONZE}/live"     # surowe wiadomosci z Kafki 1:1 (key, value, topic, offset, kafka_time)
SILVER_LIVE = f"{SILVER}/live"     # sparsowane, zwalidowane, bez duplikatow
GOLD_LIVE = f"{GOLD}/live"         # alerty, anomalie, pogoda (wzbogacone) i aktywnosc lotnisk w oknach 5 min
