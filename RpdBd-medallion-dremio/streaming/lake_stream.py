"""Warstwy medallion na zywo: Kafka -> bronze -> silver -> gold (HDFS), gold -> PostgreSQL (dashboard).

Spark Structured Streaming, osobny job obok streaming/risk.py (ten liczy alerty i nie jest ruszany).

  bronze  surowe wiadomosci z Kafki 1:1 (key, value, topic, partition, offset, kafka_time)
  silver  sparsowane wg schematow z contracts/, tylko poprawne lotniska i czasy, bez duplikatow
  gold    alerts, anomalies, weather (wzbogacone o pola do raportu) oraz airport_activity
          (samoloty na lotnisku w oknach 5 min); alerts/anomalies/weather trafiaja tez do PostgreSQL

Pliki: <root>/bronze|silver|gold/live[-prefiks]/<temat>, parquet, partycje po dacie.
Uruchomienie na n1: infra/lake_stream.sh [prefix]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType  # noqa: E402

from common import lake  # noqa: E402
from streaming.risk import (BOOTSTRAP, GUST_MAX, OPENSKY_SCHEMA, PRECIP_MAX,  # noqa: E402
                            WEATHER_SCHEMA, parse_json)

AIRPORTS = ["ATL", "ORD", "DFW", "JFK", "LAX"]

ALERTS_SCHEMA = StructType([
    StructField("ts", LongType()),
    StructField("airport", StringType()),
    StructField("risk", DoubleType()),
    StructField("level", StringType()),
    StructField("reason", StringType()),
    StructField("aircraft", LongType()),
    StructField("delay_prob", DoubleType()),
])

ANOMALIES_SCHEMA = StructType([
    StructField("ts", LongType()),
    StructField("airport", StringType()),
    StructField("type", StringType()),
    StructField("icao24", StringType()),
    StructField("detail", StringType()),
])

# temat Kafki -> (nazwa katalogu, schemat, klucze deduplikacji)
TOPICS = {
    "opensky.states": ("opensky_states", OPENSKY_SCHEMA, ["icao24", "ts"]),
    "weather.current": ("weather_current", WEATHER_SCHEMA, ["airport", "ts"]),
    "alerts": ("alerts", ALERTS_SCHEMA, ["airport", "ts"]),
    "anomalies": ("anomalies", ANOMALIES_SCHEMA, ["airport", "ts", "type", "icao24"]),
}


def read_raw(spark: SparkSession, topic: str, starting: str) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", starting)
            .option("failOnDataLoss", "false")
            .load())


def bronze_frame(raw: DataFrame) -> DataFrame:
    """surowa wiadomosc bez zadnej interpretacji"""
    return (raw.select(F.col("key").cast("string").alias("key"),
                       F.col("value").cast("string").alias("value"),
                       "topic", "partition", "offset",
                       F.col("timestamp").alias("kafka_time"))
            .withColumn("ingest_date", F.to_date("kafka_time")))


def silver_frame(raw: DataFrame, topic: str) -> DataFrame:
    """parsowanie wg schematu, walidacja i deduplikacja (stan czyszczony watermarkiem 15 min)"""
    _, schema, keys = TOPICS[topic]
    df = parse_json(raw, schema).where(F.col("ts").isNotNull() & F.col("airport").isin(AIRPORTS))
    if topic == "opensky.states":
        df = df.where(F.col("icao24").isNotNull() & F.col("lat").isNotNull() & F.col("lon").isNotNull())
    df = df.withWatermark("event_time", "15 minutes").dropDuplicates([*keys, "event_time"])
    return df.withColumn("ingest_date", F.to_date("event_time"))


def gold_alerts(df: DataFrame) -> DataFrame:
    return df.withColumn("high_risk", F.col("level") == "high")


def gold_anomalies(df: DataFrame) -> DataFrame:
    return df.withColumn("airport_wide", F.col("icao24").isNull())


def gold_weather(df: DataFrame) -> DataFrame:
    severe = (F.col("wind_gusts_10m") >= GUST_MAX) | (F.col("precipitation") >= PRECIP_MAX)
    return df.withColumn("severe", F.coalesce(severe, F.lit(False)))


GOLD = {"alerts": gold_alerts, "anomalies": gold_anomalies, "weather.current": gold_weather}


def airport_activity(states: DataFrame) -> DataFrame:
    """gold: samoloty na lotnisku w oknach 5 min. Watermark (15 min) jest juz ustawiony w silver_frame,
    drugiego nie wolno ustawiac; okno trafia do pliku po jego zamknieciu, ok. 20 min po starcie okna."""
    return (states
            .groupBy(F.window("event_time", "5 minutes"), "airport")
            .agg(F.approx_count_distinct("icao24").alias("aircraft"),
                 F.approx_count_distinct(F.when(~F.col("on_ground"), F.col("icao24"))).alias("airborne"),
                 F.avg("velocity").alias("avg_velocity"),
                 F.avg("baro_altitude").alias("avg_altitude"))
            .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                    "airport", "aircraft", "airborne", "avg_velocity", "avg_altitude")
            .withColumn("ingest_date", F.to_date("window_start")))


def push_postgres(topic: str, gold: DataFrame) -> None:
    """gold -> PostgreSQL; blad bazy nie zatrzymuje jeziora (duplikaty pomija baza)"""
    try:
        from common import db
        cols = db.TABLES[topic][1]
        rows = [r.asDict() for r in gold.select(*cols).collect()]
        conn = db.connect()
        try:
            db.init_schema(conn)
            db.insert(conn, topic, rows)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        print(f"UWAGA: zapis do PostgreSQL ({topic}) nie powiodl sie: {exc}", file=sys.stderr)


def make_batch_writer(topic: str, silver_path: str, gold_path: str | None, to_pg: bool):
    gold_fn = GOLD.get(topic)

    def write(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        cached = batch.cache()
        try:
            cached.write.mode("append").partitionBy("ingest_date").parquet(silver_path)
            if gold_fn is not None and gold_path:
                gold = gold_fn(cached)
                gold.write.mode("append").partitionBy("ingest_date").parquet(gold_path)
                if to_pg:
                    push_postgres(topic, gold)
        finally:
            cached.unpersist()

    return write


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default="", help="prefiks tematow, np. dev- do testow")
    ap.add_argument("--fs", default="hdfs://n1:9000", help="system plikow, lokalnie np. file:///tmp/lake")
    ap.add_argument("--checkpoint", default="", help="domyslnie <fs>/checkpoints/lake-<prefiks>")
    ap.add_argument("--starting", default="earliest", choices=["earliest", "latest"])
    ap.add_argument("--trigger", default="30 seconds")
    ap.add_argument("--no-pg", action="store_true", help="nie zapisuj gold do PostgreSQL")
    args = ap.parse_args()

    live = "live" + (f"-{args.prefix.strip('-')}" if args.prefix else "")
    base = lambda layer: f"{args.fs}{layer.rsplit('/', 1)[0]}/{live}"  # noqa: E731
    ckpt_root = args.checkpoint or f"{args.fs}/checkpoints/lake-{args.prefix.strip('-') or 'prod'}"

    to_pg = False
    if not args.no_pg:
        from common import db
        to_pg = db.enabled()
    print(f"warstwy: {base(lake.BRONZE_LIVE)} | {base(lake.SILVER_LIVE)} | {base(lake.GOLD_LIVE)}; PostgreSQL: {to_pg}")

    spark = (SparkSession.builder.appName("lake-stream")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    for topic, (name, _, _) in TOPICS.items():
        kafka_topic = args.prefix + topic
        bronze_path = f"{base(lake.BRONZE_LIVE)}/{name}"
        silver_path = f"{base(lake.SILVER_LIVE)}/{name}"
        gold_path = f"{base(lake.GOLD_LIVE)}/{name}" if topic in GOLD else None

        raw = read_raw(spark, kafka_topic, args.starting)
        (bronze_frame(raw).writeStream.format("parquet")
         .option("path", bronze_path).option("checkpointLocation", f"{ckpt_root}/{name}-bronze")
         .partitionBy("ingest_date").outputMode("append")
         .trigger(processingTime=args.trigger).queryName(f"bronze_{name}").start())

        silver = silver_frame(read_raw(spark, kafka_topic, args.starting), topic)
        (silver.writeStream.foreachBatch(make_batch_writer(topic, silver_path, gold_path, to_pg))
         .option("checkpointLocation", f"{ckpt_root}/{name}-silver-gold")
         .outputMode("append").trigger(processingTime=args.trigger)
         .queryName(f"silver_gold_{name}").start())

        if topic == "opensky.states":
            states = silver_frame(read_raw(spark, kafka_topic, args.starting), topic)
            (airport_activity(states).writeStream.format("parquet")
             .option("path", f"{base(lake.GOLD_LIVE)}/airport_activity")
             .option("checkpointLocation", f"{ckpt_root}/airport_activity")
             .partitionBy("ingest_date").outputMode("append")
             .trigger(processingTime=args.trigger).queryName("gold_airport_activity").start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
