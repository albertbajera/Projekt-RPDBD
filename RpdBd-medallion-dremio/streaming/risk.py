"""strumien: ryzyko zaklocen z regul na pogodzie, opcjonalnie z modelem od B.

czyta tematy opensky.states i weather.current, liczy w oknach 5 min per lotnisko
i zapisuje wynik do tematu alerts (schemat w contracts/schemas.json).
bez --model: tylko reguly (risk, level, reason). pole ts to czas emisji alertu, nie okna.
z --model: dodatkowo delay_prob, czyli prawdopodobienstwo opoznienia odlotu >15 min z modelu
GBT od B (contracts/features.json). risk i level zostaja z regul, delay_prob jest polem dodatkowym.

uruchomienie na n1: infra/stream.sh [prefix]  (z modelem /models/delay_gbt; bez modelu: MODEL= infra/stream.sh)
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                               StructField, StructType)

BOOTSTRAP = "n1:9092,n2:9092,n3:9092"

OPENSKY_SCHEMA = StructType([
    StructField("ts", LongType()),
    StructField("airport", StringType()),
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("on_ground", BooleanType()),
])

WEATHER_SCHEMA = StructType([
    StructField("ts", LongType()),
    StructField("airport", StringType()),
    StructField("temperature_2m", DoubleType()),
    StructField("wind_speed_10m", DoubleType()),
    StructField("wind_gusts_10m", DoubleType()),
    StructField("precipitation", DoubleType()),
    StructField("cloud_cover", DoubleType()),
])

# progi do dostrojenia po obejrzeniu prawdziwych danych
GUST_MAX = 60.0     # km/h, od tej wartosci skladnik wiatru jest pelny
PRECIP_MAX = 5.0    # mm
CLOUD_MAX = 100.0   # procent
W_GUST, W_PRECIP, W_CLOUD = 0.5, 0.3, 0.2

# model B byl uczony na godzinie lokalnej lotniska i dniu tygodnia jak w DOT (1 = poniedzialek)
AIRPORT_TZ = {
    "ATL": "America/New_York", "JFK": "America/New_York",
    "ORD": "America/Chicago", "DFW": "America/Chicago",
    "LAX": "America/Los_Angeles",
}
MODEL_FEATURES = ["airport", "hour", "day_of_week", "month", "temperature_2m",
                  "wind_speed_10m", "wind_gusts_10m", "precipitation", "cloud_cover"]


def read_topic(spark: SparkSession, topic: str, schema: StructType) -> DataFrame:
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", topic)
           .option("startingOffsets", "latest")
           .load())
    return parse_json(raw, schema)


def parse_json(raw: DataFrame, schema: StructType) -> DataFrame:
    """kolumna value (json z tematu) -> kolumny ze schematu plus event_time"""
    parsed = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("m"))
    return parsed.select("m.*").withColumn("event_time", F.col("ts").cast("timestamp"))


def aggregate(states: DataFrame, weather: DataFrame) -> DataFrame:
    """okna 5 min per lotnisko: liczba samolotow i pogoda. okna bez pogody sa pomijane."""
    # jeden strumien z obu zrodel, brakujace kolumny wypelnia null
    events = (states.select("event_time", "airport", "icao24")
              .unionByName(weather.select("event_time", "airport", "temperature_2m",
                                          "wind_speed_10m", "wind_gusts_10m",
                                          "precipitation", "cloud_cover"),
                           allowMissingColumns=True))
    return (events.withWatermark("event_time", "15 minutes")
            .groupBy(F.window("event_time", "5 minutes"), "airport")
            .agg(F.approx_count_distinct("icao24").alias("aircraft"),
                 F.avg("temperature_2m").alias("temp"),
                 F.avg("wind_speed_10m").alias("wind"),
                 F.max("wind_gusts_10m").alias("gust"),
                 F.max("precipitation").alias("precip"),
                 F.max("cloud_cover").alias("cloud"))
            .where(F.col("gust").isNotNull()))  # bez pogody w oknie nie liczymy ryzyka


def add_risk(df: DataFrame) -> DataFrame:
    """df ma kolumny gust, precip, cloud. dodaje risk (0-1), level i reason."""
    gust = F.coalesce(F.col("gust"), F.lit(0.0))
    precip = F.coalesce(F.col("precip"), F.lit(0.0))
    cloud = F.coalesce(F.col("cloud"), F.lit(0.0))

    p_gust = F.least(gust / GUST_MAX, F.lit(1.0)) * W_GUST
    p_precip = F.least(precip / PRECIP_MAX, F.lit(1.0)) * W_PRECIP
    p_cloud = F.least(cloud / CLOUD_MAX, F.lit(1.0)) * W_CLOUD

    out = df.withColumn("risk", F.round(p_gust + p_precip + p_cloud, 2))
    out = out.withColumn(
        "level",
        F.when(F.col("risk") >= 0.66, "high").when(F.col("risk") >= 0.33, "medium").otherwise("low"),
    )
    return out.withColumn(
        "reason",
        F.when(F.col("level") == "low", "spokojnie")
        .when((p_gust >= p_precip) & (p_gust >= p_cloud), "porywy wiatru")
        .when(p_precip >= p_cloud, "opady")
        .otherwise("zachmurzenie"),
    )


def add_time_features(df: DataFrame) -> DataFrame:
    """dodaje hour, day_of_week, month w czasie lokalnym lotniska z poczatku okna.
    wymaga spark.sql.session.timeZone = UTC, inaczej godzina wyjdzie przesunieta."""
    pairs = [x for kv in AIRPORT_TZ.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    tz = F.coalesce(F.create_map(*pairs)[F.col("airport")], F.lit("UTC"))
    local = F.from_utc_timestamp(F.col("window.start"), tz)
    # dayofweek w sparku: 1 = niedziela, w DOT: 1 = poniedzialek
    return (df.withColumn("hour", F.hour(local))
            .withColumn("day_of_week", ((F.dayofweek(local) + 5) % 7) + 1)
            .withColumn("month", F.month(local)))


def vector_to_array(col: str):
    """to samo co pyspark.ml.functions.vector_to_array, ale bez importu pandas
    (na n1 jest tylko numpy, a ten modul importuje pandas na poziomie pliku)."""
    from pyspark import SparkContext
    from pyspark.sql.column import Column, _to_java_column
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.org.apache.spark.ml.functions.vector_to_array(_to_java_column(col), "float64"))


def score_batch(batch: DataFrame, model) -> DataFrame:
    """batch ma kolumny z agregacji okna plus risk, level, reason. zwraca ten sam zestaw wierszy
    z dodatkowa kolumna delay_prob (null, gdy brakuje ktorejs cechy modelu).
    bez join, bo samo-zlaczenie wyniku modelu z wejsciem konfliktuje atrybuty w foreachBatch."""
    from functools import reduce

    feat = (add_time_features(batch)
            .withColumn("temperature_2m", F.col("temp"))
            .withColumn("wind_speed_10m", F.col("wind"))
            .withColumn("wind_gusts_10m", F.col("gust"))
            .withColumn("precipitation", F.col("precip"))
            .withColumn("cloud_cover", F.col("cloud")))
    complete = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in MODEL_FEATURES])
    feat = feat.withColumn("features_ok", complete)
    # brakujace wartosci wypelniamy tylko po to, zeby model nie wyrzucil wiersza
    for c in MODEL_FEATURES[1:]:
        feat = feat.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))
    pred = model.transform(feat)
    return pred.withColumn(
        "delay_prob",
        F.when(F.col("features_ok"), F.round(vector_to_array("probability")[1], 3)))


def to_alert_json(df: DataFrame) -> DataFrame:
    fields = ["airport", "risk", "level", "reason", "aircraft"]
    if "delay_prob" in df.columns:
        fields.append("delay_prob")
    return df.select(
        F.col("airport").alias("key"),
        F.to_json(F.struct(F.unix_timestamp(F.current_timestamp()).alias("ts"), *fields)).alias("value"),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default="", help="prefiks tematow, np. dev- do testow")
    ap.add_argument("--checkpoint", default="hdfs://n1:9000/checkpoints/streaming-v1")
    ap.add_argument("--trigger", default="30 seconds")
    ap.add_argument("--model", default="", help="sciezka do PipelineModel od B, np. hdfs://n1:9000/models/delay_gbt")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("risk-v2" if args.model else "risk-v1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    states = read_topic(spark, args.prefix + "opensky.states", OPENSKY_SCHEMA)
    weather = read_topic(spark, args.prefix + "weather.current", WEATHER_SCHEMA)

    scored = add_risk(aggregate(states, weather))

    if args.model:
        from pyspark.ml import PipelineModel
        model = PipelineModel.load(args.model)

        def write_batch(batch: DataFrame, batch_id: int) -> None:
            if batch.isEmpty():
                return
            cached = batch.cache()
            try:
                out = to_alert_json(score_batch(cached, model))
                (out.write.format("kafka")
                 .option("kafka.bootstrap.servers", BOOTSTRAP)
                 .option("topic", args.prefix + "alerts")
                 .save())
            finally:
                cached.unpersist()  # bez tego kazda mikro-paczka zostaje w pamieci executorow

        query = (scored.writeStream.foreachBatch(write_batch)
                 .option("checkpointLocation", args.checkpoint)
                 .outputMode("update")
                 .trigger(processingTime=args.trigger)
                 .start())
    else:
        query = (to_alert_json(scored).writeStream.format("kafka")
                 .option("kafka.bootstrap.servers", BOOTSTRAP)
                 .option("topic", args.prefix + "alerts")
                 .option("checkpointLocation", args.checkpoint)
                 .outputMode("update")
                 .trigger(processingTime=args.trigger)
                 .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
