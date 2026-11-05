"""Wsad, normy ruchu i model opóźnienia odlotu. .

Próbka na laptopie, zanim wstanie klaster:

    python batch/train.py --local --sample 50000

Na n1, po bramce 1, bez dodatkowych argumentów (ścieżki HDFS z instrukcji):

    spark-submit --master spark://n1:7077 --total-executor-cores 12 \\
      --executor-cores 2 --executor-memory 8g --driver-memory 2g batch/train.py

albo po prostu: bash batch/cluster_train.sh
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def prepare_local_env() -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    tools = ROOT / ".tools"
    jdk_root = tools / "jdk"
    if jdk_root.exists():
        homes = [p for p in jdk_root.iterdir() if (p / "bin" / "java.exe").exists()]
        if homes:
            os.environ["JAVA_HOME"] = str(homes[0])
            os.environ["PATH"] = str(homes[0] / "bin") + os.pathsep + os.environ.get("PATH", "")
    hadoop = tools / "hadoop"
    if (hadoop / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(hadoop)
        os.environ["hadoop.home.dir"] = str(hadoop)
        os.environ["PATH"] = str(hadoop / "bin") + os.pathsep + os.environ.get("PATH", "")


prepare_local_env()

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(ROOT))
from common import lake  # noqa: E402

# warstwy medallion na HDFS: bronze (surowe) -> silver (polaczone) -> gold (produkty), patrz common/lake.py
HDFS_PATHS = {
    "flights": f"{lake.BRONZE_FLIGHTS}/flights.csv",
    "weather": lake.BRONZE_WEATHER,
    "curated": lake.SILVER_FLIGHTS_WEATHER,
    "baseline": lake.GOLD_BASELINE,
    "gold_delay": lake.GOLD_DELAY_BY_AIRPORT_MONTH,
    "model": lake.MODEL,
    "metrics": lake.METRICS,
}

KAGGLE_COLUMNS = {
    "YEAR": ("YEAR", "year", "Year"),
    "MONTH": ("MONTH", "month", "Month"),
    "DAY": ("DAY", "day", "Day", "DAY_OF_MONTH", "dayofmonth"),
    "DAY_OF_WEEK": ("DAY_OF_WEEK", "day_of_week", "DayOfWeek"),
    "ORIGIN_AIRPORT": ("ORIGIN_AIRPORT", "origin_airport", "ORIGIN", "Origin"),
    "SCHEDULED_DEPARTURE": (
        "SCHEDULED_DEPARTURE",
        "scheduled_departure",
        "CRS_DEP_TIME",
        "crs_dep_time",
    ),
    "DEPARTURE_DELAY": ("DEPARTURE_DELAY", "departure_delay", "DEP_DELAY", "dep_delay"),
    "CANCELLED": ("CANCELLED", "cancelled", "CANCELED"),
}


# podzial czasowy z instrukcji: trening styczen-wrzesien, test pazdziernik-grudzien.
# contracts/features.json moze go nadpisac kluczem "split" (w zamrozonej wersji go nie ma)
DEFAULT_SPLIT = {"train_months": list(range(1, 10)), "test_months": [10, 11, 12]}


def split_months(features: dict) -> tuple[list[int], list[int]]:
    split = features.get("split") or DEFAULT_SPLIT
    train = [int(m) for m in split.get("train_months", DEFAULT_SPLIT["train_months"])]
    test = [int(m) for m in split.get("test_months", DEFAULT_SPLIT["test_months"])]
    if set(train) & set(test):
        raise SystemExit(f"miesiace treningu i testu sie pokrywaja: {sorted(set(train) & set(test))}")
    return train, test


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trening modelu opóźnień")
    parser.add_argument("--local", action="store_true", help="Spark local[*] na laptopie")
    parser.add_argument("--sample", type=int, default=0, help="Liczba wierszy po joinie, 0 = całość")
    parser.add_argument("--flights", default="")
    parser.add_argument("--weather-dir", default="")
    parser.add_argument("--curated", default="")
    parser.add_argument("--baseline", default="")
    parser.add_argument("--model", default="")
    parser.add_argument("--features", default=str(ROOT / "contracts" / "features.json"))
    parser.add_argument("--airports", default=str(ROOT / "contracts" / "airports.json"))
    parser.add_argument("--metrics", default="")
    parser.add_argument("--max-iter", type=int, default=30)
    return parser.parse_args()


def is_remote(path: str) -> bool:
    return path.startswith(("hdfs:", "/data/", "/lake/", "/models", "/dev/", "/checkpoints"))


def to_spark_path(path: str) -> str:
    if is_remote(path) or path.startswith("file:"):
        return path
    return Path(path).resolve().as_uri()


def load_contract(features_path: str, airports_path: str) -> tuple[dict, list[str], dict[str, str]]:
    features = json.loads(Path(features_path).read_text(encoding="utf-8"))
    names = []
    for item in features["features"]:
        names.append(item["name"] if isinstance(item, dict) else item)
    expected = [
        "airport",
        "hour",
        "day_of_week",
        "month",
        "temperature_2m",
        "wind_speed_10m",
        "wind_gusts_10m",
        "precipitation",
        "cloud_cover",
    ]
    if names != expected:
        raise SystemExit(f"contracts/features.json ma inną kolejność niż zamrożona: {names}")
    airports = json.loads(Path(airports_path).read_text(encoding="utf-8"))
    rows = airports if isinstance(airports, list) else airports["airports"]
    id_to_iata = {str(item["bts_id"]): item["iata"] for item in rows}
    return features, names, id_to_iata


def spark_session(local: bool) -> SparkSession:
    builder = SparkSession.builder.appName("delay-gbt-train")
    if local:
        builder = (
            builder.master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        )
    return builder.getOrCreate()


def hdfs_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
    return fs.exists(hadoop_path)


def resolve_flights(spark: SparkSession, local: bool, given: str) -> str:
    if given:
        return given
    if local:
        folder = ROOT / "data" / "raw" / "flights"
        for name in ("flights.csv", "flights.parquet"):
            candidate = folder / name
            if candidate.exists():
                return str(candidate)
        raise SystemExit(f"Brak flights.csv albo flights.parquet w {folder}. Uruchom batch/fetch_data.py.")
    for candidate in (
        f"{lake.BRONZE_FLIGHTS}/flights.csv",
        f"{lake.BRONZE_FLIGHTS}/flights.parquet",
    ):
        if hdfs_exists(spark, candidate):
            return candidate
    raise SystemExit(f"Na HDFS nie ma {lake.BRONZE_FLIGHTS}/flights.csv ani flights.parquet.")


def rename_kaggle(df: DataFrame) -> DataFrame:
    available = set(df.columns)
    for target, aliases in KAGGLE_COLUMNS.items():
        if target in available:
            continue
        source = next((name for name in aliases if name in available), None)
        if source is None:
            raise SystemExit(f"Brak kolumny {target}. Są: {sorted(available)}")
        df = df.withColumnRenamed(source, target)
    if "YEAR" not in df.columns and "FL_DATE" in df.columns:
        df = (
            df.withColumn("FL_DATE", F.to_date("FL_DATE"))
            .withColumn("YEAR", F.year("FL_DATE"))
            .withColumn("MONTH", F.month("FL_DATE"))
            .withColumn("DAY", F.dayofmonth("FL_DATE"))
        )
    return df


def read_flights(spark: SparkSession, path: str) -> DataFrame:
    reader = spark.read
    if path.endswith(".parquet"):
        df = reader.parquet(to_spark_path(path) if not is_remote(path) else path)
    else:
        df = reader.option("header", True).option("inferSchema", False).csv(
            to_spark_path(path) if not is_remote(path) else path
        )
    return rename_kaggle(df)


def map_airports(df: DataFrame, id_to_iata: dict[str, str]) -> DataFrame:
    pairs = []
    for numeric_id, iata in id_to_iata.items():
        pairs.extend((F.lit(numeric_id), F.lit(iata)))
    mapping = F.create_map(*pairs)
    origin = F.col("ORIGIN_AIRPORT").cast("string")
    numeric = origin.rlike("^[0-9]+$")
    return df.withColumn("origin_raw", origin).withColumn(
        "airport",
        F.when(~numeric, origin).otherwise(mapping[origin]),
    )


WEATHER_FIELDS = [
    "airport",
    "year",
    "month",
    "day",
    "hour",
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_gusts_10m",
    "cloud_cover",
]


def weather_rows(blobs: list[tuple[str, str]]) -> list[dict]:
    rows: list[dict] = []
    for path, text in blobs:
        if not path.endswith(".json"):
            continue
        name = path.rstrip("/").split("/")[-1].split("\\")[-1]
        airport = name.split(".")[0].split("_", 1)[1]
        hourly = json.loads(text)["hourly"]
        for index, stamp in enumerate(hourly["time"]):
            date, clock = stamp.split("T")
            year, month, day = (int(part) for part in date.split("-"))
            row = {
                "airport": airport,
                "year": year,
                "month": month,
                "day": day,
                "hour": int(clock.split(":")[0]),
            }
            for key in WEATHER_FIELDS[5:]:
                row[key] = hourly[key][index]
            rows.append(row)
    if not rows:
        raise SystemExit("Brak godzin w plikach weather_*.json")
    return rows


def weather_csv_text(rows: list[dict]) -> str:
    lines = [",".join(WEATHER_FIELDS)]
    for row in rows:
        lines.append(",".join("" if row[key] is None else str(row[key]) for key in WEATHER_FIELDS))
    return "\n".join(lines) + "\n"


def write_hdfs_text(spark: SparkSession, path: str, text: str) -> None:
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
    target = jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(target):
        fs.delete(target, True)
    stream = fs.create(target, True)
    writer = jvm.java.io.OutputStreamWriter(stream, "UTF-8")
    writer.write(text)
    writer.close()


def read_weather_csv(spark: SparkSession, path: str) -> DataFrame:
    source = path if is_remote(path) else to_spark_path(path)
    weather = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(source)
    )
    return weather.dropDuplicates(["airport", "year", "month", "day", "hour"])


def load_weather(spark: SparkSession, weather_dir: str) -> DataFrame:
    if not is_remote(weather_dir):
        folder = Path(weather_dir)
        csv_path = folder / "weather_hourly.csv"
        if not csv_path.exists():
            blobs = [
                (str(path), path.read_text(encoding="utf-8"))
                for path in sorted(folder.glob("weather_*.json"))
            ]
            csv_path.write_text(weather_csv_text(weather_rows(blobs)), encoding="utf-8")
        return read_weather_csv(spark, str(csv_path))
    csv_path = weather_dir.rstrip("/") + "/weather_hourly.csv"
    if hdfs_exists(spark, csv_path):
        return read_weather_csv(spark, csv_path)
    blobs = spark.sparkContext.wholeTextFiles(weather_dir).collect()
    write_hdfs_text(spark, csv_path, weather_csv_text(weather_rows(blobs)))
    return read_weather_csv(spark, csv_path)


def build_frame(flights: DataFrame, weather: DataFrame, iatas: list[str]) -> tuple[DataFrame, dict]:
    numeric = F.col("origin_raw").rlike("^[0-9]+$")
    kept = flights.filter(F.col("airport").isin(iatas))
    stats = kept.agg(
        F.count("*").alias("rows_five_airports"),
        F.sum(F.when(numeric, 1).otherwise(0)).alias("numeric_origin_rows"),
    ).collect()[0]
    scheduled = F.col("SCHEDULED_DEPARTURE").cast("double")
    hour = F.floor(scheduled / F.lit(100.0)).cast("int")
    labelled = (
        kept.withColumn("hour", hour)
        .withColumn("year", F.col("YEAR").cast("int"))
        .withColumn("month", F.col("MONTH").cast("int"))
        .withColumn("day", F.col("DAY").cast("int"))
        .withColumn("day_of_week", F.col("DAY_OF_WEEK").cast("int"))
        .withColumn("departure_delay", F.col("DEPARTURE_DELAY").cast("double"))
        .withColumn("cancelled", F.col("CANCELLED").cast("double"))
        .filter(F.col("cancelled") == 0)
        .filter(F.col("departure_delay").isNotNull())
        .filter((F.col("hour") >= 0) & (F.col("hour") <= 23))
        .withColumn("delayed", (F.col("departure_delay") > F.lit(15)).cast("int"))
    )
    joined = labelled.join(weather, ["airport", "year", "month", "day", "hour"], "inner")
    joined = joined.dropna(
        subset=[
            "temperature_2m",
            "wind_speed_10m",
            "wind_gusts_10m",
            "precipitation",
            "cloud_cover",
        ]
    )
    profile = {
        "rows_five_airports": int(stats["rows_five_airports"]),
        "numeric_origin_rows": int(stats["numeric_origin_rows"] or 0),
    }
    return joined, profile


def class_weights(train: DataFrame) -> tuple[DataFrame, dict]:
    counts = train.groupBy("delayed").count().collect()
    by_label = {int(row["delayed"]): int(row["count"]) for row in counts}
    negative = by_label.get(0, 0)
    positive = by_label.get(1, 0)
    total = negative + positive
    info = {
        "train_negative": negative,
        "train_positive": positive,
        "positive_rate": (positive / total) if total else 0.0,
    }
    if positive == 0 or negative == 0:
        return train.withColumn("weight", F.lit(1.0)), info
    weight_positive = total / (2.0 * positive)
    weight_negative = total / (2.0 * negative)
    weighted = train.withColumn(
        "weight",
        F.when(F.col("delayed") == 1, F.lit(weight_positive)).otherwise(F.lit(weight_negative)),
    )
    info["weight_positive"] = weight_positive
    info["weight_negative"] = weight_negative
    return weighted, info


def fit_model(train: DataFrame, feature_names: list[str], max_iter: int):
    indexer = StringIndexer(inputCol="airport", outputCol="airport_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["airport_idx"], outputCols=["airport_oh"], handleInvalid="keep")
    assembler = VectorAssembler(
        inputCols=["airport_oh", *feature_names[1:]],
        outputCol="features",
        handleInvalid="skip",
    )
    gbt = GBTClassifier(
        labelCol="delayed",
        featuresCol="features",
        maxIter=max_iter,
        maxDepth=5,
        seed=42,
    )
    if gbt.hasParam("weightCol"):
        gbt = gbt.setWeightCol("weight")
        used_weights = True
    else:
        used_weights = False
    pipeline = Pipeline(stages=[indexer, encoder, assembler, gbt])
    return pipeline.fit(train), used_weights


def positive_f1(predictions: DataFrame) -> dict:
    rows = predictions.select("delayed", "prediction").groupBy("delayed", "prediction").count().collect()
    table = {(int(row["delayed"]), int(row["prediction"])): int(row["count"]) for row in rows}
    tp = table.get((1, 1), 0)
    fp = table.get((0, 1), 0)
    fn = table.get((1, 0), 0)
    tn = table.get((0, 0), 0)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision_positive": precision,
        "recall_positive": recall,
        "f1_positive": f1,
    }


def write_metrics(path: str, payload: dict, spark: SparkSession) -> None:
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    print(text)
    if is_remote(path):
        write_hdfs_text(spark, path, text)
        return
    dest = Path(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(text, encoding="utf-8")


def main() -> None:
    args = parse_args()
    features, feature_names, id_to_iata = load_contract(args.features, args.airports)
    iatas = list(dict.fromkeys(id_to_iata.values()))
    spark = spark_session(args.local)
    spark.sparkContext.setLogLevel("WARN")

    if args.local and args.sample:
        curated = args.curated or str(ROOT / "data" / "sample" / "curated")
        baseline = args.baseline or str(ROOT / "data" / "sample" / "baseline")
        model_path = args.model or str(ROOT / "model" / "delay_gbt_sample")
        metrics_path = args.metrics or str(ROOT / "data" / "metrics.json")
        weather_dir = args.weather_dir or str(ROOT / "data" / "raw" / "weather")
    elif args.local:
        curated = args.curated or str(ROOT / "data" / "curated" / "flights_weather")
        baseline = args.baseline or str(ROOT / "data" / "curated" / "baseline")
        model_path = args.model or str(ROOT / "model" / "delay_gbt")
        metrics_path = args.metrics or str(ROOT / "data" / "metrics.json")
        weather_dir = args.weather_dir or str(ROOT / "data" / "raw" / "weather")
    else:
        curated = args.curated or HDFS_PATHS["curated"]
        baseline = args.baseline or HDFS_PATHS["baseline"]
        model_path = args.model or HDFS_PATHS["model"]
        metrics_path = args.metrics or HDFS_PATHS["metrics"]
        weather_dir = args.weather_dir or HDFS_PATHS["weather"]

    flights_path = resolve_flights(spark, args.local, args.flights)
    flights = map_airports(read_flights(spark, flights_path), id_to_iata)
    weather = load_weather(spark, weather_dir)
    joined, profile = build_frame(flights, weather, iatas)

    if args.sample:
        total = joined.count()
        if total == 0:
            raise SystemExit("Join lotów z pogodą jest pusty.")
        fraction = min(1.0, args.sample / total)
        fractions = {month: fraction for month in range(1, 13)}
        joined = joined.sampleBy("month", fractions, seed=42)

    month_counts = {
        int(row["month"]): int(row["count"])
        for row in joined.groupBy("month").count().collect()
    }
    train_months, test_months = split_months(features)
    missing_test = [month for month in test_months if month_counts.get(month, 0) == 0]
    if missing_test:
        raise SystemExit(
            "Test październik–grudzień jest pusty w miesiącach "
            f"{missing_test}. Sprawdź mapowanie numerycznych kodów lotnisk. "
            f"Rozkład miesięcy: {month_counts}"
        )

    train = joined.filter(F.col("month").isin(train_months))
    test = joined.filter(F.col("month").isin(test_months))
    train, weight_info = class_weights(train)
    model, used_weights = fit_model(train, feature_names, args.max_iter)
    predictions = model.transform(test)
    auc = BinaryClassificationEvaluator(
        labelCol="delayed",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    ).evaluate(predictions)
    scores = positive_f1(predictions)

    curated_cols = ["year", "day", *feature_names, "delayed"]
    out = joined.select(*curated_cols)
    out.write.mode("overwrite").parquet(to_spark_path(curated) if not is_remote(curated) else curated)
    baseline_df = joined.groupBy("airport", "hour", "day_of_week").agg(
        F.count("*").alias("departures"),
        F.avg("departure_delay").alias("mean_departure_delay"),
    )
    baseline_writer = baseline_df.coalesce(1).write.mode("overwrite")
    baseline_target = to_spark_path(baseline) if not is_remote(baseline) else baseline
    baseline_writer.json(baseline_target)

    # gold: agregat pod raport (opoznienia per lotnisko i miesiac); tylko na HDFS, lokalnie niepotrzebny
    if not args.local:
        delay_by_month = joined.groupBy("airport", "month").agg(
            F.count("*").alias("departures"),
            F.avg("departure_delay").alias("mean_departure_delay"),
            F.avg("delayed").alias("delayed_share"),
        )
        delay_by_month.write.mode("overwrite").parquet(HDFS_PATHS["gold_delay"])

    model_target = to_spark_path(model_path) if not is_remote(model_path) else model_path
    model.write().overwrite().save(model_target)

    payload = {
        "flights": flights_path,
        "sample": args.sample or None,
        "rows_five_airports": profile["rows_five_airports"],
        "numeric_origin_rows": profile["numeric_origin_rows"],
        "month_counts": month_counts,
        "auc": auc,
        "used_weight_col": used_weights,
        "model": model_path,
        "curated": curated,
        "baseline": baseline,
        "feature_order": feature_names,
        **weight_info,
        **scores,
    }
    write_metrics(metrics_path, payload, spark)
    if auc <= 0.5:
        print("AUC nie jest wyższe od 0,5.", file=sys.stderr)
        spark.stop()
        raise SystemExit(1)
    spark.stop()


if __name__ == "__main__":
    main()
