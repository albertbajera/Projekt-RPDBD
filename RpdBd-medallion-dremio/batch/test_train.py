"""testy wsadu B na malych danych w spark local: python -m unittest batch.test_train
wymagaja pyspark i javy (jak na n1), bez nich sa pomijane."""
import json
import sys
import unittest
import warnings
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "batch"))

try:
    import train
    from pyspark.sql import SparkSession
    from pyspark.sql import Row
except Exception as e:  # brak pyspark albo javy
    train = None
    SKIP = str(e)
else:
    SKIP = ""

SPARK = None
warnings.filterwarnings("ignore", category=ResourceWarning)  # gniazda py4j


def spark():
    global SPARK
    if SPARK is None:
        SPARK = (SparkSession.builder.master("local[2]").appName("test-b")
                 .config("spark.sql.shuffle.partitions", "2")
                 .config("spark.ui.enabled", "false")
                 .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        SPARK.sparkContext.setLogLevel("ERROR")
    return SPARK


def flight(month=1, day=5, dow=1, origin="ATL", sched="1530", delay="20", cancelled="0"):
    return Row(YEAR="2015", MONTH=str(month), DAY=str(day), DAY_OF_WEEK=str(dow), ORIGIN_AIRPORT=origin,
               SCHEDULED_DEPARTURE=sched, DEPARTURE_DELAY=delay, CANCELLED=cancelled)


def weather(airport="ATL", month=1, day=5, hour=15, gust=30.0):
    return Row(airport=airport, year=2015, month=month, day=day, hour=hour, temperature_2m=10.0,
               precipitation=0.0, wind_speed_10m=12.0, wind_gusts_10m=gust, cloud_cover=50.0)


@unittest.skipIf(train is None, f"brak pyspark: {SKIP}")
class Contract(unittest.TestCase):
    def test_repo_contract_loads(self):
        features, names, ids = train.load_contract(str(ROOT / "contracts" / "features.json"),
                                                   str(ROOT / "contracts" / "airports.json"))
        self.assertEqual(names[0], "airport")
        self.assertEqual(ids["10397"], "ATL")
        self.assertEqual(sorted(ids.values()), ["ATL", "DFW", "JFK", "LAX", "ORD"])

    def test_wrong_feature_order_is_rejected(self):
        bad = ROOT / "batch" / "_test_features.json"
        f = json.loads((ROOT / "contracts" / "features.json").read_text())
        f["features"] = list(reversed(f["features"]))
        bad.write_text(json.dumps(f))
        try:
            with self.assertRaises(SystemExit):
                train.load_contract(str(bad), str(ROOT / "contracts" / "airports.json"))
        finally:
            bad.unlink()

    def test_split_default_when_contract_has_none(self):
        # zamrozony contracts/features.json nie ma klucza split, wczesniej konczylo sie KeyError
        self.assertEqual(train.split_months({}), (list(range(1, 10)), [10, 11, 12]))
        self.assertEqual(train.split_months({"split": {"train_months": [1, 2], "test_months": [3]}}), ([1, 2], [3]))
        with self.assertRaises(SystemExit):
            train.split_months({"split": {"train_months": [1, 2], "test_months": [2]}})


@unittest.skipIf(train is None, f"brak pyspark: {SKIP}")
class Weather(unittest.TestCase):
    def test_archive_json_to_rows(self):
        blob = json.dumps({"hourly": {"time": ["2015-01-01T00:00", "2015-01-01T01:00"],
                                      "temperature_2m": [1.0, 2.0], "precipitation": [0.0, 0.5],
                                      "wind_speed_10m": [5.0, 6.0], "wind_gusts_10m": [9.0, None],
                                      "cloud_cover": [10, 20]}})
        rows = train.weather_rows([("/x/weather_ORD.json", blob), ("/x/readme.txt", "")])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["airport"], "ORD")
        self.assertEqual((rows[1]["month"], rows[1]["day"], rows[1]["hour"]), (1, 1, 1))
        self.assertIn(",,", train.weather_csv_text(rows).splitlines()[2])   # None -> pusty

    def test_no_rows_is_an_error(self):
        with self.assertRaises(SystemExit):
            train.weather_rows([])


@unittest.skipIf(train is None, f"brak pyspark: {SKIP}")
class Frame(unittest.TestCase):
    def test_airport_codes_numeric_and_iata(self):
        df = spark().createDataFrame([flight(origin="ATL"), flight(origin="13930"), flight(origin="99999")])
        out = [r["airport"] for r in train.map_airports(df, {"13930": "ORD"}).collect()]
        self.assertEqual(out, ["ATL", "ORD", None])

    def test_labels_filters_and_join(self):
        flights = spark().createDataFrame([
            flight(sched="1530", delay="20"),               # opozniony, godzina 15
            flight(sched="1545", delay="15"),               # dokladnie 15 min = nie opozniony
            flight(sched="1550", delay="40", cancelled="1"),  # odwolany, pomijany
            flight(sched="1555", delay=None),               # brak opoznienia, pomijany
            flight(sched="0905", delay="5"),                # brak pogody o 9, odpada w join
            flight(origin="BOS", sched="1500", delay="99"),  # spoza 5 lotnisk
        ])
        flights = train.map_airports(flights, {"10397": "ATL"})
        wx = spark().createDataFrame([weather(hour=15)])
        joined, profile = train.build_frame(flights, wx, ["ATL", "ORD", "DFW", "JFK", "LAX"])
        rows = sorted((r["hour"], r["departure_delay"], r["delayed"]) for r in joined.collect())
        self.assertEqual(rows, [(15, 15.0, 0), (15, 20.0, 1)])
        self.assertEqual(profile["rows_five_airports"], 5)

    def test_class_weights_balance_classes(self):
        df = spark().createDataFrame([Row(delayed=1)] + [Row(delayed=0)] * 3)
        w, info = train.class_weights(df)
        total = {r["delayed"]: r["s"] for r in w.groupBy("delayed").agg({"weight": "sum"})
                 .withColumnRenamed("sum(weight)", "s").collect()}
        self.assertAlmostEqual(total[0], total[1])
        self.assertAlmostEqual(info["positive_rate"], 0.25)

    def test_positive_f1(self):
        df = spark().createDataFrame([Row(delayed=1, prediction=1.0), Row(delayed=1, prediction=0.0),
                                      Row(delayed=0, prediction=1.0), Row(delayed=0, prediction=0.0)])
        s = train.positive_f1(df)
        self.assertEqual((s["tp"], s["fp"], s["fn"], s["tn"]), (1, 1, 1, 1))
        self.assertAlmostEqual(s["f1_positive"], 0.5)


def tiny_model():
    """maly model w ukladzie z train.fit_model (uzywa go tez test strumienia A)"""
    import random
    rnd = random.Random(1)
    rows = []
    for i in range(400):
        gust = rnd.uniform(0, 80)
        rows.append(Row(airport=rnd.choice(["ATL", "ORD", "DFW", "JFK", "LAX"]), hour=rnd.randint(0, 23),
                        day_of_week=rnd.randint(1, 7), month=rnd.randint(1, 9),
                        temperature_2m=rnd.uniform(-5, 30), wind_speed_10m=gust * 0.6, wind_gusts_10m=gust,
                        precipitation=rnd.uniform(0, 4), cloud_cover=rnd.uniform(0, 100),
                        delayed=int(gust + rnd.uniform(-20, 20) > 50)))
    df, _ = train.class_weights(spark().createDataFrame(rows))
    names = json.loads((ROOT / "contracts" / "features.json").read_text())["features"]
    model, _ = train.fit_model(df, names, max_iter=5)
    return model


@unittest.skipIf(train is None, f"brak pyspark: {SKIP}")
class Model(unittest.TestCase):
    def test_model_learns_wind_signal(self):
        from pyspark.ml.evaluation import BinaryClassificationEvaluator
        model = tiny_model()
        test = spark().createDataFrame([Row(airport="ATL", hour=12, day_of_week=3, month=10, temperature_2m=10.0,
                                            wind_speed_10m=g * 0.6, wind_gusts_10m=float(g), precipitation=0.0,
                                            cloud_cover=50.0, delayed=int(g > 50)) for g in range(0, 80, 2)])
        auc = BinaryClassificationEvaluator(labelCol="delayed").evaluate(model.transform(test))
        self.assertGreater(auc, 0.7)            # porywy niosa sygnal, model go lapie


if __name__ == "__main__":
    unittest.main()
