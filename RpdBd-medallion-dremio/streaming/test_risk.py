"""testy strumienia A w spark local, bez kafki: python -m unittest streaming.test_risk
wymagaja pyspark i javy (jak na n1), bez nich sa pomijane."""
import json
import sys
import unittest
import warnings
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "streaming"))
sys.path.insert(0, str(ROOT / "producers"))
sys.path.insert(0, str(ROOT / "batch"))
sys.path.insert(0, str(ROOT))

try:
    import risk
    from pyspark.sql import functions as F
    from batch import test_train
except Exception as e:
    risk = None
    SKIP = str(e)
else:
    SKIP = ""

warnings.filterwarnings("ignore", category=ResourceWarning)


def ts(y, mo, d, h, mi=0):
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp())


def spark():
    return test_train.spark()


def scored_rows(rows):
    df = spark().createDataFrame(rows, "airport string, gust double, precip double, cloud double")
    return {r["airport"]: r for r in risk.add_risk(df).collect()}


@unittest.skipIf(risk is None, f"brak pyspark: {SKIP}")
class Rules(unittest.TestCase):
    def test_risk_levels_and_reason(self):
        r = scored_rows([("A", 60.0, 5.0, 100.0), ("B", 0.0, 0.0, 10.0), ("C", 30.0, 5.0, 0.0),
                         ("D", 6.0, 0.0, 100.0), ("E", None, None, None), ("F", 120.0, 0.0, 0.0)])
        self.assertEqual((r["A"]["risk"], r["A"]["level"], r["A"]["reason"]), (1.0, "high", "porywy wiatru"))
        self.assertEqual((r["B"]["risk"], r["B"]["level"], r["B"]["reason"]), (0.02, "low", "spokojnie"))
        self.assertEqual((r["C"]["risk"], r["C"]["level"], r["C"]["reason"]), (0.55, "medium", "opady"))
        self.assertEqual((r["D"]["risk"], r["D"]["reason"]), (0.25, "spokojnie"))
        self.assertEqual(r["E"]["risk"], 0.0)                      # brak pogody liczony jak spokoj
        self.assertEqual(r["F"]["risk"], 0.5)                      # skladnik wiatru ograniczony do 0,5

    def test_thresholds_match_dashboard(self):
        r = scored_rows([("M", 39.6, 0.0, 0.0), ("H", 60.0, 2.7, 0.0), ("U", 60.0, 2.5, 0.0)])
        self.assertEqual((r["M"]["risk"], r["M"]["level"]), (0.33, "medium"))   # prog 0,33
        self.assertEqual((r["H"]["risk"], r["H"]["level"]), (0.66, "high"))     # prog 0,66 (po zaokragleniu)
        self.assertEqual((r["U"]["risk"], r["U"]["level"]), (0.65, "medium"))


@unittest.skipIf(risk is None, f"brak pyspark: {SKIP}")
class TimeFeatures(unittest.TestCase):
    def features(self, airport, when):
        df = spark().createDataFrame([(airport, when)], "airport string, t long").select(
            "airport", F.struct(F.col("t").cast("timestamp").alias("start")).alias("window"))
        r = risk.add_time_features(df).collect()[0]
        return r["hour"], r["day_of_week"], r["month"]

    def test_local_hour_and_dot_weekday(self):
        # poniedzialek 6 lipca 2026, 12:00 UTC (czas letni)
        self.assertEqual(self.features("ATL", ts(2026, 7, 6, 12)), (8, 1, 7))
        self.assertEqual(self.features("ORD", ts(2026, 7, 6, 12)), (7, 1, 7))
        self.assertEqual(self.features("LAX", ts(2026, 7, 6, 12)), (5, 1, 7))

    def test_sunday_is_7_and_winter_time(self):
        # niedziela 4 stycznia 2026, 12:00 UTC (czas zimowy)
        self.assertEqual(self.features("JFK", ts(2026, 1, 4, 12)), (7, 7, 1))

    def test_local_midnight_changes_day_and_month(self):
        # 1 sierpnia 03:00 UTC to jeszcze 31 lipca 23:00 w Nowym Jorku (piatek)
        self.assertEqual(self.features("ATL", ts(2026, 8, 1, 3)), (23, 5, 7))


def c_messages():
    """wiadomosci w formacie producenta C (producer.parse_states) jako json, jak na temacie"""
    import producer
    sample = json.loads((ROOT / "producers" / "tests_data" / "opensky_sample.json").read_text())
    return [json.dumps(m) for m in producer.parse_states("ATL", sample)]


@unittest.skipIf(risk is None, f"brak pyspark: {SKIP}")
class Aggregation(unittest.TestCase):
    def frames(self, states, weather):
        s = spark().createDataFrame([(v,) for v in states], "value string")
        w = spark().createDataFrame([(v,) for v in weather], "value string")
        return risk.parse_json(s, risk.OPENSKY_SCHEMA), risk.parse_json(w, risk.WEATHER_SCHEMA)

    def test_producer_c_messages_feed_the_stream(self):
        base = ts(2026, 7, 6, 12)
        states = []
        for k, m in enumerate(c_messages()):
            for t in (base + 10, base + 130):           # dwie probki co 120 s jak producent
                d = json.loads(m)
                d["ts"] = t
                states.append(json.dumps(d))
        weather = [json.dumps({"ts": base + 200, "airport": "ATL", "temperature_2m": 21.5, "wind_speed_10m": 20.0,
                               "wind_gusts_10m": 66, "precipitation": 2.5, "cloud_cover": 100})]
        s, w = self.frames(states, weather)
        self.assertEqual(s.where(F.col("icao24").isNull()).count(), 0)   # schemat C pasuje do A
        rows = risk.add_risk(risk.aggregate(s, w)).collect()
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual((r["airport"], r["aircraft"]), ("ATL", 2))       # 2 rozne samoloty z probki C
        self.assertEqual((r["gust"], r["precip"], r["cloud"]), (66.0, 2.5, 100.0))  # int z api tez dziala
        self.assertEqual((r["level"], r["reason"]), ("high", "porywy wiatru"))

    def test_window_without_weather_gives_no_alert(self):
        base = ts(2026, 7, 6, 12)
        states = [json.dumps({**json.loads(m), "ts": base + 1000}) for m in c_messages()]  # okno 12:15
        weather = [json.dumps({"ts": base + 10, "airport": "ATL", "temperature_2m": 1.0, "wind_speed_10m": 1.0,
                               "wind_gusts_10m": 5.0, "precipitation": 0.0, "cloud_cover": 0})]
        s, w = self.frames(states, weather)
        rows = risk.aggregate(s, w).collect()
        self.assertEqual([(r["window"]["start"].hour, r["window"]["start"].minute) for r in rows], [(12, 0)])


@unittest.skipIf(risk is None, f"brak pyspark: {SKIP}")
class ModelScoring(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = test_train.tiny_model()   # ten sam uklad pipeline co batch/train.py

    def batch(self, rows):
        df = spark().createDataFrame(rows, "airport string, t long, aircraft long, temp double, wind double, "
                                           "gust double, precip double, cloud double")
        df = df.withColumn("window", F.struct(F.col("t").cast("timestamp").alias("start"))).drop("t")
        return risk.add_risk(df)

    def test_delay_prob_from_model_b(self):
        t = ts(2026, 7, 6, 18)
        out = risk.score_batch(self.batch([("ORD", t, 30, 12.0, 40.0, 75.0, 3.0, 100.0),
                                           ("LAX", t, 20, 22.0, 5.0, 8.0, 0.0, 5.0),
                                           ("ATL", t, 25, None, 10.0, 20.0, 0.0, 30.0)]), self.model)
        r = {x["airport"]: x for x in out.collect()}
        self.assertTrue(0 <= r["ORD"]["delay_prob"] <= 1)
        self.assertGreater(r["ORD"]["delay_prob"], r["LAX"]["delay_prob"])   # silny wiatr -> wyzej
        self.assertIsNone(r["ATL"]["delay_prob"])                             # brak temperatury
        self.assertEqual(r["ATL"]["level"], "low")                            # reguly dzialaja dalej

    def test_alert_json_matches_contract(self):
        t = ts(2026, 7, 6, 18)
        scored = risk.score_batch(self.batch([("ORD", t, 30, 12.0, 40.0, 75.0, 3.0, 100.0)]), self.model)
        row = risk.to_alert_json(scored).collect()[0]
        msg = json.loads(row["value"])
        schema = json.loads((ROOT / "contracts" / "schemas.json").read_text())["alerts"]
        self.assertEqual(row["key"], "ORD")
        self.assertEqual(set(msg) - {"delay_prob"}, set(schema))
        plain = json.loads(risk.to_alert_json(self.batch([("ORD", t, 30, 1.0, 1.0, 1.0, 0.0, 0.0)])).collect()[0]["value"])
        self.assertNotIn("delay_prob", plain)                                 # v1 bez modelu


if __name__ == "__main__":
    unittest.main()
