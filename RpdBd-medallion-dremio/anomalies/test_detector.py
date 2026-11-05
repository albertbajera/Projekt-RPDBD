"""Testy detektora na danych syntetycznych: python -m unittest anomalies.test_detector"""
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
import detector  # noqa: E402
from detector import BIN, Detector, angle_diff, load_baseline, local_hour_dow  # noqa: E402


def msg(ts, icao="aaa111", airport="ATL", alt=1500.0, track=0.0, vr=0.0, ground=False, v=80.0):
    return {"ts": ts, "airport": airport, "icao24": icao, "callsign": "DAL123", "lat": 0, "lon": 0,
            "baro_altitude": alt, "velocity": v, "true_track": track, "vertical_rate": vr,
            "on_ground": ground}


def feed(det, rows):
    out = []
    for r in rows:
        out += det.process(r)
    return out


def utc(y, mo, d, h=0, mi=0):
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp())


def uniform_shape(value=1.0):
    return {(a, h, d): value for a in ("ATL", "ORD", "DFW", "JFK", "LAX")
            for h in range(24) for d in range(1, 8)}


class Filters(unittest.TestCase):
    def test_general_aviation_is_ignored(self):
        rows = [dict(msg(1000 + 120 * i, track=(90 * i) % 360), callsign="N123AB") for i in range(6)]
        self.assertEqual(feed(Detector(), rows), [])

    def test_missing_callsign_is_ignored(self):
        rows = [dict(msg(1000 + 120 * i, track=(90 * i) % 360), callsign=None) for i in range(6)]
        self.assertEqual(feed(Detector(), rows), [])

    def test_altitude_is_relative_to_airport_elevation(self):
        # ATL lezy na 313 m: 500 m barometrycznie to tylko ~190 m nad lotniskiem (niskie podejscie)
        rows = [msg(1000, airport="ATL", alt=500, vr=-4), msg(1120, airport="ATL", alt=420, vr=-3),
                msg(1240, airport="ATL", alt=600, vr=5)]
        self.assertEqual([a["type"] for a in feed(Detector(), rows)], ["go_around"])
        # ten sam profil nad JFK (4 m) to 400-500 m nad ziemia: nie jest to drugi krag
        rows = [msg(1000, airport="JFK", alt=500, vr=-4), msg(1120, airport="JFK", alt=420, vr=-3),
                msg(1240, airport="JFK", alt=600, vr=5)]
        self.assertEqual(feed(Detector(), rows), [])


class Speed(unittest.TestCase):
    def test_slow_aircraft_do_not_trigger(self):
        rows = [msg(1000 + 120 * i, track=(90 * i) % 360, v=40.0) for i in range(6)]
        self.assertEqual(feed(Detector(), rows), [])
        rows = [msg(1000, alt=600, vr=-4, v=40.0), msg(1120, alt=200, vr=-3, v=40.0), msg(1240, alt=350, vr=5, v=40.0)]
        self.assertEqual(feed(Detector(), rows), [])


class Holding(unittest.TestCase):
    def test_angle_diff_wraps(self):
        self.assertEqual(angle_diff(350, 10), 20)
        self.assertEqual(angle_diff(10, 350), -20)

    def test_circling_is_detected(self):
        rows = [msg(1000 + 120 * i, track=(90 * i) % 360) for i in range(6)]
        out = feed(Detector(), rows)
        self.assertEqual([a["type"] for a in out], ["holding"])
        self.assertEqual(out[0]["icao24"], "aaa111")

    def test_straight_flight_is_not(self):
        rows = [msg(1000 + 120 * i, track=270.0) for i in range(8)]
        self.assertEqual(feed(Detector(), rows), [])

    def test_high_altitude_turns_are_ignored(self):
        rows = [msg(1000 + 120 * i, alt=10000.0, track=(90 * i) % 360) for i in range(6)]
        self.assertEqual(feed(Detector(), rows), [])

    def test_cooldown_gives_one_alarm(self):
        rows = [msg(1000 + 120 * i, track=(90 * i) % 360) for i in range(12)]
        self.assertEqual(len(feed(Detector(), rows)), 1)


class GoAround(unittest.TestCase):
    def test_descent_then_climb_without_landing(self):
        rows = [msg(1000, alt=600, vr=-4), msg(1120, alt=200, vr=-3), msg(1240, alt=350, vr=5, track=0)]
        out = feed(Detector(), rows)
        self.assertEqual([a["type"] for a in out], ["go_around"])

    def test_takeoff_is_not_go_around(self):
        rows = [msg(1000, alt=0, vr=0, ground=True), msg(1120, alt=250, vr=8), msg(1240, alt=900, vr=9)]
        self.assertEqual(feed(Detector(), rows), [])

    def test_landing_then_takeoff_is_not(self):
        rows = [msg(1000, alt=200, vr=-3), msg(1120, alt=0, vr=0, ground=True),
                msg(1240, alt=300, vr=6)]
        self.assertEqual(feed(Detector(), rows), [])


class Weather(unittest.TestCase):
    def _wx(self, ts, gusts=55.0, precip=0.0):
        return {"ts": ts, "airport": "ATL", "temperature_2m": 10.0, "wind_speed_10m": 30.0,
                "wind_gusts_10m": gusts, "precipitation": precip, "cloud_cover": 90}

    def test_weather_is_added_to_detail(self):
        det = Detector()
        det.update_weather(self._wx(900, gusts=55.0, precip=2.5))
        out = feed(det, [msg(1000 + 120 * i, track=(90 * i) % 360) for i in range(6)])
        self.assertIn("porywy 55 km/h", out[0]["detail"])
        self.assertIn("opady 2.5 mm", out[0]["detail"])

    def test_calm_weather_adds_nothing(self):
        det = Detector()
        det.update_weather(self._wx(900, gusts=10.0))
        out = feed(det, [msg(1000 + 120 * i, track=(90 * i) % 360) for i in range(6)])
        self.assertNotIn("pogoda", out[0]["detail"])

    def test_stale_weather_is_ignored(self):
        det = Detector()
        det.update_weather(self._wx(1))
        out = feed(det, [msg(10000 + 120 * i, track=(90 * i) % 360) for i in range(6)])
        self.assertNotIn("pogoda", out[0]["detail"])


class LocalTime(unittest.TestCase):
    def test_local_hour_and_weekday_follow_airport_timezone(self):
        ts = utc(2026, 11, 2, 15, 0)                    # poniedzialek 15:00 UTC
        self.assertEqual(local_hour_dow(ts, "ATL"), (10, 1))      # EST = UTC-5
        self.assertEqual(local_hour_dow(ts, "LAX"), (7, 1))       # PST = UTC-8
        self.assertEqual(local_hour_dow(utc(2026, 11, 2, 3, 0), "LAX"), (19, 7))   # niedziela wieczor


class DepartureDrop(unittest.TestCase):
    START = utc(2026, 11, 2, 15, 0)                     # poniedzialek 10:00 w ATL, kwadrans-wyrownane

    def _window(self, det, start, departures, pool=12):
        """Jedno okno 15 min: w pierwszym cyklu `departures` samolotow zmienia on_ground True -> False."""
        out = []
        for k in range(BIN // 120):
            t = start + 120 * k
            for i in range(pool):                       # poprzednia probka: wszystkie stoja
                det.process(msg(t - 1, icao=f"p{i:02d}", ground=True, alt=0))
            for i in range(pool):
                ground = not (k == 0 and i < departures)
                out += det.process(msg(t, icao=f"p{i:02d}", ground=ground, alt=0 if ground else 400))
        return out

    def _run(self, det, counts, start=None):
        start = start or self.START
        found = []
        for w, c in enumerate(counts):
            found += self._window(det, start + w * BIN, c)
        return found

    def test_drop_is_detected_with_shape(self):
        det = Detector(shape=uniform_shape())
        found = self._run(det, [4] * 12 + [0, 4])
        drops = [a for a in found if a["type"] == "departure_drop"]
        self.assertEqual(len(drops), 1, found)
        self.assertIsNone(drops[0]["icao24"])
        self.assertEqual(set(drops[0]), {"ts", "airport", "type", "icao24", "detail"})

    def test_no_alarm_without_enough_history(self):
        det = Detector(shape=uniform_shape())
        self.assertEqual(self._run(det, [0] * 5), [])

    def test_gap_in_data_discards_window(self):
        det = Detector(shape=uniform_shape())
        found = self._run(det, [4] * 12)
        # producent stal 3 godziny i wrocil w srodku okna: to okno jest niepelne, wiec bez alarmu
        found += self._window(det, self.START + 24 * BIN + 400, 0)
        self.assertEqual([a for a in found if a["type"] == "departure_drop"], [])

    def test_night_lull_is_not_a_drop(self):
        # B: noca (godziny 0-5) ok. 2% sredniej, w dzien 1.0. Noc z zerem odlotow to norma.
        shape = {(a, h, d): (0.02 if h < 6 else 1.0) for a in ("ATL",) for h in range(24) for d in range(1, 8)}
        det = Detector(shape=shape)
        evening = utc(2026, 11, 3, 0, 0)               # 19:00 w ATL (poniedzialek)
        found = self._run(det, [4] * 12, start=evening)
        night = utc(2026, 11, 3, 6, 0)                 # 01:00 w ATL: pora z kształtem < MIN_SHAPE
        found += self._run(det, [0] * 6, start=night)
        self.assertEqual([a for a in found if a["type"] == "departure_drop"], [])

    def test_shape_scales_expectation(self):
        # godzina 10 ma kształt 2.0, godzina 11 ma 0.5: 8 odlotow o 10 i 2 odloty o 11 to ta sama norma
        shape = {("ATL", h, 1): (2.0 if h == 10 else 0.5 if h == 11 else 1.0) for h in range(24)}
        det = Detector(shape=shape)
        found = self._run(det, [8] * 5)                # 10:00-11:15 lokalnie? 5 okien po 15 min: 10:00-11:15
        found += self._run(det, [8] * 4 + [2] * 6, start=self.START + 5 * BIN)
        self.assertEqual([a for a in found if a["type"] == "departure_drop"], [])

    def test_without_baseline_needs_same_hour_history(self):
        det = Detector()                                # bez norm B: ocenia tylko wzgledem tej samej godziny
        found = self._run(det, [4] * 12 + [0, 4])
        self.assertEqual([a for a in found if a["type"] == "departure_drop"], [])
        with mock.patch.object(detector, "MIN_SAME_HOUR", 3):
            det = Detector()
            found = self._run(det, [4] * 6 + [0, 4])   # 6 okien = 1,5 godziny; zero w drugiej godzinie
        self.assertEqual([a for a in found if a["type"] == "departure_drop"], [])   # drugiej godziny nie znamy
        with mock.patch.object(detector, "MIN_SAME_HOUR", 3):
            det = Detector()
            # godz. 10: 4 okna po 4; godz. 11: 3 okna po 4, czwarte z zerem
            found = self._run(det, [4] * 7 + [0, 4])
        self.assertTrue(any(a["type"] == "departure_drop" for a in found), found)

    def test_state_is_not_mixed_between_modes(self):
        det = Detector(shape=uniform_shape())
        self._run(det, [4] * 12)
        saved = det.dump_history()
        self.assertEqual(saved["mode"], "shape")
        self.assertTrue(saved["history"]["ATL"])
        self.assertTrue(Detector(saved, shape=uniform_shape()).history["ATL"])
        self.assertEqual(dict(Detector(saved).history), {})        # inny tryb: historia ignorowana

    def test_legacy_plain_history_loads(self):
        det = Detector({"ATL": [4, 4, 5]})
        self.assertEqual(det.history["ATL"], [[None, 4], [None, 4], [None, 5]])

    def test_drop_detail_contains_weather(self):
        det = Detector(shape=uniform_shape())
        found = self._run(det, [4] * 12)
        det.update_weather({"ts": self.START + 12 * BIN, "airport": "ATL", "wind_gusts_10m": 60.0,
                            "precipitation": 0.0})
        found += self._run(det, [0, 4], start=self.START + 12 * BIN)
        drops = [a for a in found if a["type"] == "departure_drop"]
        self.assertIn("porywy 60 km/h", drops[0]["detail"])


class Baseline(unittest.TestCase):
    ROWS = [{"airport": "ATL", "hour": 10, "day_of_week": 1, "departures": 300, "mean_departure_delay": 12.0},
            {"airport": "ATL", "hour": 3, "day_of_week": 1, "departures": 10, "mean_departure_delay": 5.0},
            {"airport": "JFK", "hour": 10, "day_of_week": 1, "departures": 100, "mean_departure_delay": 9.0}]

    def test_shape_is_relative_to_airport_mean(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "b.jsonl"
            p.write_text("\n".join(json.dumps(r) for r in self.ROWS))
            shape = load_baseline(p)
        self.assertAlmostEqual(shape[("ATL", 10, 1)], 300 / 155)
        self.assertAlmostEqual(shape[("ATL", 3, 1)], 10 / 155)
        self.assertAlmostEqual(shape[("JFK", 10, 1)], 1.0)

    def test_spark_directory_with_part_files(self):
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "part-00000-abc.json").write_text("\n".join(json.dumps(r) for r in self.ROWS[:2]))
            (Path(d) / "part-00001-abc.json").write_text(json.dumps(self.ROWS[2]))
            (Path(d) / "_SUCCESS").write_text("")
            (Path(d) / ".part-00000-abc.json.crc").write_text("x")
            self.assertEqual(len(load_baseline(d)), 3)

    def test_json_array_and_empty_file_error(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "b.json"
            p.write_text(json.dumps(self.ROWS))
            self.assertEqual(len(load_baseline(p)), 3)
            e = Path(d) / "empty.jsonl"
            e.write_text("")
            with self.assertRaises(ValueError):
                load_baseline(e)


class FakeMsg:
    def __init__(self, topic, payload, err=None):
        self._t, self._v, self._e = topic, payload, err

    def topic(self):
        return self._t

    def value(self):
        return self._v if isinstance(self._v, (bytes, str)) else json.dumps(self._v)

    def error(self):
        return self._e


class FakeConsumer:
    def __init__(self, msgs):
        self.msgs, self.subscribed, self.closed = list(msgs), None, False

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, _t):
        return self.msgs.pop(0) if self.msgs else None

    def close(self):
        self.closed = True


class FakeProducer:
    def __init__(self):
        self.sent, self.flushed = [], False

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.sent.append((topic, key, json.loads(value)))

    def poll(self, _t):
        pass

    def flush(self, _t):
        self.flushed = True


class KafkaMode(unittest.TestCase):
    def _run(self, msgs, prefix="dev-", **kw):
        cons, prodcr = FakeConsumer(msgs), FakeProducer()
        with tempfile.TemporaryDirectory() as d:
            state = str(Path(d) / "state.json")
            detector.run_kafka(state, prefix=prefix, consumer=cons, producer=prodcr, max_idle=2, **kw)
            saved = Path(state).exists()
        return cons, prodcr, saved

    def test_reads_both_topics_and_publishes_anomalies(self):
        wx = FakeMsg("dev-weather.current", {"ts": 900, "airport": "ATL", "wind_gusts_10m": 55.0, "precipitation": 0.0})
        states = [FakeMsg("dev-opensky.states", msg(1000 + 120 * i, track=(90 * i) % 360)) for i in range(6)]
        cons, prodcr, saved = self._run([wx, *states])
        self.assertEqual(cons.subscribed, ["dev-opensky.states", "dev-weather.current"])
        self.assertEqual([(t, k) for t, k, _ in prodcr.sent], [("dev-anomalies", "ATL")])
        self.assertIn("porywy", prodcr.sent[0][2]["detail"])
        self.assertTrue(prodcr.flushed and cons.closed and saved)

    def test_empty_prefix_uses_production_topics(self):
        states = [FakeMsg("opensky.states", msg(1000 + 120 * i, track=(90 * i) % 360)) for i in range(6)]
        cons, prodcr, _ = self._run(states, prefix="")
        self.assertEqual(cons.subscribed, ["opensky.states", "weather.current"])
        self.assertEqual(prodcr.sent[0][0], "anomalies")

    def test_bad_messages_do_not_stop_the_detector(self):
        bad = [FakeMsg("dev-opensky.states", b"nie json"), FakeMsg("dev-opensky.states", None),
               FakeMsg("dev-opensky.states", {}, err="blad brokera")]
        good = [FakeMsg("dev-opensky.states", msg(1000 + 120 * i, track=(90 * i) % 360)) for i in range(6)]
        _, prodcr, _ = self._run(bad[:1] + [bad[2]] + good)
        self.assertEqual(len(prodcr.sent), 1)


class FileMode(unittest.TestCase):
    def test_file_mode_with_weather_and_baseline(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            (d / "s.jsonl").write_text("\n".join(json.dumps(msg(1000 + 120 * i, track=(90 * i) % 360)) for i in range(6)))
            (d / "w.jsonl").write_text(json.dumps({"ts": 900, "airport": "ATL", "wind_gusts_10m": 70.0, "precipitation": 0.0}))
            (d / "b.jsonl").write_text(json.dumps({"airport": "ATL", "hour": 10, "day_of_week": 1, "departures": 5}))
            found = detector.run_file(d / "s.jsonl", d / "out" / "a.jsonl", str(d / "state.json"),
                                      weather=d / "w.jsonl", baseline=d / "b.jsonl")
            written = [json.loads(l) for l in (d / "out" / "a.jsonl").read_text().splitlines()]
        self.assertEqual(found, written)
        self.assertIn("porywy 70 km/h", found[0]["detail"])


if __name__ == "__main__":
    unittest.main()
