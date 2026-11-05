"""Testy na zebranych danych (out/): zgodnosc ze schematami i wykrywanie wstrzyknietych anomalii
wsrod prawdziwego ruchu. Pomijane, gdy brak out/opensky.jsonl.
    python -m unittest anomalies.test_real_data
"""
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).parent))
from detector import Detector  # noqa: E402

DATA = ROOT / "out" / "opensky.jsonl"
WEATHER = ROOT / "out" / "weather.jsonl"
SCHEMAS = json.loads((ROOT / "contracts" / "schemas.json").read_text())
TYPES = {"ts": int, "airport": str, "icao24": str, "callsign": str, "lat": float, "lon": float,
         "baro_altitude": float, "velocity": float, "true_track": float, "vertical_rate": float,
         "on_ground": bool}
NULLABLE = {"callsign", "lat", "lon", "baro_altitude", "velocity", "true_track", "vertical_rate"}


def load(p):
    with open(p) as f:
        return [json.loads(l) for l in f if l.strip()]


@unittest.skipUnless(DATA.exists(), "brak out/opensky.jsonl")
class RealData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = sorted(load(DATA), key=lambda r: r["ts"])

    def test_opensky_rows_match_contract(self):
        for r in self.rows:
            self.assertEqual(set(r), set(SCHEMAS["opensky.states"]), r)
            self.assertIn(r["airport"], {"ATL", "ORD", "DFW", "JFK", "LAX"})
            self.assertIsInstance(r["ts"], int)
            self.assertIsInstance(r["on_ground"], bool)
            for k, t in TYPES.items():
                v = r[k]
                if v is None:
                    self.assertIn(k, NULLABLE, f"{k} nie moze byc null: {r}")
                elif t is float:
                    self.assertIsInstance(v, (int, float), (k, r))
                else:
                    self.assertIsInstance(v, t, (k, r))

    @unittest.skipUnless(WEATHER.exists(), "brak out/weather.jsonl")
    def test_weather_rows_match_contract(self):
        w = load(WEATHER)
        self.assertGreater(len(w), 0)
        for r in w:
            self.assertEqual(set(r), set(SCHEMAS["weather.current"]), r)
            for k in ("temperature_2m", "wind_speed_10m", "wind_gusts_10m", "precipitation", "cloud_cover"):
                self.assertIsInstance(r[k], (int, float), (k, r))

    def test_all_five_airports_present(self):
        self.assertEqual({r["airport"] for r in self.rows}, {"ATL", "ORD", "DFW", "JFK", "LAX"})

    def test_no_false_alarms_on_real_traffic(self):
        det, found = Detector(), []
        for r in self.rows:
            found += det.process(r)
        self.assertEqual(found, [], found)

    def test_output_matches_anomalies_contract(self):
        r = self._inject()
        det, found = Detector(), []
        for m in r:
            found += det.process(m)
        self.assertTrue(found)
        for a in found:
            self.assertEqual(set(a), set(SCHEMAS["anomalies"]), a)

    def _inject(self):
        """Prawdziwy ruch + 2 sztuczne samoloty: jeden krazy nad ATL, drugi odchodzi na drugi krag nad LAX."""
        t0 = self.rows[0]["ts"]
        fake = []
        for i in range(6):                                       # 10 min, probka co 120 s, zakret 90 stopni
            fake.append({"ts": t0 + 120 * i + 7, "airport": "ATL", "icao24": "zz0001", "callsign": "DAL999",
                         "lat": 33.6, "lon": -84.4, "baro_altitude": 1500.0, "velocity": 85.0,
                         "true_track": float((90 * i) % 360), "vertical_rate": 0.0, "on_ground": False})
        for i, (alt, vr) in enumerate([(500.0, -4.0), (110.0, -3.0), (260.0, 6.0)]):
            fake.append({"ts": t0 + 120 * i + 9, "airport": "LAX", "icao24": "zz0002", "callsign": "UAL777",
                         "lat": 33.9, "lon": -118.4, "baro_altitude": 38.0 + alt, "velocity": 75.0,
                         "true_track": 250.0, "vertical_rate": vr, "on_ground": False})
        return sorted(self.rows + fake, key=lambda r: r["ts"])

    def test_injected_anomalies_found_in_real_traffic_and_nothing_else(self):
        det, found = Detector(), []
        for m in self._inject():
            found += det.process(m)
        got = sorted((a["type"], a["icao24"]) for a in found)
        self.assertEqual(got, [("go_around", "zz0002"), ("holding", "zz0001")])


if __name__ == "__main__":
    unittest.main()
