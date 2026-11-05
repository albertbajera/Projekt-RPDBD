"""Testy producenta bez sieci (odpowiedzi API i Kafka sa symulowane): python -m unittest producers.test_producer"""
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
import producer  # noqa: E402

SAMPLE = Path(__file__).parent / "tests_data" / "opensky_sample.json"
SCHEMAS = json.loads((Path(__file__).parent.parent / "contracts" / "schemas.json").read_text())


class FakeResp:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code = status
        self._p = payload or {}
        self.headers = headers or {}

    def json(self):
        return self._p

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class Parsing(unittest.TestCase):
    def test_states_match_contract_and_handle_nulls(self):
        rows = producer.parse_states("ATL", json.loads(SAMPLE.read_text()))
        self.assertEqual(len(rows), 2)
        for r in rows:
            self.assertEqual(set(r), set(SCHEMAS["opensky.states"]))
        self.assertEqual(rows[0]["callsign"], "DAL123")        # spacje obciete
        self.assertIsNone(rows[1]["callsign"])                  # pusty znak -> null
        self.assertIsNone(rows[1]["baro_altitude"])

    def test_empty_states(self):
        self.assertEqual(producer.parse_states("ATL", {"time": 1, "states": None}), [])

    def test_weather_matches_contract(self):
        payload = {"current": {"temperature_2m": 12.3, "wind_speed_10m": 18.0, "wind_gusts_10m": 34.0,
                               "precipitation": 0.0, "cloud_cover": 40}}
        with mock.patch.object(producer.requests, "get", return_value=FakeResp(200, payload)):
            m = producer.fetch_weather("ATL", 33.6, -84.4)
        self.assertEqual(set(m), set(SCHEMAS["weather.current"]))


class RateLimit(unittest.TestCase):
    def test_retry_after_header_is_honored(self):
        r = FakeResp(429, headers={"X-Rate-Limit-Retry-After-Seconds": "900"})
        self.assertEqual(producer.retry_after(r, 120), 900)

    def test_retry_after_is_capped(self):
        r = FakeResp(429, headers={"Retry-After": "999999"})
        self.assertEqual(producer.retry_after(r, 120), producer.MAX_BACKOFF)

    def test_without_header_interval_doubles(self):
        self.assertEqual(producer.retry_after(FakeResp(429), 120), 240)
        self.assertEqual(producer.retry_after(FakeResp(429), 3000), producer.MAX_BACKOFF)

    def test_garbage_header_falls_back_to_doubling(self):
        self.assertEqual(producer.retry_after(FakeResp(429, headers={"Retry-After": "jutro"}), 120), 240)


class ErrorHandling(unittest.TestCase):
    def _run_once(self, fetch, argv=("producer.py", "--once")):
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(producer, "OUT", Path(d)), \
                mock.patch.object(producer.OpenSky, "fetch", fetch), \
                mock.patch.object(producer.OpenSky, "headers", return_value={}), \
                mock.patch.object(producer, "fetch_weather", mock.Mock(side_effect=OSError("siec"))), \
                mock.patch.object(sys, "argv", list(argv)):
            producer.main()
            return {p.name: p.read_text() for p in Path(d).glob("*.jsonl")}

    def test_429_does_not_crash_and_writes_nothing(self):
        out = self._run_once(mock.Mock(return_value=FakeResp(429)))
        self.assertEqual(out.get("opensky.jsonl", ""), "")

    def test_network_error_skips_cycle_and_survives(self):
        out = self._run_once(mock.Mock(side_effect=OSError("brak sieci")))
        self.assertEqual(out.get("opensky.jsonl", ""), "")

    def test_one_failing_airport_does_not_block_others(self):
        good = FakeResp(200, json.loads(SAMPLE.read_text()))
        calls = {"n": 0}

        def fetch(self_, lat, lon):
            calls["n"] += 1
            if calls["n"] == 2:
                raise OSError("timeout")
            return good

        out = self._run_once(fetch)
        airports = {json.loads(l)["airport"] for l in out["opensky.jsonl"].splitlines()}
        self.assertEqual(len(airports), 4)                      # 5 lotnisk minus to z bledem

    def test_weather_failure_does_not_stop_opensky(self):
        good = FakeResp(200, json.loads(SAMPLE.read_text()))
        out = self._run_once(mock.Mock(return_value=good))
        self.assertGreater(len(out["opensky.jsonl"].splitlines()), 0)


class FakeKafkaProducer:
    instances = []

    def __init__(self, conf):
        self.conf, self.sent, self.flushed, self.fail_once = conf, [], 0, False
        FakeKafkaProducer.instances.append(self)

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.fail_once:
            self.fail_once = False
            raise BufferError("kolejka pelna")
        self.sent.append((topic, key, json.loads(value)))
        if on_delivery:
            on_delivery(None, None)

    def poll(self, _t):
        return 0

    def flush(self, _t):
        self.flushed += 1


def fake_confluent():
    mod = types.ModuleType("confluent_kafka")
    mod.Producer = FakeKafkaProducer
    return mod


class KafkaSink(unittest.TestCase):
    def setUp(self):
        FakeKafkaProducer.instances.clear()

    def _run(self, argv, fetch=None):
        good = FakeResp(200, json.loads(SAMPLE.read_text()))
        weather = {"ts": 1, "airport": "ATL", "temperature_2m": 1.0, "wind_speed_10m": 2.0,
                   "wind_gusts_10m": 3.0, "precipitation": 0.0, "cloud_cover": 4}
        with mock.patch.dict(sys.modules, {"confluent_kafka": fake_confluent()}), \
                mock.patch.object(producer.OpenSky, "fetch", fetch or mock.Mock(return_value=good)), \
                mock.patch.object(producer.OpenSky, "headers", return_value={}), \
                mock.patch.object(producer, "fetch_weather", mock.Mock(side_effect=lambda a, lat, lon: {**weather, "airport": a})), \
                mock.patch.object(sys, "argv", ["producer.py", "--once", *argv]):
            producer.main()
        return FakeKafkaProducer.instances[0]

    def test_prefix_topics_with_keys_from_contract(self):
        p = self._run(["--sink", "kafka", "--prefix", "dev-"])
        topics = {t for t, _, _ in p.sent}
        self.assertEqual(topics, {"dev-opensky.states", "dev-weather.current"})
        for t, key, m in p.sent:
            if t == "dev-opensky.states":
                self.assertEqual(key, m["icao24"])               # klucz opensky.states = icao24
            else:
                self.assertEqual(key, m["airport"])              # klucz weather.current = airport
        self.assertGreaterEqual(p.flushed, 1)

    def test_no_prefix_uses_production_topics(self):
        p = self._run(["--sink", "kafka"])
        self.assertEqual({t for t, _, _ in p.sent}, {"opensky.states", "weather.current"})

    def test_message_counts(self):
        p = self._run(["--sink", "kafka"])
        n_states = sum(1 for t, _, _ in p.sent if t == "opensky.states")
        n_weather = sum(1 for t, _, _ in p.sent if t == "weather.current")
        self.assertEqual((n_states, n_weather), (2 * 5, 5))      # 2 samoloty x 5 lotnisk, 5 x pogoda

    def test_full_queue_is_retried(self):
        sink = None
        with mock.patch.dict(sys.modules, {"confluent_kafka": fake_confluent()}):
            sink = producer.Sink("kafka")
            sink.p.fail_once = True
            sink.send("weather", "ATL", {"airport": "ATL"})
        self.assertEqual(len(sink.p.sent), 1)

    def test_delivery_error_is_counted(self):
        with mock.patch.dict(sys.modules, {"confluent_kafka": fake_confluent()}):
            sink = producer.Sink("kafka")
        sink._delivered("BROKER_DOWN", None)
        self.assertEqual(sink.errors, 1)

    def test_kafka_failure_does_not_crash_other_airports(self):
        # blad OpenSky dla jednego lotniska nie moze zatrzymac wysylki pozostalych na Kafke
        good = FakeResp(200, json.loads(SAMPLE.read_text()))
        calls = {"n": 0}

        def fetch(self_, lat, lon):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("timeout")
            return good

        p = self._run(["--sink", "kafka"], fetch=fetch)
        self.assertEqual(sum(1 for t, _, _ in p.sent if t == "opensky.states"), 2 * 4)


if __name__ == "__main__":
    unittest.main()
