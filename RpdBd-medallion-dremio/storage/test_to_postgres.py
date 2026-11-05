"""Test zapisu do PostgreSQL bez bazy i Kafki: python -m unittest storage.test_to_postgres"""
import json
import unittest
from unittest import mock

from common import db
from storage import to_postgres


class Msg:
    def __init__(self, topic, value):
        self._t, self._v = topic, value

    def error(self):
        return None

    def topic(self):
        return self._t

    def value(self):
        return self._v


class FakeConsumer:
    def __init__(self, msgs):
        self.msgs, self.commits, self.closed, self.topics = list(msgs), 0, False, None

    def subscribe(self, topics):
        self.topics = topics

    def poll(self, _t):
        return self.msgs.pop(0) if self.msgs else None

    def commit(self, asynchronous=False):
        self.commits += 1

    def close(self):
        self.closed = True


class Sink(unittest.TestCase):
    def test_rows_go_to_matching_tables_and_offsets_are_committed(self):
        a = {"ts": 1, "airport": "ATL", "risk": 0.5, "level": "medium", "reason": "opady", "aircraft": 3}
        w = {"ts": 1, "airport": "ATL", "wind_gusts_10m": 30.0}
        msgs = [Msg("dev-alerts", json.dumps(a).encode()), Msg("dev-weather.current", json.dumps(w).encode()),
                Msg("dev-alerts", b"nie json")]
        seen, conn, cons = [], mock.Mock(), FakeConsumer(msgs)
        with mock.patch.object(db, "insert", lambda c, t, rows: seen.append((t, len(rows))) or len(rows)):
            total = to_postgres.run("dev-", consumer=cons, conn=conn, max_idle=2)
        self.assertEqual(sorted(seen), [("alerts", 1), ("weather.current", 1)])
        self.assertEqual(total, 2)
        self.assertEqual(sorted(cons.topics), ["dev-alerts", "dev-anomalies", "dev-weather.current"])
        self.assertGreaterEqual(cons.commits, 1)
        self.assertTrue(cons.closed)


if __name__ == "__main__":
    unittest.main()
