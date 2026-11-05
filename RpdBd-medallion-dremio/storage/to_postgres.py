"""Konsument Kafki zapisujacy alerts, anomalies i weather.current do PostgreSQL.

  PG_DSN=postgresql://rpdbd:haslo@n5:5432/rpdbd python storage/to_postgres.py [--prefix dev-]
Offsety zatwierdza dopiero po udanym zapisie, wiec po awarii nic nie ginie (duplikaty pomija baza).
"""
import argparse
import json
import os
import signal
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import data as _data  # noqa: E402,F401  (ladowanie .env)
from common import db  # noqa: E402


def run(prefix: str, consumer=None, conn=None, max_idle=None) -> int:
    if consumer is None:
        from confluent_kafka import Consumer
        consumer = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "n1:9092,n2:9092,n3:9092"),
                             "group.id": prefix + "pg-sink", "enable.auto.commit": False,
                             "auto.offset.reset": "earliest"})
    if conn is None:
        conn = db.connect()
        db.init_schema(conn)
    names = {prefix + t: t for t in db.TABLES}
    consumer.subscribe(list(names))
    stop = {"now": False}
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, lambda *_: stop.update(now=True))
        except ValueError:
            pass

    def flush(buf):
        n = 0
        for topic, rows in buf.items():
            n += db.insert(conn, topic, rows)
        if buf:
            consumer.commit(asynchronous=False)
        buf.clear()
        return n

    buf, last, total, idle = defaultdict(list), time.time(), 0, 0
    try:
        while not stop["now"]:
            m = consumer.poll(1.0)
            if m is None:
                idle += 1
                if max_idle is not None and idle >= max_idle:
                    break
            elif not m.error():
                idle = 0
                try:
                    v = json.loads(m.value())
                except (TypeError, ValueError):
                    v = None
                if isinstance(v, dict):
                    buf[names[m.topic()]].append(v)
            if buf and (m is None or time.time() - last > 2 or sum(map(len, buf.values())) >= 200):
                total += flush(buf)
                last = time.time()
    finally:
        total += flush(buf)
        consumer.close()
        conn.close()
    return total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default=os.getenv("TOPIC_PREFIX", ""))
    a = ap.parse_args()
    if not db.enabled():
        sys.exit("brak PG_DSN (patrz .env.example)")
    print(f"zapis do PostgreSQL: tematy z prefiksem '{a.prefix}'", flush=True)
    run(a.prefix)


if __name__ == "__main__":
    main()
