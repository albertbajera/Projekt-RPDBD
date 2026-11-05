"""Odczyt tematow Kafki (alerts, anomalies, weather.current) dla dashboardu i agenta.

Bez KAFKA_BOOTSTRAP czyta data/fixtures/*.jsonl (tryb demo, patrz common/demo_data.py).
Nic nie zapisuje do Kafki.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "data" / "fixtures"
AIRPORTS = ("ATL", "ORD", "DFW", "JFK", "LAX")


def _load_dotenv() -> None:
    path = ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()


def _from_fixtures(topic: str) -> list[dict]:
    path = FIXTURES / f"{topic}.jsonl"
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]


def _from_kafka(topic: str, limit: int) -> list[dict]:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
                  "group.id": "dashboard-reader", "enable.auto.commit": False})
    try:
        meta = c.list_topics(topic, timeout=10).topics.get(topic)
        if meta is None or meta.error is not None:
            return []
        parts = []
        for p in meta.partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if hi > lo:
                parts.append((TopicPartition(topic, p, max(lo, hi - limit)), hi))
        if not parts:
            return []
        c.assign([tp for tp, _ in parts])
        left = sum(hi - tp.offset for tp, hi in parts)
        rows, idle = [], 0
        while left > 0 and idle < 5:
            m = c.poll(1.0)
            if m is None:
                idle += 1
                continue
            if m.error():
                continue
            left -= 1
            try:
                v = json.loads(m.value())
            except (ValueError, TypeError):
                continue
            if isinstance(v, dict):
                rows.append(v)
        return rows
    finally:
        c.close()


def read_topic(topic: str, limit: int = 500) -> list[dict]:
    """Zrodlo: PostgreSQL (PG_DSN, ostatnie 24 h), inaczej Kafka (ostatnie ~limit na partycje), inaczej fixtures."""
    if os.environ.get("PG_DSN", "").strip() and topic in ("alerts", "anomalies", "weather.current"):
        from common import db
        return db.fetch(topic)
    if os.environ.get("KAFKA_BOOTSTRAP", "").strip():
        rows = _from_kafka(os.environ.get("TOPIC_PREFIX", "") + topic, limit)
    else:
        rows = _from_fixtures(topic)
    return sorted(rows, key=lambda r: int(r.get("ts") or 0))


def demo_mode() -> bool:
    return not (os.environ.get("KAFKA_BOOTSTRAP", "").strip() or os.environ.get("PG_DSN", "").strip())
