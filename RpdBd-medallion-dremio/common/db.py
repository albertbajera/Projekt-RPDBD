"""PostgreSQL: historia alertow, anomalii i pogody. Polaczenie z PG_DSN, np. postgresql://rpdbd:haslo@n5:5432/rpdbd"""
from __future__ import annotations

import os

from common import data as _data  # noqa: F401  (wczytuje .env z PG_DSN)

TABLES = {  # temat Kafki -> (tabela, kolumny)
    "alerts": ("alerts", ["ts", "airport", "risk", "level", "reason", "aircraft", "delay_prob"]),
    "anomalies": ("anomalies", ["ts", "airport", "type", "icao24", "detail"]),
    "weather.current": ("weather", ["ts", "airport", "temperature_2m", "wind_speed_10m",
                                    "wind_gusts_10m", "precipitation", "cloud_cover"]),
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
  ts BIGINT NOT NULL, airport TEXT NOT NULL, risk DOUBLE PRECISION, level TEXT, reason TEXT,
  aircraft INT, delay_prob DOUBLE PRECISION, PRIMARY KEY (ts, airport));
CREATE TABLE IF NOT EXISTS anomalies (
  ts BIGINT NOT NULL, airport TEXT NOT NULL, type TEXT NOT NULL, icao24 TEXT, detail TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS anomalies_uq ON anomalies (ts, airport, type, COALESCE(icao24, ''));
CREATE TABLE IF NOT EXISTS weather (
  ts BIGINT NOT NULL, airport TEXT NOT NULL, temperature_2m DOUBLE PRECISION, wind_speed_10m DOUBLE PRECISION,
  wind_gusts_10m DOUBLE PRECISION, precipitation DOUBLE PRECISION, cloud_cover DOUBLE PRECISION,
  PRIMARY KEY (ts, airport));
"""


def enabled() -> bool:
    return bool(os.environ.get("PG_DSN", "").strip())


def connect():
    import psycopg2
    return psycopg2.connect(os.environ["PG_DSN"])


def init_schema(conn) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(SCHEMA)


def insert(conn, topic: str, rows: list[dict]) -> int:
    """Wstawia wiadomosci z tematu do tabeli; duplikaty (ten sam klucz) sa pomijane."""
    from psycopg2.extras import execute_values
    table, cols = TABLES[topic]
    values = [tuple(r.get(c) for c in cols) for r in rows if r.get("ts") is not None and r.get("airport")]
    if not values:
        return 0
    with conn, conn.cursor() as cur:
        execute_values(cur, f"INSERT INTO {table} ({','.join(cols)}) VALUES %s ON CONFLICT DO NOTHING", values)
    return len(values)


def fetch(topic: str, hours: float = 24, cap: int = 50000) -> list[dict]:
    """Wiersze z ostatnich `hours` godzin (wzgledem najnowszego wpisu), rosnaco po ts."""
    table, cols = TABLES[topic]
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"SELECT {','.join(cols)} FROM {table} "
                        f"WHERE ts >= (SELECT COALESCE(MAX(ts), 0) FROM {table}) - %s "
                        f"ORDER BY ts DESC LIMIT %s", (int(hours * 3600), cap))
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows[::-1]
