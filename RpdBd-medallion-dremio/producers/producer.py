"""Producent: OpenSky (co 120 s) + Open-Meteo (co 4 min) -> pliki JSONL albo Kafka.

Lokalnie (zapis do out/*.jsonl):   python producers/producer.py
Test bez API:                      python producers/producer.py --replay producers/tests_data/opensky_sample.json --once
Kafka, tematy produkcyjne:         python producers/producer.py --sink kafka
Kafka, tematy testowe dev-*:       python producers/producer.py --sink kafka --prefix dev-
Dane logowania OpenSky: credentials.json w katalogu repo albo .env (patrz .env.example).
"""
import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

for p in (Path(__file__).resolve().parent.parent / ".env", Path(__file__).parent / ".env"):
    if p.exists():
        load_dotenv(p)

AIRPORTS = {
    "ATL": (33.6407, -84.4277),
    "ORD": (41.9742, -87.9073),
    "DFW": (32.8998, -97.0403),
    "JFK": (40.6413, -73.7781),
    "LAX": (33.9416, -118.4085),
}
BOX = 0.5
OPENSKY_URL = "https://opensky-network.org/api/states/all"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
TOKEN_URL = os.getenv(
    "OPENSKY_TOKEN_URL",
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
)
OPENSKY_EVERY = 120
WEATHER_EVERY = 240  # s: krocej niz okno strumienia (5 min), zeby kazde okno mialo pogode
MAX_BACKOFF = 3600
OUT = Path(__file__).resolve().parent.parent / "out"
STOP = {"now": False}


def topic_names(prefix=""):
    """Tematy z contracts/topics.md; prefiks (np. dev-) pozwala testowac obok produkcji."""
    return {"opensky": prefix + "opensky.states", "weather": prefix + "weather.current"}


class Sink:
    """Zapis do plikow JSONL (domyslnie) albo na Kafke."""

    def __init__(self, mode=None, prefix=""):
        self.mode = mode or os.getenv("SINK", "file")
        self.errors = 0
        if self.mode == "kafka":
            from confluent_kafka import Producer
            self.p = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "n1:9092,n2:9092,n3:9092"),
                               "linger.ms": 50})
            self.topics = topic_names(prefix)
        else:
            OUT.mkdir(exist_ok=True)

    def _delivered(self, err, _msg):
        if err:
            self.errors += 1
            print(f"blad dostarczenia do Kafki: {err}", file=sys.stderr, flush=True)

    def send(self, kind, key, msg):
        if self.mode == "kafka":
            payload = json.dumps(msg)
            try:
                self.p.produce(self.topics[kind], key=key, value=payload, on_delivery=self._delivered)
            except BufferError:                       # kolejka pelna: zrob miejsce i sprobuj jeszcze raz
                self.p.poll(1)
                self.p.produce(self.topics[kind], key=key, value=payload, on_delivery=self._delivered)
            self.p.poll(0)
        else:
            with open(OUT / f"{kind}.jsonl", "a") as f:
                f.write(json.dumps(msg) + "\n")

    def flush(self):
        if self.mode == "kafka":
            self.p.flush(10)


class OpenSky:
    def __init__(self):
        self.cid = os.getenv("OPENSKY_CLIENT_ID")
        self.secret = os.getenv("OPENSKY_CLIENT_SECRET")
        if not (self.cid and self.secret):
            root = Path(__file__).resolve().parent.parent
            for f in (root / "credentials.json", Path.home() / "credentials.json"):
                if f.exists():
                    j = json.loads(f.read_text())
                    self.cid = j.get("clientId") or j.get("client_id")
                    self.secret = j.get("clientSecret") or j.get("client_secret")
                    break
        self.token = None
        self.exp = 0

    def headers(self):
        if not (self.cid and self.secret):
            return {}  # anonimowo - tylko do testow
        if time.time() > self.exp - 30:
            r = requests.post(TOKEN_URL, data={
                "grant_type": "client_credentials",
                "client_id": self.cid, "client_secret": self.secret}, timeout=20)
            r.raise_for_status()
            j = r.json()
            self.token = j["access_token"]
            self.exp = time.time() + j.get("expires_in", 300)
        return {"Authorization": f"Bearer {self.token}"}

    def fetch(self, lat, lon):
        params = {"lamin": lat - BOX, "lamax": lat + BOX, "lomin": lon - BOX, "lomax": lon + BOX}
        return requests.get(OPENSKY_URL, params=params, headers=self.headers(), timeout=30)


def retry_after(resp, default):
    """Przy 429 OpenSky podaje w naglowku, ile sekund czekac; w innym razie podwajamy odstep."""
    h = getattr(resp, "headers", None) or {}
    for k in ("X-Rate-Limit-Retry-After-Seconds", "Retry-After"):
        try:
            return min(max(int(float(h[k])), OPENSKY_EVERY), MAX_BACKOFF)
        except (KeyError, ValueError, TypeError):
            continue
    return min(default * 2, MAX_BACKOFF)


def parse_states(airport, data):
    ts = data.get("time") or int(time.time())
    out = []
    for s in data.get("states") or []:
        out.append({
            "ts": int(ts), "airport": airport, "icao24": s[0],
            "callsign": (s[1] or "").strip() or None,
            "lat": s[6], "lon": s[5], "baro_altitude": s[7],
            "velocity": s[9], "true_track": s[10], "vertical_rate": s[11],
            "on_ground": s[8],
        })
    return out


def fetch_weather(airport, lat, lon):
    r = requests.get(WEATHER_URL, params={
        "latitude": lat, "longitude": lon,
        "current": "temperature_2m,precipitation,wind_speed_10m,wind_gusts_10m,cloud_cover",
    }, timeout=20)
    r.raise_for_status()
    c = r.json()["current"]
    return {
        "ts": int(time.time()), "airport": airport,
        "temperature_2m": c["temperature_2m"], "wind_speed_10m": c["wind_speed_10m"],
        "wind_gusts_10m": c["wind_gusts_10m"], "precipitation": c["precipitation"],
        "cloud_cover": c["cloud_cover"],
    }


def _sleep_until(t):
    """Spi do chwili t, ale reaguje na SIGTERM/Ctrl+C co sekunde."""
    while not STOP["now"]:
        left = t - time.time()
        if left <= 0:
            return
        time.sleep(min(left, 1.0))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay", help="plik JSON z zapisana odpowiedzia OpenSky (zamiast API)")
    ap.add_argument("--once", action="store_true", help="jeden cykl i koniec")
    ap.add_argument("--save-raw", action="store_true", help="zapisuj surowe odpowiedzi OpenSky do out/raw/")
    ap.add_argument("--sink", choices=["file", "kafka"], help="gdzie pisac (domyslnie z SINK, a bez niego: file)")
    ap.add_argument("--prefix", default=os.getenv("TOPIC_PREFIX", ""), help="prefiks tematow Kafki, np. dev- (puste = produkcja)")
    args = ap.parse_args()

    STOP["now"] = False
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, lambda *_: STOP.update(now=True))
        except ValueError:                           # nie glowny watek (testy)
            pass

    sink, osky = Sink(args.sink, args.prefix), OpenSky()
    if sink.mode == "kafka":
        print(f"Kafka: {sink.topics}", flush=True)
    interval = OPENSKY_EVERY
    next_sky = next_wx = 0.0
    try:
        while not STOP["now"]:
            now = time.time()
            if now >= next_sky:
                ok = True
                for ap_code, (lat, lon) in AIRPORTS.items():
                    try:
                        if args.replay:
                            data = json.load(open(args.replay))
                        else:
                            r = osky.fetch(lat, lon)
                            if r.status_code == 429:
                                interval = retry_after(r, interval)
                                print(f"429 -> odstep {interval}s", file=sys.stderr, flush=True)
                                ok = False
                                break
                            r.raise_for_status()
                            left = (getattr(r, "headers", None) or {}).get("X-Rate-Limit-Remaining")
                            if left is not None and ap_code == "ATL":
                                print(f"pozostale kredyty OpenSky: {left}", flush=True)
                            data = r.json()
                            if args.save_raw:
                                d = OUT / "raw"
                                d.mkdir(parents=True, exist_ok=True)
                                (d / f"{ap_code}_{int(now)}.json").write_text(json.dumps(data))
                        rows = parse_states(ap_code, data)
                        for m in rows:
                            sink.send("opensky", m["icao24"], m)
                        print(f"{ap_code}: {len(rows)} samolotow", flush=True)
                    except Exception as e:  # blad sieci -> pomijamy lotnisko/cykl
                        print(f"blad OpenSky {ap_code}: {e}", file=sys.stderr, flush=True)
                if ok and interval > OPENSKY_EVERY and not args.replay:
                    interval = max(OPENSKY_EVERY, interval // 2)  # powrot do normalnego odstepu
                next_sky = time.time() + interval
            if now >= next_wx and not args.replay:
                for ap_code, (lat, lon) in AIRPORTS.items():
                    try:
                        m = fetch_weather(ap_code, lat, lon)
                        sink.send("weather", ap_code, m)
                        print(f"{ap_code}: pogoda ok", flush=True)
                    except Exception as e:
                        print(f"blad pogody {ap_code}: {e}", file=sys.stderr, flush=True)
                next_wx = time.time() + WEATHER_EVERY
            sink.flush()
            if args.once:
                break
            _sleep_until(min(next_sky, next_wx if not args.replay else next_sky))
    finally:
        sink.flush()
        if sink.mode == "kafka" and sink.errors:
            print(f"UWAGA: {sink.errors} wiadomosci nie dotarlo do Kafki", file=sys.stderr)


if __name__ == "__main__":
    main()
