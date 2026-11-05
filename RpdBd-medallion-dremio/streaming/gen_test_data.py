"""generator sztucznych wiadomosci do testow streamingu i dashboardu.

wysyla do tematow <prefix>opensky.states i <prefix>weather.current dla lotnisk
z contracts/airports.json. porywy wiatru rosna i opadaja, zeby ryzyko sie zmienialo.

instalacja (raz, na n1, w osobnym srodowisku):
  python3 -m venv ~/venv-gen && ~/venv-gen/bin/pip install confluent-kafka
uruchomienie:
  ~/venv-gen/bin/python streaming/gen_test_data.py --prefix dev-
"""
import argparse
import json
import math
import random
import time
from pathlib import Path

from confluent_kafka import Producer

BOOTSTRAP = "n1:9092,n2:9092,n3:9092"
AIRPORTS = json.loads((Path(__file__).parent.parent / "contracts" / "airports.json").read_text())


def send(p: Producer, topic: str, key: str, msg: dict) -> None:
    p.produce(topic, key=key, value=json.dumps(msg))
    p.poll(0)


def fake_states(airport: dict, now: int) -> list:
    out = []
    for i in range(random.randint(6, 14)):
        out.append({
            "ts": now, "airport": airport["iata"],
            "icao24": f"{airport['iata'].lower()}{i:03d}",
            "callsign": f"TST{random.randint(100, 999)}",
            "lat": airport["lat"] + random.uniform(-0.3, 0.3),
            "lon": airport["lon"] + random.uniform(-0.3, 0.3),
            "baro_altitude": random.uniform(100, 3000),
            "velocity": random.uniform(60, 200),
            "true_track": random.uniform(0, 360),
            "vertical_rate": random.uniform(-8, 8),
            "on_ground": random.random() < 0.2,
        })
    return out


def fake_weather(airport: dict, now: int, phase: float) -> dict:
    gust = 35 + 30 * math.sin(phase)  # 5-65 km/h
    return {
        "ts": now, "airport": airport["iata"],
        "temperature_2m": round(random.uniform(5, 25), 1),
        "wind_speed_10m": round(max(gust * 0.6, 0), 1),
        "wind_gusts_10m": round(max(gust, 0), 1),
        "precipitation": round(max(random.gauss(1.0, 1.5), 0), 1),
        "cloud_cover": random.randint(0, 100),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default="dev-", help="prefiks tematow, domyslnie testowy")
    ap.add_argument("--interval", type=float, default=5.0, help="sekundy miedzy seriami stanow")
    ap.add_argument("--weather-every", type=int, default=2, help="co ile serii pogoda")
    args = ap.parse_args()

    if args.prefix == "":
        raise SystemExit("pusty prefiks zalalby tematy produkcyjne danymi testowymi, podaj np. dev-")

    p = Producer({"bootstrap.servers": BOOTSTRAP})
    n = 0
    while True:
        now = int(time.time())
        for k, ap_ in enumerate(AIRPORTS):
            for s in fake_states(ap_, now):
                send(p, args.prefix + "opensky.states", s["icao24"], s)
            if n % args.weather_every == 0:
                send(p, args.prefix + "weather.current", ap_["iata"],
                     fake_weather(ap_, now, phase=n / 10 + k))
        p.flush()
        n += 1
        print(f"seria {n} wyslana", flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
