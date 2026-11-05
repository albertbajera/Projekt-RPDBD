"""Dane demo bez klastra: python -m common.demo_data  ->  data/fixtures/*.jsonl"""
import json
import math
import random
import time

from common.data import AIRPORTS, FIXTURES

BASE = {"ATL": 0.30, "ORD": 0.38, "DFW": 0.28, "JFK": 0.35, "LAX": 0.22}


def main(hours: int = 6, step_min: int = 5) -> None:
    random.seed(7)
    FIXTURES.mkdir(parents=True, exist_ok=True)
    now = int(time.time())
    alerts, weather, anomalies = [], [], []
    n = hours * 60 // step_min
    for i in range(n):
        ts = now - (n - 1 - i) * step_min * 60
        for a in AIRPORTS:
            storm = a == "ORD" and i > n * 0.55          # burza nad ORD pod koniec okresu
            gust = max(0.0, 15 + 8 * random.random() + (40 if storm else 0))
            rain = round(random.random() * 0.3 + (3.5 if storm else 0), 1)
            cloud = min(100, int(30 + 20 * random.random() + (50 if storm else 0)))
            risk = round(0.5 * min(gust / 60, 1) + 0.3 * min(rain / 5, 1) + 0.2 * cloud / 100, 2)
            p = BASE[a] + 0.2 * risk + 0.03 * math.sin(i / 6) + random.uniform(-0.01, 0.01)
            level = "high" if risk >= 0.66 else "medium" if risk >= 0.33 else "low"
            alerts.append({"ts": ts, "airport": a, "risk": risk, "level": level,
                           "reason": "spokojnie" if level == "low" else "porywy wiatru",
                           "aircraft": random.randint(30, 110), "delay_prob": round(min(p, 0.95), 3)})
            weather.append({"ts": ts, "airport": a, "temperature_2m": 14.0, "wind_speed_10m": round(gust * 0.6, 1),
                            "wind_gusts_10m": round(gust, 1), "precipitation": rain, "cloud_cover": cloud})
    t0 = now - 90 * 60
    anomalies += [
        {"ts": t0, "airport": "ORD", "type": "holding", "icao24": "a1b2c3", "detail": "krazenie 340 stopni w 10 min | pogoda: porywy 52 km/h"},
        {"ts": t0 + 900, "airport": "ORD", "type": "go_around", "icao24": "d4e5f6", "detail": "drugi krag z 180 m"},
        {"ts": t0 + 1500, "airport": "ORD", "type": "departure_drop", "icao24": None, "detail": "2 odloty w 15 min, norma ok. 9"},
        {"ts": t0 - 3000, "airport": "JFK", "type": "holding", "icao24": "0a1b2c", "detail": "krazenie 320 stopni w 10 min"},
    ]
    for name, rows in (("alerts", alerts), ("weather.current", weather), ("anomalies", anomalies)):
        (FIXTURES / f"{name}.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    print(f"zapisano dane demo w {FIXTURES}")


if __name__ == "__main__":
    main()
