"""Narzedzia agenta: tylko odczyt wynikow systemu (alerts, anomalies, weather.current)."""
from __future__ import annotations

import time

from common.data import AIRPORTS, read_topic


def _of(rows: list[dict], airport: str) -> list[dict]:
    return [r for r in rows if r.get("airport") == airport]


def airport_status(airport: str) -> dict:
    """Ostatni alert (ryzyko pogodowe + prawdopodobienstwo opoznienia z modelu) i pogoda."""
    alerts = _of(read_topic("alerts"), airport)
    weather = _of(read_topic("weather.current"), airport)
    age = None if not alerts else max(0, int(time.time()) - int(alerts[-1]["ts"])) // 60
    return {"airport": airport,
            "alert": alerts[-1] if alerts else None,
            "alert_age_min": age,                     # wiek ostatniego alertu w minutach
            "weather": weather[-1] if weather else None}


def risk_trend(airport: str, minutes: int = 120) -> dict:
    """Jak zmienialo sie prawdopodobienstwo opoznienia (delay_prob) w ostatnich minutach."""
    rows = _of(read_topic("alerts"), airport)
    if not rows:
        return {"airport": airport, "points": 0}
    rows = [r for r in rows if r["ts"] >= rows[-1]["ts"] - minutes * 60]
    key = "delay_prob" if rows[-1].get("delay_prob") is not None else "risk"
    first, last = rows[0].get(key), rows[-1].get(key)
    change = None if first is None or last is None else round(last - first, 3)
    direction = None
    if change is not None:
        direction = "rosnie" if change > 0.03 else "spada" if change < -0.03 else "stabilnie"
    return {"airport": airport, "metric": key, "points": len(rows), "from": first, "to": last,
            "change": change, "direction": direction}


def recent_anomalies(airport: str, minutes: int = 120) -> list[dict]:
    rows = _of(read_topic("anomalies"), airport)
    alerts = read_topic("alerts")
    now = alerts[-1]["ts"] if alerts else (rows[-1]["ts"] if rows else 0)
    return [r for r in rows if r["ts"] >= now - minutes * 60]


def ranking() -> list[dict]:
    """Lotniska od najbardziej zagrozonego (delay_prob, a gdy go brak: risk)."""
    out = []
    for a in AIRPORTS:
        al = airport_status(a)["alert"]
        if al:
            out.append({"airport": a, "delay_prob": al.get("delay_prob"), "risk": al.get("risk"),
                        "level": al.get("level"), "reason": al.get("reason")})
    return sorted(out, key=lambda r: r["delay_prob"] if r["delay_prob"] is not None else (r["risk"] or 0),
                  reverse=True)


TOOLS = {
    "ranking": (ranking, "Lotniska posortowane od najwiekszego zagrozenia opoznieniami.", {}),
    "airport_status": (airport_status, "Biezace ryzyko, prawdopodobienstwo opoznienia i pogoda dla lotniska.",
                       {"airport": "kod IATA: ATL, ORD, DFW, JFK lub LAX"}),
    "risk_trend": (risk_trend, "Trend prawdopodobienstwa opoznienia na lotnisku.",
                   {"airport": "kod IATA", "minutes": "okno w minutach (domyslnie 120)"}),
    "recent_anomalies": (recent_anomalies, "Wykryte anomalie ruchu (krazenie, drugi krag, spadek odlotow).",
                         {"airport": "kod IATA", "minutes": "okno w minutach (domyslnie 120)"}),
}


def openai_specs() -> list[dict]:
    specs = []
    for name, (_, desc, params) in TOOLS.items():
        props = {k: {"type": "string" if k == "airport" else "integer", "description": v}
                 for k, v in params.items()}
        specs.append({"type": "function", "function": {
            "name": name, "description": desc,
            "parameters": {"type": "object", "properties": props,
                           "required": [k for k in props if k == "airport"]}}})
    return specs


def call(name: str, args: dict):
    fn = TOOLS[name][0]
    if "airport" in args:
        args = {**args, "airport": str(args["airport"]).upper()}
        if args["airport"] not in AIRPORTS:
            return {"error": f"nieznane lotnisko, dostepne: {', '.join(AIRPORTS)}"}
    return fn(**args)
