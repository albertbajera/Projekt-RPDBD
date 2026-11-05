"""Detektor anomalii.

Czyta wiadomosci z tematow `opensky.states` i `weather.current` (schematy: contracts/schemas.json)
i zapisuje wykryte anomalie w formacie tematu `anomalies`:
    {"ts", "airport", "type", "icao24", "detail"}
Dla anomalii calego lotniska (`departure_drop`) `icao24` jest null.

Reguly (progi na gorze pliku, do dostrojenia na danych):
  holding         - krazenie: suma zmian true_track > 300 stopni w 10 min, nisko, w powietrzu
  go_around       - drugi krag: zejscie ponizej ~300 m nad lotniskiem, potem dodatni vertical_rate
  departure_drop  - spadek odlotow w oknie 15 min wzgledem WLASNEJ mediany z historii. Normy z wsadu
                    (/lake/gold/baseline) daja tylko KSZTALT doby (godzina x dzien tygodnia),
                    poziom liczy sie z danych OpenSky, bo OpenSky widzi tylko czesc samolotow.
Dane pogodowe z `weather.current` dopisuja do opisu anomalii kontekst (porywy, opady).

Uzycie (z katalogu glownego repo):
  python anomalies/detector.py --file out/opensky.jsonl --weather out/weather.jsonl     # offline
  python anomalies/detector.py --kafka                      # opensky.states, weather.current -> anomalies
  python anomalies/detector.py --kafka --prefix dev-        # to samo na tematach testowych dev-*
  dodatkowo: --baseline out/baseline.jsonl                  # normy z wsadu (hdfs dfs -getmerge /lake/gold/baseline ...)
"""
import argparse
import json
import os
import re
import signal
import statistics
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --- progi -----------------------------------------------------------------
HOLD_WINDOW = 600        # s: okno krazenia
HOLD_TURN = 300.0        # stopnie: suma |zmian kursu| w oknie
HOLD_ALT = 4000.0        # m nad lotniskiem: krazenie liczymy tylko nisko (strefa oczekiwania)
HOLD_MIN_SAMPLES = 3     # min. liczba probek w powietrzu w oknie
HOLD_MIN_SPEED = 65.0    # m/s: srednia predkosc w oknie; wolniejsze to ogolne/smiglowce (rejsowe >= ok. 65)
GA_ALT = 300.0           # m nad lotniskiem: "zszedl nisko" = prawie ladowanie
GA_DESCENT = -0.5        # m/s: probka podejscia (opadanie)
GA_CLIMB = 1.5           # m/s: wznoszenie po podejsciu
GA_MIN_SPEED = 55.0      # m/s: predkosc na podejsciu (predkosc ladowania rejsowych ok. 65-80)
BIN = 900                # s: okno liczenia odlotow (15 min)
MIN_BINS = 8             # z normami B: min. liczba pelnych okien historii (2 h) przed pierwszym alarmem
MIN_SAME_HOUR = 8        # bez norm B: min. okien z tej samej godziny doby (ok. 2 dni)
MIN_SHAPE = 0.1          # okna o porze, gdy B widzi < 10% sredniej godziny, pomijamy (noc)
Z_THRESHOLD = -2.0
MAX_HISTORY = 672        # okien na lotnisko (7 dni po 15 min)
GAP = 360                # s: przerwa w danych = producent nie dzialal; okno odrzucamy
COOLDOWN = 900           # s: ten sam alarm dla tego samego samolotu nie czesciej niz co 15 min
WEATHER_MAX_AGE = 1800   # s: starsza pogode pomijamy
GUST_NOTE = 40.0         # km/h: od tylu porywow dopisujemy pogode do opisu anomalii
PRECIP_NOTE = 1.0        # mm

# Wysokosc lotniska n.p.m. [m]: OpenSky podaje wysokosc barometryczna, wiec "nisko" liczymy
# nad poziomem lotniska (ATL lezy na ok. 313 m, wiec staly prog 300 m nie mialby sensu).
AIRPORT_ELEV = {"ATL": 313.0, "ORD": 204.0, "DFW": 185.0, "JFK": 4.0, "LAX": 38.0}
AIRPORT_TZ = {"ATL": "America/New_York", "ORD": "America/Chicago", "DFW": "America/Chicago",
              "JFK": "America/New_York", "LAX": "America/Los_Angeles"}
_STD_OFFSET = {"America/New_York": -5, "America/Chicago": -6, "America/Los_Angeles": -8}

# Ruch rejsowy: znak wywolawczy = 3 litery operatora + cyfry (DAL123, UAL4B). Samoloty
# ogolnego przeznaczenia (N12345) i smiglowce latajace w tym samym boxie dawaly fale
# falszywych alarmow (patrz docs/c.md), a nie wplywaja na opoznienia lotow rejsowych.
COMMERCIAL = re.compile(r"^[A-Z]{3}\d{1,4}[A-Z]{0,2}$")


def angle_diff(a, b):
    """Najkrotsza roznica katow w stopniach, w zakresie [-180, 180)."""
    return (b - a + 180.0) % 360.0 - 180.0


def local_hour_dow(ts, airport):
    """(godzina lokalna 0-23, dzien tygodnia 1=pon..7=niedz jak w BTS) dla lotniska."""
    name = AIRPORT_TZ.get(airport, "America/New_York")
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.fromtimestamp(ts, ZoneInfo(name))
    except Exception:                                   # brak bazy stref: czas zimowy
        dt = datetime.fromtimestamp(ts, timezone.utc) + timedelta(hours=_STD_OFFSET.get(name, -5))
    return dt.hour, dt.isoweekday()


def load_baseline(path):
    """Normy B -> kszalt doby: {(airport, hour, dow): wzgledna liczba odlotow, srednia lotniska = 1}.

    `path`: plik JSONL/JSON albo katalog z plikami Spark (part-*.json). Wiersze:
    {"airport","hour","day_of_week","departures","mean_departure_delay"}.
    """
    p = Path(path)
    files = sorted(set(p.glob("*.json*")) | set(p.glob("part-*"))) if p.is_dir() else [p]
    rows = []
    for f in files:
        if f.name.startswith(".") or f.suffix == ".crc":
            continue
        text = f.read_text().strip()
        if not text:
            continue
        if text.startswith("["):
            rows += json.loads(text)
        else:
            rows += [json.loads(l) for l in text.splitlines() if l.strip()]
    per_airport = defaultdict(list)
    for r in rows:
        per_airport[r["airport"]].append(float(r["departures"]))
    mean = {a: sum(v) / len(v) for a, v in per_airport.items() if v}
    shape = {}
    for r in rows:
        m = mean.get(r["airport"])
        if m:
            shape[(r["airport"], int(r["hour"]), int(r["day_of_week"]))] = float(r["departures"]) / m
    if not shape:
        raise ValueError(f"brak wierszy norm w {path}")
    return shape


class Detector:
    def __init__(self, history=None, shape=None):
        self.shape = shape                     # kształt doby z norm B albo None
        self.samples = defaultdict(deque)      # (airport, icao24) -> probki z ostatnich 10 min
        self.cool = {}                         # (typ, airport, icao24) -> ts ostatniego alarmu
        self.bins = {}                         # airport -> stan biezacego okna odlotow
        self.weather = {}                      # airport -> ostatnia wiadomosc weather.current
        self.history = defaultdict(list)       # airport -> [[godzina, liczba (znormalizowana)], ...]
        self._load_history(history)
        self._n = 0

    # -- stan -----------------------------------------------------------------
    @property
    def mode(self):
        return "shape" if self.shape else "raw"

    def _load_history(self, history):
        if not history:
            return
        if "history" in history:               # nowy format {"mode":..., "history":{...}}
            if history.get("mode") != self.mode:
                return                         # inna skala (z/bez norm B): nie mieszamy
            history = history["history"]
        for k, v in history.items():
            self.history[k] = [e if isinstance(e, list) else [None, e] for e in v]

    def dump_history(self):
        return {"mode": self.mode, "history": {k: v for k, v in self.history.items()}}

    # -- wejscie ------------------------------------------------------------
    def update_weather(self, w):
        """Przyjmuje wiadomosc weather.current (kontekst do opisow anomalii)."""
        if w and w.get("airport"):
            self.weather[w["airport"]] = w

    def process(self, m):
        """Przyjmuje jedna wiadomosc opensky.states, zwraca liste anomalii."""
        if not COMMERCIAL.match(m.get("callsign") or ""):
            return []                          # nie ruch rejsowy
        key = (m["airport"], m["icao24"])
        dq = self.samples[key]
        if dq and m["ts"] <= dq[-1]["ts"]:
            return []                          # duplikat / spozniona probka
        prev = dq[-1] if dq else None
        dq.append(m)
        while dq and m["ts"] - dq[0]["ts"] > HOLD_WINDOW:
            dq.popleft()
        self._n += 1
        if self._n % 2000 == 0:
            self._purge(m["ts"])
        out = []
        for rule in (self._holding, self._go_around):
            a = rule(m, dq)
            if a:
                out.append(a)
        out += self._departures(m, prev)
        return out

    # -- reguly ---------------------------------------------------------------
    def _holding(self, m, dq):
        alt = m.get("baro_altitude")
        if m["on_ground"] or alt is None or alt - AIRPORT_ELEV.get(m["airport"], 0.0) >= HOLD_ALT:
            return None
        air = [s for s in dq if not s["on_ground"] and s.get("true_track") is not None]
        if len(air) < HOLD_MIN_SAMPLES:
            return None
        speeds = [s["velocity"] for s in air if s.get("velocity") is not None]
        if not speeds or sum(speeds) / len(speeds) < HOLD_MIN_SPEED:
            return None
        total = sum(abs(angle_diff(a["true_track"], b["true_track"])) for a, b in zip(air, air[1:]))
        if total <= HOLD_TURN:
            return None
        minutes = max(1, round((air[-1]["ts"] - air[0]["ts"]) / 60))
        return self._emit(m, "holding", f"{total / 360:.1f} okrazen w {minutes} min")

    def _go_around(self, m, dq):
        vr = m.get("vertical_rate")
        if m["on_ground"] or vr is None or vr <= GA_CLIMB:
            return None
        elev = AIRPORT_ELEV.get(m["airport"], 0.0)
        samples = list(dq)[:-1]
        for i in range(len(samples) - 1, -1, -1):
            s = samples[i]
            if s["on_ground"]:
                return None                    # po ostatnim ladowaniu nic nie bylo podejsciem
            alt, svr = s.get("baro_altitude"), s.get("vertical_rate")
            if (s.get("velocity") or 0.0) < GA_MIN_SPEED:
                continue
            if alt is not None and alt - elev < GA_ALT and svr is not None and svr < GA_DESCENT:
                return self._emit(m, "go_around",
                                  f"zejscie do {alt - elev:.0f} m nad lotniskiem, potem wznoszenie {vr:.1f} m/s")
        return None

    def _departures(self, m, prev):
        a = m["airport"]
        b = m["ts"] // BIN
        st = self.bins.get(a)
        if st is None:
            st = self.bins[a] = {"bin": b, "count": 0, "dirty": True, "last_ts": m["ts"]}
        gap = m["ts"] - st["last_ts"] > GAP
        if gap:
            st["dirty"] = True
        res = []
        if b != st["bin"]:
            res = self._close_bin(a, st)
            st["bin"], st["count"], st["dirty"] = b, 0, gap
        st["last_ts"] = m["ts"]
        if prev is not None and prev["on_ground"] and m["on_ground"] is False and m["ts"] - prev["ts"] <= GAP:
            st["count"] += 1
        return res

    def _close_bin(self, airport, st):
        if st["dirty"]:
            return []                          # niepelne okno (start lub luka): ignorujemy
        x = st["count"]
        hour, dow = local_hour_dow(st["bin"] * BIN, airport)
        if self.shape is not None:
            w = self.shape.get((airport, hour, dow), 0.0)
            if w < MIN_SHAPE:
                return []                      # o tej porze prawie nie ma lotow: nie oceniamy
            y, need = x / w, MIN_BINS
            pool = [e[1] for e in self.history[airport]]
        else:
            w, y, need = 1.0, float(x), MIN_SAME_HOUR
            pool = [e[1] for e in self.history[airport] if e[0] == hour]
        anomaly = None
        if len(pool) >= need:
            med = statistics.median(pool)
            mad = statistics.median(abs(h - med) for h in pool)
            z = (y - med) / max(1.4826 * mad, 1.0)
            if z < Z_THRESHOLD:
                end = (st["bin"] + 1) * BIN
                anomaly = {"ts": end, "airport": airport, "type": "departure_drop", "icao24": None,
                           "detail": f"{x} odlotow w 15 min, oczekiwane ok. {med * w:.1f} (z={z:.1f})"
                                     + self._weather_note(airport, end)}
        if anomaly is None:
            hist = self.history[airport]
            hist.append([hour, y])             # alarmowych okien nie uczymy sie jako normy
            del hist[:-MAX_HISTORY]
        return [anomaly] if anomaly else []

    # -- pomocnicze -----------------------------------------------------------
    def _weather_note(self, airport, ts):
        w = self.weather.get(airport)
        if not w or abs(ts - w.get("ts", 0)) > WEATHER_MAX_AGE:
            return ""
        parts = []
        if (w.get("wind_gusts_10m") or 0) >= GUST_NOTE:
            parts.append(f"porywy {w['wind_gusts_10m']:g} km/h")
        if (w.get("precipitation") or 0) >= PRECIP_NOTE:
            parts.append(f"opady {w['precipitation']:g} mm")
        return " | pogoda: " + ", ".join(parts) if parts else ""

    def _emit(self, m, typ, detail):
        k = (typ, m["airport"], m["icao24"])
        if m["ts"] - self.cool.get(k, -10 ** 12) < COOLDOWN:
            return None
        self.cool[k] = m["ts"]
        return {"ts": m["ts"], "airport": m["airport"], "type": typ, "icao24": m["icao24"],
                "detail": detail + self._weather_note(m["airport"], m["ts"])}

    def _purge(self, now):
        for k in [k for k, dq in self.samples.items() if not dq or now - dq[-1]["ts"] > 3600]:
            del self.samples[k]


# --- wejscie/wyjscie -----------------------------------------------------------
def topic_names(prefix=""):
    """(wejscie samoloty, wejscie pogoda, wyjscie anomalie, group.id). Prefiks np. dev- do testow."""
    return (prefix + "opensky.states", prefix + "weather.current", prefix + "anomalies",
            prefix + "anomaly-detector")


def _read_jsonl(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def _load_state(state):
    if state and Path(state).exists():
        try:
            return json.loads(Path(state).read_text())
        except ValueError:
            print(f"uszkodzony plik stanu {state}: zaczynam od zera", file=sys.stderr)
    return None


def _save_state(det, state):
    if state:
        Path(state).parent.mkdir(parents=True, exist_ok=True)
        Path(state).write_text(json.dumps(det.dump_history()))


def run_file(src, dst, state, weather=None, baseline=None):
    det = Detector(_load_state(state), load_baseline(baseline) if baseline else None)
    events = [(r["ts"], 1, r) for r in _read_jsonl(src)]
    if weather and Path(weather).exists():
        events += [(r["ts"], 0, r) for r in _read_jsonl(weather)]   # pogoda przed samolotami o tym samym ts
    events.sort(key=lambda e: (e[0], e[1]))
    found, n = [], 0
    for _, kind, r in events:
        if kind == 0:
            det.update_weather(r)
        else:
            found += det.process(r)
            n += 1
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    with open(dst, "w") as f:
        for a in found:
            f.write(json.dumps(a) + "\n")
    _save_state(det, state)
    counts = defaultdict(int)
    for a in found:
        counts[(a["airport"], a["type"])] += 1
    span = (events[-1][0] - events[0][0]) / 60 if events else 0
    print(f"{n} wiadomosci, {span:.0f} min danych, {len(found)} anomalii -> {dst}")
    for k, v in sorted(counts.items()):
        print(f"  {k[0]} {k[1]}: {v}")
    print("historia odlotow (pelne okna):", {k: len(v) for k, v in det.history.items()})
    return found


def run_kafka(state, prefix="", baseline=None, consumer=None, producer=None, max_idle=None):
    """Zwykly konsument w Pythonie (bez Sparka). `consumer`/`producer` do wstrzykniecia w testach,
    `max_idle`: konczy po tylu pustych odczytach z rzedu (None = dziala az do SIGTERM/Ctrl+C)."""
    t_in, t_wx, t_out, group = topic_names(prefix)
    boot = os.getenv("KAFKA_BOOTSTRAP", "n1:9092,n2:9092,n3:9092")
    if consumer is None or producer is None:
        from confluent_kafka import Consumer, Producer
        consumer = consumer or Consumer({"bootstrap.servers": boot, "group.id": group,
                                         "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET", "latest")})
        producer = producer or Producer({"bootstrap.servers": boot})
    det = Detector(_load_state(state), load_baseline(baseline) if baseline else None)
    consumer.subscribe([t_in, t_wx])
    stop = {"now": False}

    def _stop(*_):
        stop["now"] = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _stop)
        except ValueError:                     # nie glowny watek
            pass

    def _delivered(err, _msg):
        if err:
            print(f"blad dostarczenia anomalii: {err}", file=sys.stderr, flush=True)

    n = idle = 0
    print(f"detektor: {t_in}, {t_wx} -> {t_out} (group {group}, normy: {'tak' if baseline else 'nie'})", flush=True)
    try:
        while not stop["now"]:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                if max_idle is not None and idle >= max_idle:
                    break
                continue
            idle = 0
            if msg.error():
                print(f"blad Kafki: {msg.error()}", file=sys.stderr, flush=True)
                continue
            try:
                m = json.loads(msg.value())
            except (ValueError, TypeError):
                continue
            if msg.topic() == t_wx:
                det.update_weather(m)
                continue
            for a in det.process(m):
                producer.produce(t_out, key=a["airport"], value=json.dumps(a), on_delivery=_delivered)
                print("ANOMALIA", a, flush=True)
            producer.poll(0)
            n += 1
            if n % 500 == 0:
                _save_state(det, state)
    finally:
        producer.flush(10)
        _save_state(det, state)
        consumer.close()
    return det


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--file", help="plik JSONL z opensky.states (tryb offline)")
    ap.add_argument("--weather", help="plik JSONL z weather.current (tryb offline, opcjonalnie)")
    ap.add_argument("--out", default="out/anomalies.jsonl")
    ap.add_argument("--kafka", action="store_true")
    ap.add_argument("--prefix", default=os.getenv("TOPIC_PREFIX", ""), help="prefiks tematow Kafki, np. dev- (puste = produkcja)")
    ap.add_argument("--baseline", help="normy z wsadu (JSONL albo katalog Spark) jako ksztalt doby")
    ap.add_argument("--state", default="out/departure_history.json",
                    help="plik z historia odlotow (wlasna norma), zeby przetrwala restart")
    a = ap.parse_args()
    if a.kafka:
        run_kafka(a.state, a.prefix, a.baseline)
    elif a.file:
        run_file(a.file, a.out, a.state, a.weather, a.baseline)
    else:
        ap.error("podaj --file albo --kafka")


if __name__ == "__main__":
    main()
