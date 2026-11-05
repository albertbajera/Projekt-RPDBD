"""Pobiera pogodę 2015 i zbiór lotów odpowiadający Kaggle usdot/flight-delays.

Kaggle wymaga logowania. Gdy nie ma ~/.kaggle/kaggle.json, bierzemy ten sam
publiczny zbiór DOT (5 819 079 lotów z 2015) z kopii Parquet. Kolumny i tak
sprowadza batch/train.py do nazw z flights.csv.
"""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
FLIGHTS = RAW / "flights"
WEATHER = RAW / "weather"
AIRPORTS = ROOT / "contracts" / "airports.json"

HF = "https://huggingface.co/datasets/hsanchezp/us-dot-flight-delays-2015/resolve/main"
HF_FILES = {
    "flights.parquet": FLIGHTS / "flights.parquet",
    "airports.parquet": FLIGHTS / "airports.parquet",
    "airlines.parquet": FLIGHTS / "airlines.parquet",
}


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        print(f"jest {dest.name} ({dest.stat().st_size} B)")
        return
    print(f"pobieram {url}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": "rpdbd/1.0"})
    with urllib.request.urlopen(req, timeout=120) as response, tmp.open("wb") as out:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
    tmp.replace(dest)
    print(f"zapisano {dest} ({dest.stat().st_size} B)")


def fetch_weather() -> None:
    spec = json.loads(AIRPORTS.read_text(encoding="utf-8"))
    airports = spec if isinstance(spec, list) else spec["airports"]
    WEATHER.mkdir(parents=True, exist_ok=True)
    fields = "temperature_2m,precipitation,wind_speed_10m,wind_gusts_10m,cloud_cover"
    for airport in airports:
        dest = WEATHER / f"weather_{airport['iata']}.json"
        url = (
            "https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={airport['lat']}&longitude={airport['lon']}"
            "&start_date=2015-01-01&end_date=2015-12-31"
            f"&hourly={fields}&timezone=auto"
        )
        download(url, dest)
        payload = json.loads(dest.read_text(encoding="utf-8"))
        if "hourly" not in payload or "time" not in payload["hourly"]:
            raise SystemExit(f"{dest.name} nie zawiera hourly.time: {list(payload)[:6]}")
        hours = len(payload["hourly"]["time"])
        print(f"{airport['iata']}: {hours} godzin, timezone={payload.get('timezone_abbreviation') or payload.get('timezone')}")


def fetch_flights() -> None:
    for name, dest in HF_FILES.items():
        download(f"{HF}/{name}?download=true", dest)


def main() -> None:
    fetch_weather()
    fetch_flights()


if __name__ == "__main__":
    main()
