"""Polskie nazwy do wyświetlania (dane w Kafce i bazie zostają ASCII, zgodnie z contracts/)."""

AIRPORT_NAMES = {
    "ATL": "Atlanta Hartsfield-Jackson",
    "ORD": "Chicago O'Hare",
    "DFW": "Dallas/Fort Worth",
    "JFK": "Nowy Jork John F. Kennedy",
    "LAX": "Los Angeles",
}
ANOMALY_NAMES = {
    "holding": "Krążenie w strefie oczekiwania",
    "go_around": "Odejście na drugi krąg",
    "departure_drop": "Spadek liczby odlotów",
}
LEVEL_NAMES = {"low": "niskie", "medium": "średnie", "high": "wysokie"}
REASON_NAMES = {"spokojnie": "spokojnie", "porywy wiatru": "porywy wiatru", "opady": "opady",
                "zachmurzenie": "zachmurzenie"}

_DETAIL = (
    ("okrazen", "okrążeń"), ("odlotow", "odlotów"), ("zejscie", "zejście"),
    ("wznoszenie", "wznoszenie"), ("oczekiwane", "oczekiwane"), ("samolotow", "samolotów"),
)


def anomaly_name(kind: str) -> str:
    return ANOMALY_NAMES.get(kind, kind)


def level_name(level) -> str:
    return LEVEL_NAMES.get(level, str(level))


def reason_name(reason) -> str:
    return REASON_NAMES.get(reason, str(reason))


def detail_pl(text) -> str:
    """Opis anomalii z detektora (ASCII) z polskimi znakami."""
    out = str(text or "")
    for src, dst in _DETAIL:
        out = out.replace(src, dst)
    return out


def airport_label(code: str) -> str:
    """Pełna nazwa z kodem w nawiasie, np. Chicago O'Hare (ORD)."""
    return f"{AIRPORT_NAMES[code]} ({code})" if code in AIRPORT_NAMES else str(code)
