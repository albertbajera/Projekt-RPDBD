"""agent.ask(question) -> str (markdown). Asystent dyżurnego: czyta wyniki systemu, doradza, nie decyduje.

Z LLM_API_KEY (dowolne API zgodne z OpenAI) model sam wybiera narzedzia z agent/tools.py
i odpowiada po polsku. Bez klucza dziala regulowy raport z tych samych narzedzi.
"""
from __future__ import annotations

import json
import os
import re
import urllib.request

from common.text import airport_label, anomaly_name, detail_pl, level_name, reason_name
from agent.tools import (AIRPORTS, airport_status, call, openai_specs, ranking,
                         recent_anomalies, risk_trend)

SYSTEM = (
    "Jesteś asystentem dyżurnego centrum operacyjnego lotniska. Odpowiadasz po polsku, z polskimi znakami, "
    "zwięźle (maks. 8 zdań), w markdown (pogrubienia dla lotnisk i liczb, krótkie listy). "
    "Dane bierzesz WYŁĄCZNIE z narzędzi; jeśli narzędzie nic nie zwraca, napisz, że danych brak. "
    "delay_prob to prawdopodobieństwo, że typowy odlot z lotniska spóźni się o ponad 15 min "
    "(model z historii 2015, słaby sygnał, AUC ok. 0,63), a risk to prosty wskaźnik pogodowy 0-1. "
    "Lotniska nazywaj pełnymi nazwami z kodem w nawiasie, np. Chicago O'Hare (ORD). Typy anomalii: holding = krążenie w strefie oczekiwania, go_around = odejście na drugi krąg, "
    "departure_drop = spadek liczby odlotów. Podaj liczby, przyczynę (pogoda/anomalie) i trend. "
    "Nie masz danych o pojedynczych rejsach ani prognozy na kolejne godziny, więc o to nie zgaduj: powiedz, czego nie wiesz. "
    "Jeśli alert_age_min > 15, zaznacz, że dane są nieświeże. Możesz zasugerować, na co zwrócić uwagę, ale decyzje podejmuje człowiek."
)

TREND = {"rosnie": "rośnie", "spada": "spada", "stabilnie": "stabilnie"}


def _minutes(q: str) -> int:
    m = re.search(r"(\d+)\s*(min|godz|h)", q.lower())
    if not m:
        return 120
    return int(m.group(1)) * (1 if m.group(2) == "min" else 60)


def _pct(x) -> str:
    return "brak" if x is None else f"{x * 100:.0f}%"


def _report(question: str) -> str:
    """Odpowiedź bez LLM (markdown): ranking + szczegóły dla pytanych lotnisk (domyślnie najgorszego)."""
    rank = ranking()
    if not rank:
        return "Brak alertów w systemie. Sprawdź, czy działa strumień (`infra/stream.sh`)."
    chosen = [a for a in AIRPORTS if a in question.upper()] or [rank[0]["airport"]]
    minutes = _minutes(question)
    lines = ["**Ranking zagrożenia opóźnieniami:** "
             + " · ".join(f"{airport_label(r['airport'])} {_pct(r['delay_prob'])}" for r in rank)]
    for a in chosen:
        st, tr, an = airport_status(a), risk_trend(a, minutes), recent_anomalies(a, minutes)
        al, w = st["alert"], st["weather"]
        lines.append("")
        if not al:
            lines.append(f"**{airport_label(a)}**: brak alertów.")
            continue
        head = f"**{airport_label(a)}**: szansa opóźnienia **{_pct(al.get('delay_prob'))}**"
        if tr.get("direction"):
            head += f", trend {TREND.get(tr['direction'], tr['direction'])}"
        lines.append(head)
        if (st.get("alert_age_min") or 0) > 15:
            lines.append(f"- ⚠ Dane nieświeże: ostatni alert sprzed {st['alert_age_min']} min")
        lines.append(f"- Ryzyko pogodowe: {al['risk']:.2f} ({level_name(al['level'])}, {reason_name(al['reason'])})")
        if w:
            lines.append(f"- Pogoda: porywy {w.get('wind_gusts_10m'):.0f} km/h, opady {w.get('precipitation'):.1f} mm, "
                         f"zachmurzenie {w.get('cloud_cover'):.0f}%")
        if an:
            lines.append(f"- Anomalie ({minutes} min): " + "; ".join(
                f"{anomaly_name(x['type']).lower()} ({detail_pl(x['detail'])})" for x in an[-5:]))
        else:
            lines.append(f"- Anomalie ({minutes} min): brak")
    lines += ["", "_To podpowiedź z danych, decyzję podejmuje dyżurny._"]
    return "\n".join(lines)


def _chat(messages: list[dict], tools: list[dict]) -> dict:
    base = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    body = {"model": os.environ.get("LLM_MODEL", "gpt-4o-mini"), "temperature": 0,
            "messages": messages, "tools": tools}
    req = urllib.request.Request(
        f"{base}/chat/completions", data=json.dumps(body).encode(), method="POST",
        headers={"Authorization": f"Bearer {os.environ['LLM_API_KEY']}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())["choices"][0]["message"]


def _llm(question: str, history: list[dict] | None = None) -> str:
    messages = [{"role": "system", "content": SYSTEM}, *(history or [])[-6:], {"role": "user", "content": question}]
    for _ in range(6):                                   # petla narzedzi: max 6 krokow
        msg = _chat(messages, openai_specs())
        messages.append(msg)
        if not msg.get("tool_calls"):
            return (msg.get("content") or "").strip()
        for tc in msg["tool_calls"]:
            try:
                result = call(tc["function"]["name"], json.loads(tc["function"]["arguments"] or "{}"))
            except Exception as e:                       # zly argument od modelu nie przerywa rozmowy
                result = {"error": str(e)}
            messages.append({"role": "tool", "tool_call_id": tc["id"],
                             "content": json.dumps(result, ensure_ascii=False)})
    return _report(question)


def report() -> str:
    """Poranny raport sytuacyjny: te same narzedzia, stale pytanie."""
    return ask("Przygotuj krotki raport sytuacyjny dla wszystkich lotnisk: ranking, anomalie z ostatnich 2 godzin, co obserwowac.")


def ask(question: str, history: list[dict] | None = None) -> str:
    """history: poprzednie wiadomości [{"role": "user"|"assistant", "content": ...}] dla pytań uzupełniających."""
    if os.environ.get("LLM_API_KEY", "").strip():
        try:
            return _llm(question, history)
        except Exception as e:
            return _report(question) + f"\n_(LLM niedostępny: {e})_"
    return _report(question)


def main() -> None:
    import sys
    args = sys.argv[1:]
    print(report() if args == ["--raport"] else ask(" ".join(args).strip() or "Co jest dziś zagrożone?"))


if __name__ == "__main__":
    main()
