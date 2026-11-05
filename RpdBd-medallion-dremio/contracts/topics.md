# Tematy Kafki

Wszystkie tematy: 3 partycje, replikacja 3. Tworzy je infra/create_topics.sh.
Czas w sekundach unixowych UTC (`ts`). Jednostki jak w API: OpenSky w metrach, m/s i stopniach, Open-Meteo w C, km/h i mm.
Przyklady wiadomosci w `schemas.json`, lotniska w `airports.json`.

| Temat | Pisze | Czyta | Klucz | Zawartosc |
| --- | --- | --- | --- | --- |
| opensky.states | C | A, C | icao24 | stan jednego samolotu w boxie lotniska |
| weather.current | C | A | airport | biezaca pogoda lotniska |
| alerts | A | D (dashboard), B (agent) | airport | ryzyko zaklocen |
| anomalies | C | D (dashboard), B (agent) | airport | wykryta anomalia |

Testy na tematach z prefiksem dev- (np. infra/create_topics.sh dev-).
Zmiana tego pliku albo schematow tylko po uzgodnieniu z wszystkimi.

## Uwagi do pol (bez zmiany schematow)

- `alerts.ts` to czas wyslania alertu, nie poczatek okna. Strumien A aktualizuje okno 15 min co 30 s, wiec dla jednego okna
  przychodzi kilka alertow. Czytelnik bierze najnowszy.
- `alerts.delay_prob` (opcjonalne): prawdopodobienstwo opoznienia odlotu >15 min z modelu B, tylko gdy strumien dziala z `--model`.
  Brak pola albo `null` oznacza brak modelu albo brakujaca ceche.
- `anomalies.icao24` jest `null` dla `departure_drop` (anomalia calego lotniska).
- Tematy testowe maja te same nazwy z prefiksem osoby, np. `dev-c-opensky.states`, `dev-c-alerts`.
  Zaklada je `infra/create_topics.sh`.
