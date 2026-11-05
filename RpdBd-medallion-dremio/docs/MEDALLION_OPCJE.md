# Warstwy medallion (bronze / silver / gold) — kontekst i opcje

## Kontekst problemu

Projekt RPBD ma już pipeline danych (Kafka, Spark, HDFS, PostgreSQL, dashboard Streamlit), ale **nie ma formalnego podziału medallion** (bronze → silver → gold).

Dziś jakość danych rośnie „po drodze”, tylko bez tej nazwy:

| Idea medallion | Co mamy dziś |
| --- | --- |
| Surowe (bronze) | HDFS `/data/raw/...`, Kafka `opensky.states` / `weather.current` |
| Oczyszczone / połączone (silver) | HDFS `/data/curated/flights_weather`, przetwarzanie w Spark / detector |
| Produkt biznesowy (gold) | model `/models/delay_gbt`, baseline, tematy `alerts` / `anomalies`, PostgreSQL, dashboard |

**Cel:** wprowadzić czytelny podział bronze / silver / gold oraz **móc to pokazać na żywo** (wejście w Web UI / podsumowanie), analogicznie do tego, jak pokazuje się Kafka albo Spark Master UI — **bez generowanych ręcznie obrazków-podsumowań**.

**Wymaganie twarde przy pokazie:** narzędzie ma **domyślnie własne ładne Web UI** (nie tylko CLI i nie tylko własna zakładka Streamlit).

---

## Co chcę osiągnąć

1. Mieć nazwane warstwy danych: **bronze → silver → gold**.
2. Mieć **transformacje** między warstwami (joby / zapytania).
3. Móc **wejść w przeglądarkę** i pokazać prowadzącemu / zespołowi stan warstw.
4. Nie rozwalać istniejącego klastra n1–n5 (Kafka + Spark + HDFS + PG + dashboard zostają).
5. Kafka / PostgreSQL / Streamlit traktować jako **ścieżkę operacyjną / serving**, a medallion głównie na **jeziorze danych (HDFS / lakehouse)**.

---

## Opcje (od najbardziej „ładnego UI” do najprostszych)

### Opcja A — Dremio (rekomendowana pod pokaz z Web UI)

- **Co to jest:** silnik lakehouse z **własnym nowoczesnym Web UI**.
- **Medallion:** spaces / foldery `bronze`, `silver`, `gold` nad plikami na HDFS (Parquet / Delta).
- **Plusy:** spełnia wymóg „wejść w ładne UI”; ładnie wygląda na demo; czyta dane z HDFS bez przepisywania całego projektu.
- **Minusy:** dodatkowa instalacja i utrzymanie na klastrze; trzeba podpiąć źródło HDFS i ułożyć spaces.
- **Kiedy brać:** gdy priorytetem jest **pokaz w przeglądarce**, nie sama biblioteka Delta.

### Opcja B — Apache Hue

- **Co to jest:** klasyczny Web UI pod Hadoop (HDFS + SQL).
- **Medallion:** katalogi na HDFS + zapytania.
- **Plusy:** typowe na zajęciach Big Data; własne UI; lżejsze skojarzenie z HDFS.
- **Minusy:** wygląda „bardziej akademicko / staro” niż Dremio; mniej „lakehouse wow”.
- **Kiedy brać:** gdy chcecie coś prostszego i bliżej czystego Hadoopa.

### Opcja C — Delta Lake na Spark + osobne UI do podglądu

- **Co to jest:** tabele Delta w HDFS (`/lake/bronze|silver|gold`) + Spark joby między warstwami.
- **UI:** Delta **nie ma** domyślnego ładnego Web UI → trzeba dokładać Hue / Dremio / HDFS UI (`:9870`) / własny ekran.
- **Plusy:** kanoniczny wzorzec medallion (Databricks); dobrze brzmi w dokumentacji; pasuje do istniejącego Sparka.
- **Minusy:** **sama Delta nie spełnia** wymogu „narzędzie z domyślnym ładnym Web UI”.
- **Kiedy brać:** gdy najpierw chcecie solidną warstwę storage, a UI dobierzecie osobno (np. A lub B).

### Opcja D — Tylko konwencja katalogów na HDFS (bez nowego silnika)

- **Co to jest:** `/data/bronze`, `/data/silver`, `/data/gold` + istniejące joby Spark.
- **UI:** HDFS NameNode UI (`n1:9870`) albo nic ekstra.
- **Plusy:** najszybsze; zero nowych serwisów.
- **Minusy:** słaby „wow”; to nie osobne narzędzie z bogatym UI; HDFS UI to przeglądarka plików, nie lakehouse.
- **Kiedy brać:** mało czasu, wystarczy uczciwy podział ścieżek.

### Opcja E — Databricks (chmura)

- **Plusy:** najładniejszy medallion + UI z pudełka.
- **Minusy:** słabo pasuje do waszego lokalnego klastra n1–n5; zwykle poza zakresem kursu „maszyny wirtualne”.
- **Kiedy brać:** raczej nie, chyba że prowadzący akceptuje demo w chmurze.

### Czego nie mylić z celem

| Rzecz | Rola |
| --- | --- |
| Streamlit dashboard | już macie; to serving alertów, nie katalog medallion |
| PostgreSQL | historia / serving, nie warstwa bronze |
| Kafka | stream; można mieć tematy raw/clean, ale to nie klasyczny medallion lakehouse |
| Wygenerowane PNG / karty | screenshot do docs — **nie** zastępuje Web UI |

---

## Rekomendowany kierunek

1. **Na pokaz z Web UI:** **Opcja A (Dremio)** — albo Hue (B), jeśli ma być prościej.
2. **Pod spodem danych:** trzymać Parquet (już jest) albo dorzucić **Delta Lake (C)** jako format tabel bronze/silver/gold.
3. **Praktyczny kompromis „ładnie + sensownie”:**  
   **Delta (lub Parquet) na HDFS + Dremio jako Web UI nad warstwami.**

Kafka, Spark streaming, Postgres i dashboard **zostają** — medallion opisuje głównie ścieżkę wsadową / jezioro.

---

## Lista kroków — co mogę zrobić (kolejność)

### Etap 0 — decyzja (1 rozmowa z zespołem)

- [ ] Potwierdzić wymóg: **musi być własne Web UI narzędzia** (nie Streamlit, nie PNG).
- [ ] Wybrać wariant: **A Dremio** / **B Hue** / **D tylko HDFS** (ew. C Delta jako format).
- [ ] Ustalć nazwy ścieżek, np. `/lake/bronze`, `/lake/silver`, `/lake/gold`.
- [ ] Uzgodnić, że live Kafka + PG to **serving**, nie muszą być rename’owane na bronze/silver/gold.

### Etap 1 — ułożenie warstw na HDFS (zawsze warto, nawet przed UI)

- [ ] Zmapować istniejące dane:
  - bronze ← `/data/raw/flights`, `/data/raw/weather`
  - silver ← `/data/curated/flights_weather`
  - gold ← `/data/curated/baseline` (+ ewentualnie agregaty pod raport)
- [ ] Skopiować lub przepiąć ścieżki do `/lake/...` (albo symlink / dokumentacja mapowania).
- [ ] Dopisać 1–2 joby Spark: raw → curated (silver), curated → baseline/agregaty (gold) — część już jest w `batch/train.py`.
- [ ] Sprawdzić w HDFS UI (`n1:9870`), że widać trzy drzewa katalogów.

### Etap 2a — jeśli wybrane Dremio (A)

- [ ] Wybrać host (np. n5 albo n1) pod serwis Dremio.
- [ ] Zainstalować Dremio, otworzyć Web UI (domyślny port wg dokumentacji instalacji).
- [ ] Podłączyć źródło HDFS / file store ze ścieżkami lake.
- [ ] Utworzyć spaces: `bronze`, `silver`, `gold`.
- [ ] W UI pokazać: listę datasetów, podgląd schematu, `SELECT` / count per warstwa.
- [ ] Zapisać w docs adres UI + login demo (bez haseł w git).
- [ ] Tunel SSH z laptopa, żeby dało się wejść z przeglądarki jak do dashboardu.

### Etap 2b — jeśli wybrane Hue (B)

- [ ] Zainstalować Hue obok HDFS.
- [ ] Włączyć File Browser + edytor SQL (Hive/Spark, jeśli dostępne).
- [ ] Pokazać w UI katalogi bronze/silver/gold i przykładowe zapytanie.
- [ ] Tunel / dostęp z przeglądarki.

### Etap 2c — jeśli tylko HDFS (D)

- [ ] Uporządkować katalogi `/lake/...`.
- [ ] Na demo wejść w NameNode UI i przejść bronze → silver → gold.
- [ ] Świadomie zaakceptować słabszy efekt wizualny.

### Etap 3 — (opcjonalnie) Delta Lake jako format (C)

- [ ] Dodać pakiet Delta do `spark-submit` / sesji Sparka.
- [ ] Zapsiać silver/gold jako tabele Delta (bronze może zostać Parquet).
- [ ] Podpiąć te ścieżki pod Dremio/Hue.
- [ ] Na demo: krótko pokazać, że to tabele Delta (historia / detail), a przeglądanie i tak w Web UI narzędzia z etapu 2.

### Etap 4 — dokumentacja i pokaz

- [ ] Jedna strona w docs: schemat bronze → silver → gold + link do Web UI.
- [ ] Scenariusz demo (2–3 minuty):
  1. otwórz Web UI,
  2. bronze — surowe loty/pogoda,
  3. silver — po joinie,
  4. gold — normy / agregaty,
  5. (opcjonalnie) dashboard / Kafka jako serving obok.
- [ ] Nie zastępować UI zrzutami — zrzuty tylko jako backup do Worda dla prezesa.

---

## Szybka ściąga decyzji

| Priorytet | Wybierz |
| --- | --- |
| Ładne Web UI z pudełka | **Dremio (A)** |
| Prościej, klimat Hadoop | **Hue (B)** |
| Minimalny nakład | **katalogi HDFS (D)** |
| „Mamy Delta Lake” w CV / docs | **format Delta (C) + UI z A lub B** |
| Demo w chmurze | Databricks (E) — raczej poza zakresem |

---

## Powiązane miejsca w projekcie

- Surowe / curated / model: HDFS `/data/raw`, `/data/curated`, `/models/delay_gbt`
- Batch: `batch/train.py`, `batch/fetch_data.py`
- Stream serving: Kafka (`contracts/topics.md`), `storage/to_postgres.py`, `dashboard/app.py`
- Opis klastra: `README.md`

---

## Status

- [ ] Decyzja zespołu: A / B / C+A / D
- [ ] Etap 1 (ścieżki) zrobiony
- [ ] Etap 2 (Web UI) zrobiony
- [ ] Scenariusz pokazu sprawdzony na żywo
