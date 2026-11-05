"""Dashboard: panel boczny + 4 zakładki, razem 4 wizualizacje i asystent.  streamlit run dashboard/app.py"""
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agent.ask import ask, report  # noqa: E402
from common.data import AIRPORTS, demo_mode, read_topic  # noqa: E402
from common.text import airport_label, anomaly_name, detail_pl, level_name, reason_name  # noqa: E402

st.set_page_config(page_title="Ostrzeganie o zakłóceniach lotów", layout="wide")

st.markdown(
    """
    <style>
      .block-container {padding-top: 2.2rem; max-width: 1200px;}
      h1 {font-weight: 650; letter-spacing: -0.02em; font-size: 1.9rem !important;}
      h2, h3 {font-weight: 600; letter-spacing: -0.01em;}
      [data-testid="stSidebar"] {border-right: 1px solid #e6e8eb;}
      [data-testid="stMetricValue"] {font-size: 1.5rem;}
      .stTabs [data-baseweb="tab-list"] {gap: 1.4rem;}
      #MainMenu, footer {visibility: hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)

PALETTE = ["#2563eb", "#0d9488", "#f59e0b", "#9333ea", "#64748b"]
LEVEL_STYLE = {"niskie": "background-color:#e8f5ee;color:#1b7a46",
               "średnie": "background-color:#fff4dd;color:#9a6700",
               "wysokie": "background-color:#fde4e4;color:#b42318"}


@st.cache_data(ttl=20)
def load():
    return (pd.DataFrame(read_topic("alerts", 400)), pd.DataFrame(read_topic("anomalies", 400)),
            pd.DataFrame(read_topic("weather.current", 400)))


def line_chart(df: pd.DataFrame, color_title: str = "", percent: bool = True, height: int = 300):
    """df: kolumny time, series, value (0-1)."""
    series = sorted(df["series"].unique())
    return (
        alt.Chart(df).mark_line(strokeWidth=2, interpolate="monotone")
        .encode(
            x=alt.X("time:T", title=None, axis=alt.Axis(format="%H:%M", grid=False)),
            y=alt.Y("value:Q", title=None, axis=alt.Axis(format="%" if percent else ".2f", tickCount=5),
                    scale=alt.Scale(domainMin=0)),
            color=alt.Color("series:N", title=color_title or None,
                            scale=alt.Scale(domain=series, range=(PALETTE * 3)[: len(series)]),
                            legend=alt.Legend(orient="bottom", labelLimit=320)),
            tooltip=[alt.Tooltip("time:T", format="%H:%M", title="Czas (UTC)"),
                     alt.Tooltip("series:N", title="Seria"),
                     alt.Tooltip("value:Q", format=".0%" if percent else ".2f", title="Wartość")],
        )
        .properties(height=height).configure_view(strokeWidth=0)
        .configure_axis(labelColor="#52606d", domainColor="#d9dde2", tickColor="#d9dde2")
    )


alerts, anomalies, weather = load()

# ---------------------------------------------------------------- panel boczny
with st.sidebar:
    st.subheader("Ustawienia")
    hours = st.select_slider("Okno czasu", options=[1, 3, 6, 12, 24], value=6, format_func=lambda h: f"{h} h")
    chosen = st.multiselect("Lotniska", list(AIRPORTS), default=list(AIRPORTS), format_func=airport_label)
    if st.button("Odśwież dane", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption("Źródło: " + ("dane demonstracyjne" if demo_mode() else "PostgreSQL / Kafka"))
    if not alerts.empty:
        last_ts = int(alerts["ts"].max())
        st.caption(f"Ostatni alert: {datetime.fromtimestamp(last_ts, timezone.utc):%H:%M:%S} UTC")
        if not demo_mode() and int(time.time()) - last_ts > 900:
            st.warning(f"Alerty są starsze niż {(int(time.time()) - last_ts) // 60} min. Czy działa strumień?")
    st.caption("Czasy na wykresach w UTC.")

st.title("Ostrzeganie o zakłóceniach lotów przez pogodę")
if alerts.empty:
    st.info("Brak alertów. Uruchom strumień (infra/stream.sh) albo dane demo: python -m common.demo_data")
    st.stop()
if not chosen:
    st.info("Wybierz co najmniej jedno lotnisko w panelu bocznym.")
    st.stop()

alerts["time"] = pd.to_datetime(alerts["ts"], unit="s")
if "delay_prob" not in alerts:
    alerts["delay_prob"] = None
cutoff = int(alerts["ts"].max()) - hours * 3600
view = alerts[(alerts["ts"] >= cutoff) & alerts["airport"].isin(chosen)].copy()
view["lotnisko"] = view["airport"].map(airport_label)
an_view = (anomalies[(anomalies["ts"] >= cutoff) & anomalies["airport"].isin(chosen)].copy()
           if not anomalies.empty else anomalies)

tab_over, tab_an, tab_one, tab_ask = st.tabs(["Przegląd", "Anomalie", "Lotnisko", "Asystent"])

# ---------------------------------------------------------------- 1 + 2: przegląd
with tab_over:
    st.subheader("1. Stan lotnisk teraz")
    last = alerts.sort_values("ts").groupby("airport").tail(1).set_index("airport")
    table = last.reindex([a for a in chosen if a in last.index])[["delay_prob", "risk", "level", "reason", "aircraft"]]
    if not weather.empty:
        wx = weather.sort_values("ts").groupby("airport").tail(1).set_index("airport")
        table = table.join(wx[["wind_gusts_10m", "precipitation"]])
    table = table.sort_values("delay_prob" if table["delay_prob"].notna().any() else "risk", ascending=False)
    table.index = [airport_label(a) for a in table.index]
    table["level"] = table["level"].map(level_name)
    table["reason"] = table["reason"].map(reason_name).str.capitalize()
    table = table.rename(columns={"delay_prob": "Szansa opóźnienia", "risk": "Ryzyko pogodowe", "level": "Poziom",
                                  "reason": "Główna przyczyna", "aircraft": "Samoloty",
                                  "wind_gusts_10m": "Porywy", "precipitation": "Opady"})
    table.index.name = "Lotnisko"
    cfg = {
        "Szansa opóźnienia": st.column_config.ProgressColumn(format="percent", min_value=0, max_value=1),
        "Ryzyko pogodowe": st.column_config.NumberColumn(format="%.2f"),
        "Samoloty": st.column_config.NumberColumn(format="%d"),
        "Porywy": st.column_config.NumberColumn(format="%.0f km/h"),
        "Opady": st.column_config.NumberColumn(format="%.1f mm"),
    }
    styled = table.style.map(lambda v: LEVEL_STYLE.get(v, ""), subset=["Poziom"])
    st.dataframe(styled, column_config={k: v for k, v in cfg.items() if k in table.columns}, width="stretch")

    st.subheader("2. Szansa opóźnienia w czasie")
    st.caption("Prawdopodobieństwo, że typowy odlot spóźni się o ponad 15 minut (model uczony na danych z 2015 r.).")
    st.altair_chart(line_chart(view.rename(columns={"lotnisko": "series", "delay_prob": "value"})
                               [["time", "series", "value"]].dropna(), "Lotnisko"), width="stretch")

# ---------------------------------------------------------------- 3: anomalie
with tab_an:
    st.subheader("3. Wykryte anomalie")
    if an_view.empty:
        st.write(f"Brak anomalii w ostatnich {hours} h.")
    else:
        counts = (an_view.assign(Lotnisko=an_view["airport"].map(airport_label),
                                 Typ=an_view["type"].map(anomaly_name))
                  .groupby(["Lotnisko", "Typ"]).size().reset_index(name="Liczba"))
        bars = (alt.Chart(counts).mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(x=alt.X("Lotnisko:N", title=None, axis=alt.Axis(labelAngle=0, labelLimit=170)),
                        y=alt.Y("Liczba:Q", title=None, axis=alt.Axis(tickMinStep=1)),
                        color=alt.Color("Typ:N", title=None, legend=alt.Legend(orient="bottom"),
                                        scale=alt.Scale(range=["#2563eb", "#f59e0b", "#64748b"])),
                        tooltip=["Lotnisko", "Typ", "Liczba"])
                .properties(height=300).configure_view(strokeWidth=0)
                .configure_axis(labelColor="#52606d", domainColor="#d9dde2", tickColor="#d9dde2", grid=False))
        st.altair_chart(bars, width="stretch")
        with st.expander("Szczegóły ostatnich anomalii"):
            det = an_view.sort_values("ts", ascending=False).head(30)
            st.dataframe(pd.DataFrame({
                "Czas (UTC)": pd.to_datetime(det["ts"], unit="s").dt.strftime("%H:%M"),
                "Lotnisko": det["airport"].map(airport_label),
                "Typ": det["type"].map(anomaly_name),
                "Opis": det["detail"].map(detail_pl)}), hide_index=True, width="stretch")

# ---------------------------------------------------------------- 4: jedno lotnisko
with tab_one:
    sel = st.selectbox("Lotnisko", chosen, format_func=airport_label)
    st.subheader(f"4. Model i wskaźnik pogodowy: {airport_label(sel)}")
    one = view[view["airport"] == sel][["time", "delay_prob", "risk"]].melt("time", var_name="series")
    one["series"] = one["series"].map({"delay_prob": "Szansa opóźnienia (model)",
                                       "risk": "Ryzyko pogodowe (reguły)"})
    st.altair_chart(line_chart(one.dropna(), height=300), width="stretch")
    if not weather.empty and sel in set(weather["airport"]):
        w = weather[weather["airport"] == sel].sort_values("ts").iloc[-1]
        c1, c2, c3 = st.columns(3)
        c1.metric("Porywy wiatru", f"{w['wind_gusts_10m']:.0f} km/h")
        c2.metric("Opady", f"{w['precipitation']:.1f} mm")
        c3.metric("Zachmurzenie", f"{w['cloud_cover']:.0f}%")

# ---------------------------------------------------------------- asystent
with tab_ask:
    st.subheader("Asystent dyżurnego")
    if "chat" not in st.session_state:
        st.session_state.chat = []
    hist = st.session_state.chat

    def _answer(question: str) -> None:
        with st.spinner("Asystent czyta dane…"):
            reply = report() if question == "__raport__" else ask(question, hist)
        label = "Raport sytuacyjny" if question == "__raport__" else question
        hist.extend([{"role": "user", "content": label}, {"role": "assistant", "content": reply}])

    c1, c2, c3 = st.columns(3)
    if c1.button("Raport sytuacyjny", width="stretch"):
        _answer("__raport__")
    if c2.button("Które lotnisko jest teraz najbardziej zagrożone?", width="stretch"):
        _answer("Które lotnisko jest teraz najbardziej zagrożone i dlaczego?")
    if c3.button("Co się dzieje z anomaliami?", width="stretch"):
        _answer("Jakie anomalie wykryto w ostatnich 2 godzinach i co one oznaczają?")
    if q := st.chat_input("Zadaj pytanie, np. Dlaczego Chicago O'Hare (ORD) ma dziś opóźnienia?"):
        _answer(q)

    for m in hist:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])
    if hist and st.button("Wyczyść rozmowę"):
        st.session_state.chat = []
        st.rerun()
    st.caption("Asystent tylko czyta dane i doradza. Decyzje podejmuje człowiek.")
