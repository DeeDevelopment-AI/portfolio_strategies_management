# -*- coding: utf-8 -*-
import sqlite3, pandas as pd, numpy as np, streamlit as st, os
from config import DB_PATH

st.set_page_config(page_title="Factor Study Dashboard", layout="wide")

@st.cache_data(show_spinner=False)
def read_table(name: str) -> pd.DataFrame:
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    con = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(f"SELECT * FROM {name}", con)
    finally:
        con.close()

st.title("📊 Factor Study Dashboard")

perf = read_table("performance")
rets = read_table("portfolio_returns")
weights = read_table("weights")

if perf.empty or rets.empty:
    st.warning("Aún no hay datos en la base. Ejecuta `run_study.py` primero.")
    st.stop()

# --- Performance table
st.subheader("Ranking de estrategias")
st.dataframe(perf.sort_values("CAGR", ascending=False).style.format({
    "CAGR":"{:.2%}","AnnVol":"{:.2%}","Sharpe":"{:.2f}","Sortino":"{:.2f}","MaxDD":"{:.2%}","HitRate":"{:.2%}","N":"{:.0f}"
}), use_container_width=True)

# --- Strategy selector
strats = sorted(perf["strategy"].unique())
sel = st.multiselect("Estrategías a visualizar", options=strats, default=strats[:3])

# --- Equity curves
st.subheader("Curvas de capital (mensual)")
if sel:
    rets["date"] = pd.to_datetime(rets["date"])
    pivot = rets.pivot(index="date", columns="strategy", values="ret").sort_index()
    eq = (1 + pivot.fillna(0)).cumprod()
    st.line_chart(eq[sel])

# --- Weights snapshot (último mes) para estrategia elegida
st.subheader("Top 20 pesos último mes")
one = st.selectbox("Estrategia", options=strats, index=0)
wdf = weights.copy()
wdf["date"] = pd.to_datetime(wdf["date"])
if one and not wdf.empty:
    last = wdf[wdf["strategy"]==one]["date"].max()
    wlast = wdf[(wdf["strategy"]==one) & (wdf["date"]==last)].sort_values("weight", ascending=False).head(20)
    st.bar_chart(wlast.set_index("symbol")["weight"])
