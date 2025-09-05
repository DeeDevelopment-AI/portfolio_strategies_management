# -*- coding: utf-8 -*-
import logging
import pandas as pd
from typing import Set, List, Tuple
from fmp_client import fmp_get
from db import upsert_many
from config import EXCHANGES

def get_universe(exchanges: Set[str], include_delisted: bool, universe_size: int, seed: int = 42) -> pd.DataFrame:
    logging.info("Descargando tickers activos...")
    active = fmp_get("/api/v3/stock/list") or []
    df_a = pd.DataFrame(active)
    if not df_a.empty and "exchangeShortName" in df_a.columns:
        df_a = df_a[df_a["exchangeShortName"].isin(list(exchanges))]
    df_a["is_delisted"] = 0

    if include_delisted:
        logging.info("Descargando tickers deslistados...")
        dl = fmp_get("/api/v3/delisted-companies") or []
        df_d = pd.DataFrame(dl)
        if df_d.empty:
            df_d = pd.DataFrame(columns=["symbol","name","exchangeShortName"])
        df_d["is_delisted"] = 1
        df = pd.concat([df_a, df_d], ignore_index=True).drop_duplicates(subset=["symbol"])
    else:
        df = df_a.copy()

    df = df.rename(columns={"exchangeShortName":"exchange"})
    df = df[df["symbol"].astype(str).str.len() <= 8]
    df = df[~df["symbol"].astype(str).str.contains(r"[^A-Z\.]")]
    df = df[["symbol","name","exchange","is_delisted"]].dropna(subset=["symbol"]).reset_index(drop=True)

    if len(df) > universe_size:
        n_del = int(universe_size*0.2)
        df_del = df[df["is_delisted"]==1].sample(min(n_del, (df["is_delisted"]==1).sum()), random_state=seed) if (df["is_delisted"]==1).any() else df.head(0)
        df_act = df[df["is_delisted"]==0].sample(universe_size - len(df_del), random_state=seed)
        df = pd.concat([df_act, df_del], ignore_index=True)

    return df

def fetch_profiles(symbols: List[str]) -> List[Tuple[str, str, str, float]]:
    rows=[]
    for s in symbols:
        data = fmp_get(f"/api/v3/profile/{s}") or []
        sector = industry = None
        mcap = None
        if isinstance(data, list) and data:
            d = data[0]
            sector = d.get("sector")
            industry = d.get("industry")
            mcap = d.get("mktCap") or d.get("marketCap")
        rows.append((s, sector, industry, float(mcap) if mcap is not None else None))
    return rows

def persist_universe(df_uni: pd.DataFrame):
    rows = [(r.symbol, r.name, r.exchange, int(r.is_delisted), None, None, None) for r in df_uni.itertuples()]
    upsert_many("universe", rows, "?,?,?,?,?,?,?")
