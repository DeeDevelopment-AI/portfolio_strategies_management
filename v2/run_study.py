# -*- coding: utf-8 -*-
import os, logging, argparse
import numpy as np, pandas as pd
from datetime import datetime
from typing import Dict

from config import (DEFAULT_START, DEFAULT_END, EXCHANGES, OUT_DIR, TOP_Q, BOTTOM_Q, MIN_LIQ_PCTL, BETA_WINDOW_D)
from db import init_db, upsert_many, latest_price_date
from universe import get_universe, fetch_profiles, persist_universe
from prices_ndl import get_eod_prices_ndl, persist_prices
from fundamentals import compute_static_factors_from_ndl
from altdata_fmp import insider_net_90d, sentiment_30d
from neutralize import winsorize, zscore, residualize_industry_size
from portfolio import next_month_returns, beta_rolling_daily, build_long_only, build_long_short_beta_neutral, portfolio_returns_from_weights
from performance import perf_stats

os.makedirs(OUT_DIR, exist_ok=True)

def run(start: str, end: str, universe_size: int, include_delisted: bool, loglevel: str = "INFO", seed: int = 42):
    logging.basicConfig(level=getattr(logging, loglevel.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s: %(message)s")
    init_db()

    # 1) Universo (FMP)
    uni = get_universe(EXCHANGES, include_delisted, universe_size, seed=seed)
    persist_universe(uni)

    # Perfilar (sector/industria/mcap)
    prof = fetch_profiles(uni["symbol"].tolist())
    upsert_many("universe", [(s,None,None,None,sector,industry,mcap) for s,sector,industry,mcap in prof], "?,?,?,?,?,?,?")
    prof_df = pd.DataFrame(prof, columns=["symbol","sector","industry","market_cap"]).set_index("symbol")
    df_uni = uni.set_index("symbol").join(prof_df, how="left")
    industries = df_uni["industry"]; log_mcap = np.log1p(df_uni["market_cap"])

    # 2) Precios (Sharadar SEP) incremental + SPY también via SEP
    syms = uni["symbol"].tolist()
    syms_all = syms + ["SPY"]
    price_map: Dict[str, pd.DataFrame] = {}
    for i,s in enumerate(syms_all,1):
        if i % 25 == 0: logging.info("Precios NDL %d/%d ...", i, len(syms_all))
        last_dt = latest_price_date(s)
        start_dt = start
        if last_dt:
            try:
                next_dt = (pd.to_datetime(last_dt)+pd.Timedelta(days=1)).date().isoformat()
                if next_dt > end: continue
                start_dt = next_dt
            except Exception: pass
        df = get_eod_prices_ndl(s, start_dt, end)
        if df is not None and not df.empty:
            price_map[s] = df
    if price_map:
        persist_prices({k:v for k,v in price_map.items() if k in syms})
    if not price_map:
        logging.error("Sin datos de precios (NDL). Revisa API key NDL.")
        return

    # Panel completo en memoria (para el estudio actual)
    all_idx = sorted(set().union(*[df.index for df in price_map.values()]))
    df_close = pd.DataFrame(index=all_idx, data={s:price_map[s]["close"] for s in price_map}).sort_index()
    df_vol   = pd.DataFrame(index=all_idx, data={s:price_map[s]["volume"] for s in price_map}).sort_index()

    # 3) Factores estáticos: NDL (SF1) + Alt-data de FMP
    factor_rows = []
    for i,s in enumerate(syms,1):
        if i % 25 == 0: logging.info("Factores NDL+FMP %d/%d ...", i, len(syms))
        px = price_map.get(s)
        if px is None: continue
        f = compute_static_factors_from_ndl(s, px)
        # alt-data
        ins = insider_net_90d(s)
        sen = sentiment_30d(s)
        f["InsiderNet90d"] = ins
        f["Sentiment30d"]  = sen
        factor_rows.append((s, datetime.utcnow().strftime("%Y-%m-%d"), f))

    rows=[]
    for s, asof, f in factor_rows:
        rows.append((s, asof, f.get("B2M"), f.get("EBIT_EV"), f.get("ROA_TTM"), f.get("AssetGrowthYoY"),
                     f.get("MOM_12_1_last"), f.get("VOL60_last"), f.get("ADV20_last"),
                     f.get("InsiderNet90d"), f.get("Sentiment30d")))
    upsert_many("factors_static", rows, "?,?,?,?,?,?,?,?,?,?,?")

    fac_df = pd.DataFrame({r[0]:{"B2M":r[2],"EBIT_EV":r[3],"ROA_TTM":r[4],"AssetGrowthYoY":r[5],
                                 "InsiderNet90d":r[9], "Sentiment30d":r[10]} for r in rows}).T

    # 4) Mensualización + features
    m_close = df_close.resample("M").last()
    adv20_m = (df_close*df_vol).rolling(20).mean().resample("M").last()
    vol60_m = df_close.pct_change().rolling(60).std().resample("M").last()
    m_rets  = next_month_returns(m_close)

    # 5) Beta (SPY desde SEP)
    betas=None
    if "SPY" in df_close.columns:
        betas_daily = beta_rolling_daily(df_close.drop(columns=["SPY"], errors="ignore"), df_close["SPY"], window=BETA_WINDOW_D)
        betas = betas_daily.resample("M").last()
        upsert_many("betas_monthly", [(dt.strftime("%Y-%m-%d"),sym,float(b)) for dt,row in betas.iterrows() for sym,b in row.dropna().items()], "?,?,?")

    # 6) Z-residualización, carteras y composite
    months = m_close.index
    weights_panel = {}; returns_map = {}
    mom_12_1 = m_close.shift(1)/m_close.shift(12) - 1.0
    fac_static = fac_df.reindex(m_close.columns)

    for dt in months:
        mom_dt = mom_12_1.loc[dt]; vol_dt = vol60_m.loc[dt]; adv_dt = adv20_m.loc[dt]
        fac_panel = pd.DataFrame({
            "B2M": fac_static["B2M"],
            "EBIT_EV": fac_static["EBIT_EV"],
            "ROA_TTM": fac_static["ROA_TTM"],
            "AssetGrowthYoY": fac_static["AssetGrowthYoY"],
            "MOM_12_1": mom_dt,
            "VOL60": vol_dt,
            "InsiderNet90d": fac_static["InsiderNet90d"],
            "Sentiment30d": fac_static["Sentiment30d"]
        })

        z_resid={}
        for f in fac_panel.columns:
            s = winsorize(fac_panel[f].astype(float), p=0.01)
            # direcciones: +B2M +EBIT/EV +ROA +MOM +Insiders +Sentiment ; -AssetGrowth -VOL60
            dir_ = +1 if f in ["B2M","EBIT_EV","ROA_TTM","MOM_12_1","InsiderNet90d","Sentiment30d"] else -1
            z = zscore(s)*dir_
            z = residualize_industry_size(z, industries, log_mcap)
            z_resid[f]=z
        zdf = pd.DataFrame(z_resid)

        for f in ["B2M","EBIT_EV","ROA_TTM","MOM_12_1","VOL60","AssetGrowthYoY","InsiderNet90d","Sentiment30d"]:
            lo = f"FACTOR_LONG_ONLY::{f}"
            weights_panel.setdefault(lo, {})[dt] = build_long_only(zdf[f], adv_dt, MIN_LIQ_PCTL, TOP_Q)
            ls = f"FACTOR_LS_BETA_NEUTRAL::{f}"
            if betas is not None and dt in betas.index:
                weights_panel.setdefault(ls, {})[dt] = build_long_short_beta_neutral(zdf[f], betas.loc[dt], adv_dt, MIN_LIQ_PCTL, TOP_Q, BOTTOM_Q, 1.0)

        comp_cols = ["B2M","EBIT_EV","ROA_TTM","MOM_12_1","InsiderNet90d","Sentiment30d","AssetGrowthYoY","VOL60"]
        comp = zdf[comp_cols].mean(axis=1, skipna=True)
        sc = "COMPOSITE_LS_BETA_NEUTRAL"
        if betas is not None and dt in betas.index:
            weights_panel.setdefault(sc, {})[dt] = build_long_short_beta_neutral(comp, betas.loc[dt], adv_dt, MIN_LIQ_PCTL, TOP_Q, BOTTOM_Q, 1.0)

    for strat, wpan in weights_panel.items():
        returns_map[strat] = portfolio_returns_from_weights(wpan, m_rets)

    upsert_many("weights", [(dt.strftime("%Y-%m-%d"), strat, s, float(val)) for strat,wpan in weights_panel.items() for dt,w in wpan.items() for s,val in w.dropna().items() if abs(val)>0], "?,?,?,?")
    upsert_many("portfolio_returns", [(dt.strftime("%Y-%m-%d"), strat, float(r)) for strat,series in returns_map.items() for dt,r in series.dropna().items()], "?,?,?")

    perf_rows = []
    for strat, series in returns_map.items():
        stats = perf_stats(series); perf_rows.append((strat, stats["CAGR"], stats["AnnVol"], stats["Sharpe"], stats["Sortino"], stats["MaxDD"], stats["HitRate"], stats["N"]))
        series.to_csv(os.path.join(OUT_DIR, f"rets_{strat.replace('::','_')}.csv"))
    upsert_many("performance", perf_rows, "?,?,?,?,?,?,?,?")

    perf_df = pd.DataFrame(perf_rows, columns=["strategy","CAGR","AnnVol","Sharpe","Sortino","MaxDD","HitRate","N"]).sort_values("CAGR", ascending=False)
    perf_df.to_csv(os.path.join(OUT_DIR, "performance_summary.csv"), index=False)
    print("\n=== Top estrategias por CAGR ===\n")
    with pd.option_context("display.float_format", lambda x: f"{x:,.3f}"):
        print(perf_df.head(20).to_string(index=False))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end",   default=DEFAULT_END)
    ap.add_argument("--universe-size", type=int, default=500)
    ap.add_argument("--include-delisted", type=int, default=1)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--log", default="INFO")
    args = ap.parse_args()
    run(args.start, args.end, args.universe_size, args.include_delisted==1, args.log, args.seed)
