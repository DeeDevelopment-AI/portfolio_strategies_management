# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from typing import Dict, Any
from clients.nasdaq_client import ndl_get
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def _datatable_to_df(obj: dict) -> pd.DataFrame:
    if not obj or "datatable" not in obj:
        return pd.DataFrame()
    cols = [c["name"] for c in obj["datatable"]["columns"]]
    data = obj["datatable"]["data"]
    return pd.DataFrame(data, columns=cols)

def sf1_latest_ttm(symbol: str) -> Dict[str, Any]:
    frames, cursor_id = [], None
    base_params = {
        "ticker": symbol,
        "dimension": "TTM",
        "qopts.columns": "ticker,calendardate,ebit,ev,roa,pb,price,sharesbas,bvps",
        "qopts.per_page": 10000,
    }
    while True:
        params = dict(base_params)
        if cursor_id:
            params["qopts.cursor_id"] = cursor_id
        obj = ndl_get("/datatables/SHARADAR/SF1", params=params)
        df = _datatable_to_df(obj)
        if df.empty:
            break
        frames.append(df)
        meta = (obj or {}).get("meta", {}) or {}
        cursor_id = meta.get("next_cursor_id")
        if not cursor_id:
            break

        if not frames:
            return {}

        df = pd.concat(frames, ignore_index=True)
        df["calendardate"] = pd.to_datetime(df["calendardate"], errors="coerce")
        df = df.dropna(subset=["calendardate"]).sort_values("calendardate")
        return df.tail(1).to_dict(orient="records")[0]

def sf1_annual_assets(symbol: str, limit: int = 4) -> pd.DataFrame:
    params = {
        "ticker": symbol,
        "dimension": "ARY",
        "qopts.columns": "ticker,calendardate,assets",
        "order": "calendardate",
        "qopts.per_page": limit
    }
    obj = ndl_get("/datatables/SHARADAR/SF1", params=params)
    df = _datatable_to_df(obj)
    if "calendardate" in df.columns:
        df["calendardate"] = pd.to_datetime(df["calendardate"])
        df = df.sort_values("calendardate")
    return df

def compute_static_factors_from_ndl(symbol: str, px: pd.DataFrame) -> dict:
    row = sf1_latest_ttm(symbol)
    pb = row.get("pb") if row else None
    roa = row.get("roa") if row else None
    ebit = row.get("ebit") if row else None
    ev = row.get("ev") if row else None
    price = row.get("price") if row else None
    bvps = row.get("bvps") if row else None

    b2m = np.nan
    try:
        if pb not in (None, 0):
            b2m = 1.0/float(pb)
        elif bvps not in (None, 0) and price not in (None, 0):
            b2m = float(bvps)/float(price)
    except Exception:
        pass

    roa_ttm = float(roa) if roa is not None else np.nan

    ebit_ev = np.nan
    try:
        if ebit not in (None, 0) and ev not in (None, 0):
            ebit_ev = float(ebit)/float(ev)
    except Exception:
        pass

    asset_growth = np.nan
    try:
        bs = sf1_annual_assets(symbol, limit=3)
        if not bs.empty and "assets" in bs and len(bs) >= 2:
            asset_growth = (bs["assets"].iloc[-1] - bs["assets"].iloc[-2]) / abs(bs["assets"].iloc[-2])
    except Exception:
        pass

    mom_last = vol60_last = adv20_last = np.nan
    try:
        if len(px) >= 252 + 21:
            p = px["close"].astype(float)
            mom_last = p.iloc[-22]/p.iloc[-253] - 1.0
        if len(px) >= 60:
            ret = px["close"].pct_change()
            vol60_last = ret.rolling(60).std().iloc[-1]
        if len(px) >= 20:
            adv20_last = (px["close"] * px["volume"]).rolling(20).mean().iloc[-1]
    except Exception:
        pass

    return dict(B2M=b2m, EBIT_EV=ebit_ev, ROA_TTM=roa_ttm, AssetGrowthYoY=asset_growth,
                MOM_12_1_last=mom_last, VOL60_last=vol60_last, ADV20_last=adv20_last)
