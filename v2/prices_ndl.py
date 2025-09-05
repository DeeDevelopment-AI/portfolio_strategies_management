# -*- coding: utf-8 -*-
import pandas as pd
from typing import Optional, Dict
from nasdaq_client import ndl_get
from db import upsert_many

def _datatable_to_df(obj: dict) -> pd.DataFrame:
    if not obj or "datatable" not in obj:
        return pd.DataFrame()
    cols = [c["name"] for c in obj["datatable"]["columns"]]
    data = obj["datatable"]["data"]
    return pd.DataFrame(data, columns=cols)

def get_eod_prices_ndl(symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
    """
    Descarga precios EOD de SHARADAR/SEP usando paginación por cursor.
    - Sin 'order' (ordenamos en cliente).
    - Sin 'qopts.page' (usamos 'qopts.cursor_id').
    """
    frames = []
    cursor_id = None

    base_params = {
        "ticker": symbol,
        "date.gte": start,
        "date.lte": end,
        "qopts.columns": "ticker,date,close,volume",
        "qopts.per_page": 10000,
    }

    while True:
        params = dict(base_params)
        if cursor_id:
            params["qopts.cursor_id"] = cursor_id

        obj = ndl_get("/datatables/SHARADAR/SEP", params=params)
        df = _datatable_to_df(obj)
        if df.empty:
            break

        frames.append(df)

        # cursor para la siguiente página
        meta = (obj or {}).get("meta", {}) or {}
        cursor_id = meta.get("next_cursor_id")
        if not cursor_id:
            break

    if not frames:
        return None

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date").set_index("date")
    return out[["close", "volume"]].astype(float)


def persist_prices(price_map: Dict[str, pd.DataFrame]):
    rows = []
    for s, df in price_map.items():
        for dt, row in df.iterrows():
            rows.append((s, dt.strftime("%Y-%m-%d"), float(row["close"]), float(row["volume"])))
    upsert_many("prices_daily", rows, "?,?,?,?")
