# prices_ndl.py
import logging
import pandas as pd
from typing import Optional, Dict, List
from nasdaq_client import ndl_get
from db import upsert_many

def _datatable_to_df(obj: dict) -> pd.DataFrame:
    if not obj or "datatable" not in obj:
        return pd.DataFrame()
    cols = [c["name"] for c in obj["datatable"]["columns"]]
    data = obj["datatable"]["data"]
    return pd.DataFrame(data, columns=cols)


def get_eod_prices_ndl(symbol: str, start: str, end: str, batch_days: int = 250) -> Optional[pd.DataFrame]:
    """
    Fetch EOD from SHARADAR/SEP using the 'date=' filter (comma-separated list).
    If no data comes back, automatically fall back to cursor pagination with
    'date.gte/date.lte'. Emits detailed DEBUG logs.
    """
    symbol = (symbol or "").upper().strip()
    logging.debug("[SEP] start symbol=%s start=%s end=%s", symbol, start, end)

    # ---------- Strategy A: date list (your browser pattern) ----------
    frames: List[pd.DataFrame] = []

    all_dates = pd.date_range(start=start, end=end, freq="D")
    logging.debug("[SEP] date-list mode: total_days=%d batch_days=%d", len(all_dates), batch_days)


    for i in range(0, len(all_dates), batch_days):
        chunk = all_dates[i:i + batch_days]
        if len(chunk) == 0:
            continue

        dates_param = ",".join(d.strftime("%Y-%m-%d") for d in chunk)
        params = {
            "ticker": symbol,
            "date": dates_param, # EXACTLY like your examples

            "qopts.columns": "ticker,date,close,volume",
            "qopts.per_page": 10000,
        }
        obj = ndl_get("/datatables/SHARADAR/SEP", params=params)
        if obj is None:
            logging.debug("[SEP] date-list batch %s..%s -> obj=None", chunk[0].date(), chunk[-1].date())
            continue

        qerr = obj.get("quandl_error")
        if qerr:
            logging.warning("[SEP] date-list quandl_error code=%s msg=%s", qerr.get("code"), qerr.get("message"))

        df = _datatable_to_df(obj)
        logging.debug("[SEP] date-list batch %s..%s -> rows=%d", chunk[0].date(), chunk[-1].date(), len(df))
        if not df.empty:
            frames.append(df)

    if frames:

        out = pd.concat(frames, ignore_index=True).drop_duplicates()
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date"]).sort_values("date").set_index("date")
        out["close"] = pd.to_numeric(out["close"], errors="coerce")
        out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
        logging.debug("[SEP] date-list total rows=%d first_dt=%s last_dt=%s",
                      len(out), out.index.min().date(), out.index.max().date())

        return out[["close", "volume"]]
    logging.debug("[SEP] date-list mode returned 0 rows for %s. Falling back to cursor mode...", symbol)

    # ---------- Strategy B: cursor pagination (date.gte / date.lte) ----------

    frames = []
    cursor_id = None
    pages = 0
    while True:

        params = {
            "ticker": symbol,
            "date.gte": start,
            "date.lte": end,
            "qopts.columns": "ticker,date,close,volume",
            "qopts.per_page": 10000,
        }
        if cursor_id:
            params["qopts.cursor_id"] = cursor_id
        obj = ndl_get("/datatables/SHARADAR/SEP", params=params)
        if obj is None:
            logging.debug("[SEP] cursor page=%d -> obj=None", pages + 1)
            break

        qerr = obj.get("quandl_error")
        if qerr:
            logging.warning("[SEP] cursor quandl_error code=%s msg=%s", qerr.get("code"), qerr.get("message"))
            break

        df = _datatable_to_df(obj)
        rows = len(df)

        meta = (obj or {}).get("meta", {}) or {}
        cursor_id = meta.get("next_cursor_id")
        pages += 1
        logging.debug("[SEP] cursor page=%d rows=%d next_cursor_id=%s", pages, rows, cursor_id)

        if rows:
            frames.append(df)
        if not cursor_id:
            break

        # safety valve: avoid infinite loops
        if pages > 200:
            logging.warning("[SEP] cursor mode aborted after 200 pages for %s", symbol)
        break

    if not frames:
        logging.warning("[SEP] No rows returned for %s in [%s .. %s] (both modes). "
                        "Check ticker validity, subscription access to SHARADAR/SEP, and API key.",
                        symbol, start, end)

        return None

    out = pd.concat(frames, ignore_index=True).drop_duplicates()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date").set_index("date")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
    logging.debug("[SEP] cursor total rows=%d first_dt=%s last_dt=%s",
                  len(out), out.index.min().date(), out.index.max().date())

    return out[["close", "volume"]]


def persist_prices(price_map: Dict[str, pd.DataFrame]):
    rows = []
    for s, df in price_map.items():
        for dt, row in df.iterrows():
            rows.append((s, dt.strftime("%Y-%m-%d"), float(row["close"]), float(row["volume"])))
    upsert_many("prices_daily", rows, "?,?,?,?")
