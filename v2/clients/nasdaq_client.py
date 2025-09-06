# nasdaq_client.py
import time, json, logging, requests, os
from typing import Optional, Dict, Any
from http_cache import cache_get, cache_set

NDL_BASE = "https://data.nasdaq.com/api/v3"
API_ENV_KEYS = ["NDL_API_KEY", "NASDAQ_API_KEY", "QUANDL_API_KEY"]

def _get_api_key() -> Optional[str]:
    for k in API_ENV_KEYS:
        v = os.getenv(k)
        if v:
            return v
    return None

def ndl_get(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
    params = params.copy() if params else {}
    key = _get_api_key()
    if key and "api_key" not in params:
        params["api_key"] = key

    url = path if path.startswith("http") else f"{NDL_BASE}{path}"

    cached = cache_get("GET", url, params)
    if cached is not None:
        # Debug summary for cached hits
        if "datatable" in cached:
            rows = len(cached["datatable"].get("data", []))
            logging.debug("NDL (cached) %s rows=%s meta=%s", url, rows, cached.get("meta"))
        return cached

    for attempt in range(4):
        try:
            r = requests.get(url, params=params, timeout=30, verify=False)
            logging.debug("NDL GET %s | status=%s | params=%s", r.url, r.status_code, params)

            if r.status_code == 200:
                try:
                    payload = r.json()
                except Exception:
                    payload = json.loads(r.text)

                # Surface Quandl/Nasdaq logical errors even on 200
                qerr = payload.get("quandl_error")
                if qerr:
                    logging.warning("NDL quandl_error code=%s msg=%s", qerr.get("code"), qerr.get("message"))

                if "datatable" in payload:
                    rows = len(payload["datatable"].get("data", []))
                    logging.debug("NDL 200 datatable rows=%s meta=%s", rows, payload.get("meta"))
                else:
                    logging.debug("NDL 200 (non-datatable) keys=%s", list(payload.keys())[:6])

                cache_set("GET", url, params, payload)
                return payload

            # Non-200: show short body
            body = (r.text or "")[:300]
            logging.warning("NDL non-200 %s: %s", r.status_code, body)
            if r.status_code in (429, 502, 503, 504):
                time.sleep(1.0 * (attempt + 1))
            else:
                break
        except Exception as e:
            logging.warning("NDL req error: %s", e)
            time.sleep(1.0 * (attempt + 1))
    return None

