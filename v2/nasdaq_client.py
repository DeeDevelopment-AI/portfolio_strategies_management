# -*- coding: utf-8 -*-
import os, time, json, logging, requests
from typing import Optional, Dict, Any
from http_cache import cache_get, cache_set

NDL_BASE = "https://data.nasdaq.com/api/v3"
API_ENV_KEYS = ["NDL_API_KEY", "NASDAQ_API_KEY", "QUANDL_API_KEY"]

def _api_key()->Optional[str]:
    for k in API_ENV_KEYS:
        if os.getenv(k): return os.getenv(k)
    return None

def ndl_get(path: str, params: Optional[Dict[str, Any]]=None):
    params = params.copy() if params else {}
    key = _api_key()
    if key and "api_key" not in params:
        params["api_key"]=key
    url = path if path.startswith("http") else f"{NDL_BASE}{path}"
    cached = cache_get("GET", url, params)
    if cached is not None: return cached
    for attempt in range(4):
        try:
            r = requests.get(url, params=params, timeout=30, verify=False)
            if r.status_code==200:
                try:
                    payload=r.json()
                except Exception:
                    payload=json.loads(r.text)
                cache_set("GET", url, params, payload)
                return payload
            if r.status_code in (429,502,503,504):
                time.sleep(1.0*(attempt+1))
            else:
                logging.warning("NDL non-200 %s: %s", r.status_code, r.text[:200])
                time.sleep(0.8*(attempt+1))
        except Exception as e:
            logging.warning("NDL error %s: %s", url, e)
            time.sleep(1.0*(attempt+1))
    return None
