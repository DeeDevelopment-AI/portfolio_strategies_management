# -*- coding: utf-8 -*-
import time, json, logging, requests, os
from typing import Optional, Dict, Any
from http_cache import cache_get, cache_set

BASE_URL = "https://financialmodelingprep.com"
API_ENV_KEYS = ["FMP_API_KEY", "FMP_KEY", "FMP_TOKEN"]
RATE_LIMIT_QPS = float(os.getenv("FMP_QPS", "4"))

_last=[0.0]
def _throttle():
    import time as _t
    now=_t.time()
    if _last[0]:
        dt = now - _last[0]
        need = 1.0/max(RATE_LIMIT_QPS,1e-6)
        if dt < need:
            _t.sleep(need-dt)
    _last[0]=_t.time()

def _get_api_key() -> Optional[str]:
    """Return the first Financial Modeling Prep API key found in the environment."""
    for k in API_ENV_KEYS:
        v = os.getenv(k)
        if v:
            return v
    return None

def fmp_get(path: str, params: Optional[Dict[str, Any]] = None):
    params = params.copy() if params else {}
    key = _get_api_key()
    if key and "apikey" not in params:
        params["apikey"] = key
    url = path if path.startswith("http") else f"{BASE_URL}{path}"
    cached = cache_get("GET", url, params)
    if cached is not None:
        return cached
    for attempt in range(5):
        try:
            _throttle()
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
                logging.warning("FMP non-200 %s: %s", r.status_code, r.text[:200])
                time.sleep(0.8*(attempt+1))
        except Exception as e:
            logging.warning("FMP error %s: %s", url, e)
            time.sleep(1.0*(attempt+1))
    return None
