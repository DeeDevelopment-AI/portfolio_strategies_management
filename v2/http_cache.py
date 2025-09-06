# -*- coding: utf-8 -*-
import os, json, hashlib, time
from typing import Optional, Dict, Any

CACHE_DIR = os.getenv("HTTP_CACHE_DIR", "utils/.http_cache")
CACHE_TTL = int(os.getenv("HTTP_CACHE_TTL", "86400"))  # 1 día

os.makedirs(CACHE_DIR, exist_ok=True)

def _key(method: str, url: str, params: Optional[Dict[str, Any]]):
    src = json.dumps({"m":method.upper(),"u":url,"p":params or {}}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(src.encode("utf-8")).hexdigest()

def cache_get(method: str, url: str, params: Optional[Dict[str, Any]]):
    path = os.path.join(CACHE_DIR, _key(method, url, params)+".json")
    try:
        if os.path.exists(path) and (time.time() - os.path.getmtime(path)) <= CACHE_TTL:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return None
    return None

def cache_set(method: str, url: str, params: Optional[Dict[str, Any]], payload: dict):
    path = os.path.join(CACHE_DIR, _key(method, url, params)+".json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception:
        pass
