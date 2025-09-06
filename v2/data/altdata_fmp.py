# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from clients.fmp_client import fmp_get

def insider_net_90d(symbol: str) -> float | None:
    since = (datetime.utcnow().date() - timedelta(days=90)).isoformat()
    data = fmp_get("/api/v4/insider-trading", params={"symbol": symbol, "from": since}) or []
    df = pd.DataFrame(data)
    if df.empty: return np.nan
    buys = df[df.get("acqDispCode","")== "A"]["securitiesTransacted"].sum() if "acqDispCode" in df else 0
    sells= df[df.get("acqDispCode","")== "D"]["securitiesTransacted"].sum() if "acqDispCode" in df else 0
    try:
        return float(buys) - float(sells)
    except Exception:
        return np.nan

def sentiment_30d(symbol: str) -> float | None:
    since = (datetime.utcnow().date() - timedelta(days=30)).isoformat()
    for path in ["/api/v4/historical/social-sentiment",
                 "/api/v4/social-sentiment",
                 "/api/v3/historical/social-sentiment"]:
        data = fmp_get(path, params={"symbol": symbol, "from": since}) or []
        df = pd.DataFrame(data)
        if not df.empty:
            for col in ["sentiment","score","sentimentScore"]:
                if col in df.columns:
                    try:
                        return float(df[col].astype(float).mean())
                    except Exception:
                        pass
    return np.nan
