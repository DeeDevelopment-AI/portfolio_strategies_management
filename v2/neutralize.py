# -*- coding: utf-8 -*-
import numpy as np, pandas as pd
def winsorize(s: pd.Series, p: float = 0.01)->pd.Series:
    if s.dropna().empty: return s
    lo, hi = s.quantile(p), s.quantile(1-p)
    return s.clip(lower=lo, upper=hi)
def zscore(s: pd.Series)->pd.Series:
    s = s.astype(float)
    return (s - s.mean())/(s.std(ddof=0)+1e-12)
def residualize_industry_size(f: pd.Series, industries: pd.Series, log_mcap: pd.Series|None)->pd.Series:
    idx = f.dropna().index
    if len(idx)==0: return f
    y = f.loc[idx].astype(float).values.reshape(-1,1)
    d = pd.get_dummies(industries.reindex(idx).fillna("UNK"))
    if d.shape[1]>1: d = d.iloc[:,1:]
    import numpy as np
    X = np.ones((len(idx),1))
    if d.shape[1]>0: X = np.concatenate([X, d.values], axis=1)
    if log_mcap is not None:
        X = np.concatenate([X, log_mcap.reindex(idx).fillna(log_mcap.reindex(idx).median()).values.reshape(-1,1)], axis=1)
    XtX = X.T @ X
    try:
        beta = np.linalg.inv(XtX) @ X.T @ y
    except np.linalg.LinAlgError:
        beta = np.linalg.pinv(XtX) @ X.T @ y
    resid = (y - X @ beta).ravel()
    return pd.Series(resid, index=idx).pipe(zscore).reindex(f.index)
