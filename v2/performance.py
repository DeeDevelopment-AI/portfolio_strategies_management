# -*- coding: utf-8 -*-
import numpy as np, pandas as pd
def perf_stats(r: pd.Series)->dict:
    r = r.dropna()
    if len(r)==0:
        return dict(CAGR=np.nan, AnnVol=np.nan, Sharpe=np.nan, Sortino=np.nan, MaxDD=np.nan, HitRate=np.nan, N=0)
    tot = (1+r).prod(); years=len(r)/12.0; cagr = tot**(1/years)-1 if years>0 else np.nan
    vol = r.std(ddof=0)*np.sqrt(12); sharpe = r.mean()/(r.std(ddof=0)+1e-12)*np.sqrt(12)
    dn = r[r<0]; sortino = r.mean()/(dn.std(ddof=0)+1e-12)*np.sqrt(12) if len(dn)>0 else np.nan
    eq = (1+r).cumprod(); peak=eq.cummax(); maxdd=(eq/peak - 1).min()
    hit = (r>0).mean()
    return dict(CAGR=cagr, AnnVol=vol, Sharpe=sharpe, Sortino=sortino, MaxDD=maxdd, HitRate=hit, N=len(r))
