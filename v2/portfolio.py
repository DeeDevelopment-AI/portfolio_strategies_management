# -*- coding: utf-8 -*-
import numpy as np, pandas as pd
def next_month_returns(m_close: pd.DataFrame)->pd.DataFrame:
    return m_close.pct_change().shift(-1)
def beta_rolling_daily(df_close: pd.DataFrame, mkt_close: pd.Series, window: int=252)->pd.DataFrame:
    rets = df_close.pct_change(); mret = mkt_close.pct_change()
    betas = pd.DataFrame(index=df_close.index, columns=df_close.columns, dtype=float)
    for t in range(window, len(df_close)):
        s = df_close.index[t-window+1]; e = df_close.index[t]
        covs = rets.loc[s:e].apply(lambda col: col.cov(mret.loc[s:e]))
        varm = mret.loc[s:e].var()
        betas.iloc[t] = covs/(varm+1e-12)
    return betas
def build_long_only(z, adv20, min_liq_pctl=0.2, top_q=0.1):
    thr = adv20.quantile(min_liq_pctl); z_elig = z.where(adv20>=thr)
    r = z_elig.rank(pct=True, method="first"); sel = r >= (1-top_q)
    if sel.sum()==0: return pd.Series(0.0, index=z.index)
    return (sel.astype(float)/sel.sum()).fillna(0.0)
def build_long_short_beta_neutral(z, betas, adv20, min_liq_pctl=0.2, top_q=0.1, bottom_q=0.1, gross=1.0):
    thr = adv20.quantile(min_liq_pctl); z_elig = z.where(adv20>=thr)
    r = z_elig.rank(pct=True, method="first"); L = r>=(1-top_q); S = r<=bottom_q
    if L.sum()==0 or S.sum()==0: return pd.Series(0.0, index=z.index)
    wL = L.astype(float)/L.sum(); wS = S.astype(float)/S.sum()
    betaL = (wL*betas).sum(); betaS=(wS*betas).sum()
    a = abs(betaS)/(abs(betaL)+1e-12); b=abs(betaL)/(abs(betaS)+1e-12); scale = gross/(a+b+1e-12)
    return (a*scale*wL - b*scale*wS).fillna(0.0)
def portfolio_returns_from_weights(w_panel, m_rets):
    out={}
    for dt,w in w_panel.items():
        if dt in m_rets.index:
            out[dt]=float((w.reindex(m_rets.columns).fillna(0.0)*m_rets.loc[dt]).sum())
    return pd.Series(out).sort_index()
