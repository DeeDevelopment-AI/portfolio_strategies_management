"""Portfolio construction helpers."""

import numpy as np
import pandas as pd


def next_month_returns(m_close: pd.DataFrame) -> pd.DataFrame:
    """Compute forward one-month returns.

    The previous implementation relied on ``pct_change().shift(-1)``. In some
    scenarios (e.g. integer-typed data or unsorted indices) this produced
    arrays of zeros because the percentage change was evaluated *before* the
    shift.  By explicitly shifting the close prices forward and then dividing by
    the current prices we avoid those edge cases and always obtain the return
    for the next period aligned with the starting month.

    Parameters
    ----------
    m_close : pd.DataFrame
        Monthly close prices indexed by date.

    Returns
    -------
    pd.DataFrame
        DataFrame of next month's returns with the same shape as ``m_close``.
    """

    return m_close.shift(-1).divide(m_close) - 1
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
