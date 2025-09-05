# -*- coding: utf-8 -*-
import sqlite3
from contextlib import contextmanager
from typing import List, Tuple, Optional

from config import DB_PATH

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()

def init_db():
    with get_conn() as conn:
        c = conn.cursor()
        c.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS universe(
            symbol TEXT PRIMARY KEY,
            name TEXT,
            exchange TEXT,
            is_delisted INTEGER,
            sector TEXT,
            industry TEXT,
            market_cap REAL
        );
        CREATE TABLE IF NOT EXISTS prices_daily(
            symbol TEXT,
            date TEXT,
            close REAL,
            volume REAL,
            PRIMARY KEY(symbol, date)
        );
        CREATE INDEX IF NOT EXISTS idx_prices_date ON prices_daily(date);
        CREATE TABLE IF NOT EXISTS factors_static(
            symbol TEXT PRIMARY KEY,
            as_of TEXT,
            B2M REAL,
            EBIT_EV REAL,
            ROA_TTM REAL,
            AssetGrowthYoY REAL,
            MOM_12_1_last REAL,
            VOL60_last REAL,
            ADV20_last REAL,
            InsiderNet90d REAL,
            Sentiment30d REAL
        );
        CREATE TABLE IF NOT EXISTS betas_monthly(
            date TEXT,
            symbol TEXT,
            beta_252 REAL,
            PRIMARY KEY(date, symbol)
        );
        CREATE TABLE IF NOT EXISTS weights(
            date TEXT,
            strategy TEXT,
            symbol TEXT,
            weight REAL,
            PRIMARY KEY(date, strategy, symbol)
        );
        CREATE TABLE IF NOT EXISTS portfolio_returns(
            date TEXT,
            strategy TEXT,
            ret REAL,
            PRIMARY KEY(date, strategy)
        );
        CREATE TABLE IF NOT EXISTS performance(
            strategy TEXT PRIMARY KEY,
            CAGR REAL, AnnVol REAL, Sharpe REAL, Sortino REAL, MaxDD REAL, HitRate REAL, N INTEGER
        );
        """)

def upsert_many(table: str, rows: List[Tuple], placeholders: str):
    if not rows: return
    with get_conn() as conn:
        c = conn.cursor()
        c.executemany(f"INSERT OR REPLACE INTO {table} VALUES ({placeholders})", rows)

def latest_price_date(symbol: str) -> Optional[str]:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT MAX(date) FROM prices_daily WHERE symbol=?", (symbol,))
        row = c.fetchone()
        if row and row[0]:
            return row[0]
    return None
