# Factor Study Pro v2 — NDL + FMP + Dashboard

- **Precios (EOD)**: **Sharadar SEP** vía **Nasdaq Data Link (NDL)**.
- **Fundamentales**: **Sharadar SF1** (NDL) para B2M, ROA TTM, EBIT/EV, Asset Growth YoY.
- **Alt‑data**: **FMP** (Insider net 90d, Sentiment 30d).
- **Neutralización**: Industria + tamaño (log mcap); winsorize + zscore.
- **Carteras**: long‑only top 10%, long–short beta‑neutral, composite.
- **SQLite** con universo, precios, factores, betas, pesos, retornos, performance.
- **Caché HTTP** y **actualización incremental** de precios.
- **Dashboard Streamlit** conectado a la BBDD.

## Instalar
```bash
pip install -r requirements.txt
# Claves
export NDL_API_KEY="..."           # o NASDAQ_API_KEY / QUANDL_API_KEY
export FMP_API_KEY="..."
# Caché (opcional)
export HTTP_CACHE_DIR="./.http_cache"
export HTTP_CACHE_TTL=86400
```

## Ejecutar estudio
```bash
python run_study.py --start 2015-01-01 --end 2025-08-31 --universe-size 500 --include-delisted 1 --log INFO
```

## Dashboard
```bash
streamlit run dashboard/app.py
```

> Nota: si tu plan no expone alguna métrica en SF1/SEP, el pipeline rellena NaN y sigue funcionando.
