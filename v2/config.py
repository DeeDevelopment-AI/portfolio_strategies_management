# -*- coding: utf-8 -*-
import os
from datetime import datetime

OUT_DIR = os.getenv("OUT_DIR", "./out")
DB_PATH  = os.getenv("DB_PATH", "./factor_study.db")

DEFAULT_START = "2015-01-01"
DEFAULT_END   = datetime.utcnow().date().isoformat()

EXCHANGES = {"NYSE","NASDAQ","AMEX"}

TOP_Q = 0.10
BOTTOM_Q = 0.10
MIN_LIQ_PCTL = 0.20
BETA_WINDOW_D = 252
