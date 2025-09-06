import pandas as pd
import numpy as np
from ..portfolio import next_month_returns

def test_next_month_returns_basic():
    prices = pd.DataFrame({
        'A': [100, 110, 120],
        'B': [50, 55, 50]
    }, index=pd.date_range('2020-01-31', periods=3, freq='M'))
    expected = prices.shift(-1).divide(prices) - 1
    result = next_month_returns(prices)
    pd.testing.assert_frame_equal(result, expected)