"""
===============================================================================
CHART ROUTER
File: routers/chart.py
===============================================================================

PURPOSE
-------
Provides OHLC + volume chart data for frontend.

URLS
----
GET /chart?symbol=RELIANCE&tf=1D
===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import pandas as pd
from fastapi import APIRouter, Query

from db import get_connection

# =============================================================================
# ROUTER INITIALIZATION
# =============================================================================

router = APIRouter(prefix="", tags=["Chart"])

# =============================================================================
# CHART API
# =============================================================================

@router.get("/chart")
def get_chart(
    symbol: str,
    tf: str = Query("1D", enum=["1D", "1W", "1M"]),
    limit: int = 1500,  # 🔥 safety limit
):
    """
    Returns OHLC + volume chart data for a symbol.

    Params
    ------
    symbol : stock symbol (RELIANCE)
    tf     : timeframe (1D / 1W / 1M)
    limit  : max candles returned (performance)
    """

    conn = get_connection()

    df = pd.read_sql_query(
        """
        SELECT date, open, high, low, close, volume
        FROM prices
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT ?
        """,
        conn,
        params=(symbol, limit),
    )

    conn.close()

    if df.empty:
        return {
            "symbol": symbol,
            "tf": tf,
            "bars": 0,
            "data": {},
        }

    # Reverse back to ascending order
    df = df.iloc[::-1]

    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    # ================= TIMEFRAME AGGREGATION =================

    if tf == "1D":
        out = df

    elif tf == "1W":
        iso = df.index.isocalendar()
        out = df.groupby([iso.year, iso.week]).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        })
        out.index = [
            pd.Timestamp.fromisocalendar(y, w, 5)
            for y, w in out.index
        ]

    else:  # 1M
        out = df.resample("ME").agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        })

    out = out.dropna()

    return {
        "symbol": symbol,
        "tf": tf,
        "bars": len(out),
        "data": {
            "date": out.index.strftime("%Y-%m-%d").tolist(),
            "open": out["open"].tolist(),
            "high": out["high"].tolist(),
            "low": out["low"].tolist(),
            "close": out["close"].tolist(),
            "volume": out["volume"].tolist(),
        },
    }
