# scan/engine.py

import pandas as pd
from db import get_connection

from scan.indicators import (
    add_sma,
    add_ema,
    add_rsi,
    add_macd,
)

from scan.cache import get_cached, set_cache


# =====================================================
# LOAD RAW PRICES FROM DB
# =====================================================
def load_prices(symbol: str) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT date, open, high, low, close, volume
        FROM prices
        WHERE symbol = ?
        ORDER BY date
        """,
        conn,
        params=(symbol,),
    )
    conn.close()

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    return df


# =====================================================
# TIMEFRAME CANDLES (NSE SAFE)
# =====================================================
def get_tf_candles(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    if df.empty:
        return df

    if tf == "1D":
        return df

    if tf == "1W":
        iso = df.index.isocalendar()
        out = (
            df
            .groupby([iso.year, iso.week])
            .agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            })
        )
        out.index = [
            pd.Timestamp.fromisocalendar(y, w, 5)
            for y, w in out.index
        ]
        return out.dropna()

    if tf == "1M":
        return (
            df
            .resample("ME")
            .agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            })
            .dropna()
        )

    raise ValueError(f"Unsupported timeframe: {tf}")


# =====================================================
# ATTACH INDICATORS
# =====================================================
def apply_indicators(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()  # 🔒 NEVER mutate upstream data

    for period in config.get("sma", []):
        df = add_sma(df, period)

    for period in config.get("ema", []):
        df = add_ema(df, period)

    for period in config.get("rsi", []):
        df = add_rsi(df, period)

    for fast, slow, signal in config.get("macd", []):
        df = add_macd(df, fast, slow, signal)

    return df


# =====================================================
# CORE SCAN ENGINE (CACHED + SAFE)
# =====================================================
def run_scan(
    symbols: list[str],
    timeframe: str,
    indicator_config: dict,
    rule_fn,
    min_bars: int = 50,
) -> list[str]:
    """
    rule_fn: callable(df) -> bool
    """

    if not callable(rule_fn):
        raise TypeError("rule_fn must be a callable that accepts df")

    results: list[str] = []

    for symbol in symbols:
        try:
            # ---------- CACHE ----------
            cached = get_cached(symbol, timeframe, indicator_config)

            if cached is not None:
                df = cached.copy()  # 🔒 IMPORTANT
            else:
                # ---------- LOAD ----------
                df = load_prices(symbol)
                if df.empty:
                    continue

                df = get_tf_candles(df, timeframe)
                if len(df) < min_bars:
                    continue

                df = apply_indicators(df, indicator_config)
                if len(df) < min_bars:
                    continue

                # print(symbol, df.columns.tolist()[-5:])

                set_cache(symbol, timeframe, indicator_config, df)


            # ---------- RULE ----------
            if rule_fn(df):
                results.append(symbol)

        except Exception as e:
            print(f"[SCAN ERROR] {symbol}: {e}")

    return results
