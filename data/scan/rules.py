# scan/rules.py
import pandas as pd

# =====================================================
# INTERNAL HELPERS
# =====================================================

def _last(df: pd.DataFrame):
    if len(df) < 1:
        return None
    return df.iloc[-1]


def _prev(df: pd.DataFrame):
    if len(df) < 2:
        return None
    return df.iloc[-2]


# =====================================================
# PRICE NEAR MOVING AVERAGE
# =====================================================

def near_ma(*, ma_col: str, tolerance_pct: float = 1.0):
    """
    Price within ± tolerance_pct of MA column
    Example:
        near_ma(ma_col="SMA_50", tolerance_pct=5)
    """
    def _rule(df: pd.DataFrame) -> bool:
        last = _last(df)
        if last is None:
            return False

        if ma_col not in df.columns:
            return False

        ma = last[ma_col]
        if pd.isna(ma) or ma == 0:
            return False

        price = last["close"]
        diff_pct = abs(price - ma) / ma * 100
        return diff_pct <= tolerance_pct

    return _rule


# =====================================================
# MOVING AVERAGE RISING
# =====================================================

def rising_ma(*, ma_col: str, lookback: int = 3):
    """
    MA rising for last N candles
    """
    def _rule(df: pd.DataFrame) -> bool:
        if ma_col not in df.columns:
            return False

        if len(df) < lookback + 1:
            return False

        recent = df[ma_col].tail(lookback + 1)
        if recent.isna().any():
            return False

        return all(
            recent.iloc[i] > recent.iloc[i - 1]
            for i in range(1, len(recent))
        )

    return _rule


# =====================================================
# PRICE CROSSING ABOVE MA
# =====================================================

def crossing_up(*, ma_col: str):
    def _rule(df: pd.DataFrame) -> bool:
        last = _last(df)
        prev = _prev(df)

        if last is None or prev is None:
            return False

        if ma_col not in df.columns:
            return False

        if pd.isna(last[ma_col]) or pd.isna(prev[ma_col]):
            return False

        return (
            prev["close"] < prev[ma_col] and
            last["close"] > last[ma_col]
        )

    return _rule


# =====================================================
# RSI RULES
# =====================================================

def rsi_above(*, rsi_col: str, level: float):
    def _rule(df: pd.DataFrame) -> bool:
        last = _last(df)
        if last is None:
            return False

        if rsi_col not in df.columns:
            return False

        return last[rsi_col] > level

    return _rule


def rsi_below(*, rsi_col: str, level: float):
    def _rule(df: pd.DataFrame) -> bool:
        last = _last(df)
        if last is None:
            return False

        if rsi_col not in df.columns:
            return False

        return last[rsi_col] < level

    return _rule


# =====================================================
# MACD RULES
# =====================================================

def macd_bullish(*, macd_col: str, signal_col: str):
    def _rule(df: pd.DataFrame) -> bool:
        last = _last(df)
        prev = _prev(df)

        if last is None or prev is None:
            return False

        if macd_col not in df.columns or signal_col not in df.columns:
            return False

        return (
            prev[macd_col] <= prev[signal_col] and
            last[macd_col] > last[signal_col]
        )

    return _rule


# =====================================================
# PRICE ACTION RULES
# =====================================================

def close_above_open():
    def _rule(df: pd.DataFrame) -> bool:
        last = _last(df)
        if last is None:
            return False
        return last["close"] > last["open"]
    return _rule


def close_above_prev_close():
    def _rule(df: pd.DataFrame) -> bool:
        last = _last(df)
        prev = _prev(df)
        if last is None or prev is None:
            return False
        return last["close"] > prev["close"]
    return _rule


def close_near_high(*, tolerance_pct: float = 1.0):
    def _rule(df: pd.DataFrame) -> bool:
        last = _last(df)
        if last is None:
            return False

        high = last["high"]
        close = last["close"]

        if high == 0:
            return False

        diff_pct = abs(high - close) / high * 100
        return diff_pct <= tolerance_pct

    return _rule


def range_above_pct(*, min_pct: float):
    def _rule(df: pd.DataFrame) -> bool:
        last = _last(df)
        if last is None:
            return False

        rng = last["high"] - last["low"]
        pct = (rng / last["close"]) * 100
        return pct >= min_pct

    return _rule


# =====================================================
# USER-FRIENDLY SMA RULE (WRAPPER)
# =====================================================

def close_near_sma(*, period: int, tolerance_pct: float = 1.0):
    """
    Latest close price is within ± tolerance_pct of SMA(period)
    """
    sma_col = f"SMA_{period}"

    def _rule(df: pd.DataFrame) -> bool:
        # Column must exist
        if sma_col not in df.columns:
            return False

        # Need at least one valid row
        if len(df) == 0:
            return False

        last = df.iloc[-1]

        # SMA must be valid
        if pd.isna(last[sma_col]):
            return False

        close_price = last["close"]
        sma_value = last[sma_col]

        diff_pct = abs(close_price - sma_value) / sma_value * 100

        return diff_pct <= tolerance_pct

    return _rule




# =====================================================
# LOGICAL COMBINATORS (NESTABLE)
# =====================================================

def AND(*rules):
    def _and(df: pd.DataFrame) -> bool:
        return all(rule(df) for rule in rules)
    return _and


def OR(*rules):
    def _or(df: pd.DataFrame) -> bool:
        return any(rule(df) for rule in rules)
    return _or
