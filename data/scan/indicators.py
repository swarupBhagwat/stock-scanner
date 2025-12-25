import pandas as pd

# =====================================================
# SIMPLE MOVING AVERAGE
# =====================================================
def add_sma(df, period):
    col = f"SMA_{period}"
    df[col] = df["close"].rolling(period).mean()
    return df


# =====================================================
# EXPONENTIAL MOVING AVERAGE
# =====================================================
def add_ema(df: pd.DataFrame, length: int) -> pd.DataFrame:
    col = f"ema_{length}"

    if col not in df.columns:
        df[col] = df["close"].ewm(span=length, adjust=False).mean()

    return df


# =====================================================
# RELATIVE STRENGTH INDEX (RSI)
# =====================================================
def add_rsi(df: pd.DataFrame, length: int = 14) -> pd.DataFrame:
    col = f"rsi_{length}"

    if col in df.columns:
        return df

    delta = df["close"].diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(length).mean()
    avg_loss = loss.rolling(length).mean()

    rs = avg_gain / avg_loss
    df[col] = 100 - (100 / (1 + rs))

    return df


# =====================================================
# MACD
# =====================================================
def add_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9
) -> pd.DataFrame:

    macd_col = f"macd_{fast}_{slow}"
    signal_col = f"macd_signal_{signal}"
    hist_col = f"macd_hist"

    if macd_col in df.columns:
        return df

    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()

    df[macd_col] = ema_fast - ema_slow
    df[signal_col] = df[macd_col].ewm(span=signal, adjust=False).mean()
    df[hist_col] = df[macd_col] - df[signal_col]

    return df
