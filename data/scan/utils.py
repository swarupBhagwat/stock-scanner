# scan/utils.py

"""
Utility helpers for scan engine
"""

def required_bars(indicator_config: dict) -> int:
    """
    Calculate minimum bars required based on indicators

    Example:
        SMA 50  -> needs 50 bars
        RSI 14  -> needs 14 bars
        MACD(12,26,9) -> needs 26 bars
    """

    bars = 0

    # SMA
    for p in indicator_config.get("sma", []):
        bars = max(bars, p)

    # EMA
    for p in indicator_config.get("ema", []):
        bars = max(bars, p)

    # RSI
    for p in indicator_config.get("rsi", []):
        bars = max(bars, p)

    # MACD
    for fast, slow, signal in indicator_config.get("macd", []):
        bars = max(bars, slow)

    # add buffer for safety
    return bars + 5
