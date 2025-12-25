# scan/builder.py

from scan.rules import (
    near_ma,
    rising_ma,
    crossing_up,
    rsi_above,
    rsi_below,
    AND,
    OR,
)

# =====================================================
# COLUMN RESOLVERS (CRITICAL)
# =====================================================

def sma_col(period: int) -> str:
    return f"SMA_{period}"

def ema_col(period: int) -> str:
    return f"EMA_{period}"

def rsi_col(period: int) -> str:
    return f"RSI_{period}"

def macd_cols(fast: int, slow: int, signal: int):
    base = f"{fast}_{slow}_{signal}"
    return f"MACD_{base}", f"MACD_SIGNAL_{base}"


# =====================================================
# RULE REGISTRY
# =====================================================

RULE_MAP = {

    "near_sma": lambda cfg: near_ma(
        ma_col=sma_col(cfg["period"]),
        tolerance_pct=cfg.get("tolerance_pct", 1.0),
    ),

    "rising_sma": lambda cfg: rising_ma(
        ma_col=sma_col(cfg["period"]),
        lookback=cfg.get("lookback", 3),
    ),

    "crossing_sma": lambda cfg: crossing_up(
        ma_col=sma_col(cfg["period"]),
    ),

    "rsi_above": lambda cfg: rsi_above(
        rsi_col=rsi_col(cfg["period"]),
        level=cfg["level"],
    ),

    "rsi_below": lambda cfg: rsi_below(
        rsi_col=rsi_col(cfg["period"]),
        level=cfg["level"],
    ),

 
}



# =====================================================
# BUILD RULE (RECURSIVE, SAFE)
# =====================================================

def build_rule(rule_json):
    """
    Converts rule JSON into executable rule_fn(df)

    Supported formats:
    {
      "AND": [rule, rule]
    }
    {
      "OR": [rule, rule]
    }
    {
      "near_sma": { "period": 50, "tolerance_pct": 1 }
    }
    """

    if not isinstance(rule_json, dict):
        raise ValueError("Rule must be a dictionary")

    # ---------- AND ----------
    if "AND" in rule_json:
        rules = [build_rule(r) for r in rule_json["AND"]]
        return AND(*rules)

    # ---------- OR ----------
    if "OR" in rule_json:
        rules = [build_rule(r) for r in rule_json["OR"]]
        return OR(*rules)

    # ---------- LEAF ----------
    if len(rule_json) != 1:
        raise ValueError(f"Invalid rule structure: {rule_json}")

    rule_name, cfg = next(iter(rule_json.items()))

    if rule_name not in RULE_MAP:
        raise ValueError(f"Unknown rule: {rule_name}")

    return RULE_MAP[rule_name](cfg)
