RULE_DEFINITIONS = {
    "close_near_sma": {
        "required": ["period"],
        "optional": ["tolerance"],
    },
    "rising_ma": {
        "required": ["ma"],
        "optional": ["lookback"],
    },
    "crossing_up": {
        "required": ["ma"],
        "optional": [],
    },
    "rsi_above": {
        "required": ["rsi", "level"],
        "optional": [],
    },
    "close_above_open": {
        "required": [],
        "optional": [],
    },
}
