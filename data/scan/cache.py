# scan/cache.py
import time
import hashlib
import pandas as pd
from typing import Optional

# =====================================================
# IN-MEMORY CACHE
# =====================================================
_CACHE: dict[str, pd.DataFrame] = {}
_CACHE_TS: dict[str, float] = {}

DEFAULT_TTL = 60 * 30  # 30 minutes


# =====================================================
# HELPERS
# =====================================================
def _hash_config(config: dict) -> str:
    """
    Stable hash for indicator config
    """
    if not config:
        return "no-indicators"

    # sort deeply for stability
    items = []
    for k in sorted(config.keys()):
        v = config[k]
        if isinstance(v, list):
            items.append((k, tuple(v)))
        else:
            items.append((k, v))

    raw = repr(items)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def make_cache_key(symbol: str, tf: str, indicator_config: dict) -> str:
    cfg_hash = _hash_config(indicator_config)
    return f"{symbol}|{tf}|{cfg_hash}"


# =====================================================
# CACHE API
# =====================================================
def get_cached(symbol: str, tf: str, indicator_config: dict):
    key = make_cache_key(symbol, tf, indicator_config)

    if key not in _CACHE:
        return None

    ts = _CACHE_TS.get(key, 0)
    if time.time() - ts > DEFAULT_TTL:
        print(f"[CACHE EXPIRED] {symbol} {tf}")
        _CACHE.pop(key, None)
        _CACHE_TS.pop(key, None)
        return None

    print(f"[CACHE HIT] {symbol} {tf}")
    return _CACHE[key]


def set_cache(symbol: str, tf: str, indicator_config: dict, df: pd.DataFrame):
    key = make_cache_key(symbol, tf, indicator_config)
    _CACHE[key] = df
    _CACHE_TS[key] = time.time()
    print(f"[CACHE SET] {symbol} {tf}")



def clear_cache():
    _CACHE.clear()
    _CACHE_TS.clear()


def cache_stats():
    return {
        "entries": len(_CACHE),
        "ttl_seconds": DEFAULT_TTL
    }
