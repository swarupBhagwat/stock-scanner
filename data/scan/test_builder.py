print(">>> test_builder started")

from scan.engine import run_scan
from scan.rules import close_near_sma
from scan.utils import required_bars
from db import get_connection
from scan.cache import clear_cache

# -----------------------------------------------------
# CLEAR CACHE (IMPORTANT FOR TESTING)
# -----------------------------------------------------
clear_cache()

# -----------------------------------------------------
# LOAD FULL DATASET (ALL NSE STOCKS)
# -----------------------------------------------------
def get_all_symbols():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM symbols WHERE active = 1")
    symbols = [r[0] for r in cur.fetchall()]
    conn.close()
    return symbols


symbols = get_all_symbols()
print(f"Scanning {len(symbols)} stocks")

# -----------------------------------------------------
# USER-LIKE SCAN CONFIG
# -----------------------------------------------------
timeframe = "1D"

indicator_config = {
    "sma": [20],
}

min_bars = required_bars(indicator_config)

rule_fn = close_near_sma(
    period=20,
    tolerance_pct=1.0
)

# -----------------------------------------------------
# RUN SCAN
# -----------------------------------------------------
results = run_scan(
    symbols=symbols,
    timeframe=timeframe,
    indicator_config=indicator_config,
    rule_fn=rule_fn,
    min_bars=min_bars,
)

print("RESULTS :", results)
print("TOTAL MATCHES:", len(results))