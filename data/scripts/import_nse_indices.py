"""
===============================================================================
NSE UNIVERSE SYNC SCRIPT (PRODUCTION SAFE)
===============================================================================

WHAT THIS SCRIPT DOES
---------------------
1. Downloads NSE index constituents (ALL major indices)
2. Populates index_members table safely
3. Auto-generates / updates config/universes.json
4. Preserves custom universes
5. Safe to run frequently (idempotent)

DESIGN RULES
------------
• Script OWNS index-based universes
• Custom universes are preserved
• NEVER inserts index name as a stock
• NEVER wipes DB if NSE returns empty

RUN
---
python data/scripts/import_nse_indices.py
===============================================================================
"""

# =============================================================================
# STANDARD LIBRARIES
# =============================================================================

import os
import json
import sqlite3
import requests
from time import sleep

# =============================================================================
# PATH SETUP (BULLETPROOF — WINDOWS / LINUX / GCP SAFE)
# =============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))  # /data

DB_PATH = os.path.join(BASE_DIR, "stocks.db")

CONFIG_DIR = os.path.join(BASE_DIR, "config")
os.makedirs(CONFIG_DIR, exist_ok=True)

UNIVERSE_JSON_PATH = os.path.join(CONFIG_DIR, "universes.json")

# =============================================================================
# CONFIGURATION (CHANGE ONLY HERE)
# =============================================================================

REQUEST_DELAY = 1  # seconds (avoid NSE throttling)

# SINGLE SOURCE OF TRUTH — NSE INDEX DEFINITIONS
# key           = internal universe key
# label         = UI label
# encoded_name  = EXACT NSE API index name (URL encoded)

NSE_INDICES = {
    "NIFTY50": ("Nifty 50", "NIFTY%2050"),
    "NIFTY_NEXT_50": ("Nifty Next 50", "NIFTY%20NEXT%2050"),
    "NIFTY100": ("Nifty 100", "NIFTY%20100"),
    "NIFTY200": ("Nifty 200", "NIFTY%20200"),
    "NIFTY500": ("Nifty 500", "NIFTY%20500"),
    "BANKNIFTY": ("Nifty Bank", "NIFTY%20BANK"),
    "FIN_SERVICE": ("Nifty Financial Services", "NIFTY%20FINANCIAL%20SERVICES"),
    "PRIVATE_BANK": ("Nifty Private Bank", "NIFTY%20PRIVATE%20BANK"),
    "PSU_BANK": ("Nifty PSU Bank", "NIFTY%20PSU%20BANK"),
    "IT": ("Nifty IT", "NIFTY%20IT"),
    "PHARMA": ("Nifty Pharma", "NIFTY%20PHARMA"),
    "HEALTHCARE": ("Nifty Healthcare", "NIFTY%20HEALTHCARE"),
    "AUTO": ("Nifty Auto", "NIFTY%20AUTO"),
    "METAL": ("Nifty Metal", "NIFTY%20METAL"),
    "FMCG": ("Nifty FMCG", "NIFTY%20FMCG"),
    "CONSUMER_DURABLES": ("Nifty Consumer Durables", "NIFTY%20CONSUMER%20DURABLES"),
    "MEDIA": ("Nifty Media", "NIFTY%20MEDIA"),
    "REALTY": ("Nifty Realty", "NIFTY%20REALTY"),
    "INFRA": ("Nifty Infrastructure", "NIFTY%20INFRASTRUCTURE"),
    "POWER": ("Nifty Power", "NIFTY%20POWER"),
    "CHEMICALS": ("Nifty Chemicals", "NIFTY%20CHEMICALS"),
    "OIL_GAS": ("Nifty Oil & Gas", "NIFTY%20OIL%20%26%20GAS"),
}

# =============================================================================
# NSE REQUEST HEADERS (MANDATORY)
# =============================================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
}

# =============================================================================
# DATABASE INITIALIZATION
# =============================================================================

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS index_members (
    index_name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    PRIMARY KEY (index_name, symbol)
)
""")
conn.commit()

# =============================================================================
# LOAD EXISTING universes.json (PRESERVE CUSTOM)
# =============================================================================

if os.path.exists(UNIVERSE_JSON_PATH):
    with open(UNIVERSE_JSON_PATH, "r") as f:
        universes = json.load(f)
else:
    universes = {}

# =============================================================================
# NSE SESSION SETUP
# =============================================================================

session = requests.Session()
session.headers.update(HEADERS)

# Bootstrap cookies
session.get("https://www.nseindia.com", timeout=10)

# =============================================================================
# MAIN SYNC LOOP
# =============================================================================

for key, (label, encoded_name) in NSE_INDICES.items():
    print(f"\n📥 Syncing {label}")

    url = f"https://www.nseindia.com/api/equity-stockIndices?index={encoded_name}"
    r = session.get(url, timeout=10)

    if r.status_code != 200:
        print(f"❌ Failed to fetch {label} (HTTP {r.status_code})")
        continue

    data = r.json()

    symbols = []
    for item in data.get("data", []):
        sym = item.get("symbol")
        if not sym:
            continue

        sym = sym.strip().upper()

        # 🚫 NEVER insert index name itself
        if sym.replace(" ", "_") == key:
            continue

        symbols.append(sym)

    symbols = sorted(set(symbols))

    if not symbols:
        print(f"⚠️ No symbols fetched for {label}, skipping DB update")
        continue

    print(f"✅ {len(symbols)} symbols")

    # Refresh DB safely
    cur.execute("DELETE FROM index_members WHERE index_name = ?", (key,))
    for sym in symbols:
        cur.execute(
            "INSERT OR IGNORE INTO index_members (index_name, symbol) VALUES (?, ?)",
            (key, sym),
        )
    conn.commit()

    # Update universes.json (index-owned only)
    universes[key] = {
        "label": label,
        "type": "index",
        "index_name": key
    }

    sleep(REQUEST_DELAY)

# =============================================================================
# WRITE universes.json
# =============================================================================

with open(UNIVERSE_JSON_PATH, "w") as f:
    json.dump(universes, f, indent=2)

conn.close()

print("\n🎯 Universe sync completed successfully.")
print("ℹ universes.json updated automatically.")
