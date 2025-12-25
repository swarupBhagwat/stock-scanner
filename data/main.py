"""
===============================================================================
FASTAPI BACKEND – MAIN ENTRY POINT
===============================================================================

FILE
----
data/main.py

ARCHITECTURE RULES
------------------
1. universes.json defines ONLY groups (no stocks)
2. Database is the single source of truth for symbols
3. Charts are handled in routers/chart.py
4. Frontend talks ONLY via API (no guessing)

RUN
---
uvicorn main:app --host 0.0.0.0 --port 8000
===============================================================================
"""

# =============================================================================
# STANDARD LIBRARY IMPORTS
# =============================================================================

import os
import sys
import json
import logging
from threading import Lock

# =============================================================================
# THIRD-PARTY IMPORTS
# =============================================================================

import yfinance as yf
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

# =============================================================================
# PATH SETUP (BULLETPROOF)
# =============================================================================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# =============================================================================
# INTERNAL IMPORTS
# =============================================================================

from db import get_connection
from engine.fetch_data import run_fetch_all

from scan.engine import run_scan
from scan.builder import build_rule
from scan.utils import required_bars
from scan.validator import validate_rule

# 🔥 CHART ROUTER (SEPARATE FILE)
from routers.chart import router as chart_router

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger("uvicorn.error")

# =============================================================================
# LOAD UNIVERSES CONFIG (GROUPS ONLY)
# =============================================================================

UNIVERSE_CONFIG_PATH = os.path.join(
    CURRENT_DIR, "config", "universes.json"
)

if not os.path.exists(UNIVERSE_CONFIG_PATH):
    raise RuntimeError("❌ config/universes.json not found")

with open(UNIVERSE_CONFIG_PATH, "r") as f:
    UNIVERSES = json.load(f)

logger.info(f"Loaded {len(UNIVERSES)} universes from config")

# =============================================================================
# FASTAPI APP
# =============================================================================

app = FastAPI(title="Stock Scanner Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ REGISTER ROUTERS
app.include_router(chart_router)

# =============================================================================
# UPDATE STATE
# =============================================================================

update_state = {
    "running": False,
    "message": "Idle",
}

update_lock = Lock()

# =============================================================================
# SYSTEM META TABLE
# =============================================================================

def ensure_system_meta():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS system_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    conn.close()

def get_meta(key: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT value FROM system_meta WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def set_meta(key: str, value: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO system_meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key)
        DO UPDATE SET value=excluded.value
    """, (key, value))
    conn.commit()
    conn.close()

ensure_system_meta()

# =============================================================================
# MARKET DATE CHECK
# =============================================================================

def get_latest_market_date():
    try:
        df = yf.download(
            "RELIANCE.NS",
            period="5d",
            interval="1d",
            progress=False,
        )
        if df.empty:
            return None
        return df.index[-1].strftime("%Y-%m-%d")
    except Exception as e:
        logger.error(f"Market check failed: {e}")
        return None

# =============================================================================
# BACKGROUND UPDATE TASK
# =============================================================================

def run_update_task():
    try:
        logger.info("🚀 Starting market data update")
        max_date = run_fetch_all()
        if max_date:
            set_meta("last_price_update", max_date)
            logger.info(f"✅ Data updated till {max_date}")
    except Exception as e:
        logger.error(f"❌ Update failed: {e}")
    finally:
        update_state["running"] = False
        update_state["message"] = "Idle"

# =============================================================================
# ROOT / HEALTH
# =============================================================================

@app.get("/")
def root():
    return {"status": "ok"}

# =============================================================================
# UPDATE APIs
# =============================================================================

@app.post("/update")
def update_data(background_tasks: BackgroundTasks):
    if update_state["running"]:
        return {"status": "running"}

    update_state["running"] = True
    update_state["message"] = "Updating"

    background_tasks.add_task(run_update_task)
    return {"status": "started"}

@app.get("/update/status")
def update_status():
    return update_state

# =============================================================================
# UNIVERSES API (CONFIG ONLY)
# =============================================================================

@app.get("/universes")
def get_universes():
    """
    Returns universe groups ONLY.
    """
    return [
        {"key": key, "label": cfg.get("label", key)}
        for key, cfg in UNIVERSES.items()
    ]

# =============================================================================
# STOCK LIST API (DB ONLY)
# =============================================================================

@app.get("/stocks")
def get_stocks(universe: str):
    """
    Returns stocks belonging to a universe.
    """
    cfg = UNIVERSES.get(universe)
    if not cfg:
        return {"count": 0, "symbols": []}

    conn = get_connection()
    cur = conn.cursor()

    if cfg["type"] == "index":
        cur.execute(
            "SELECT symbol FROM index_members WHERE index_name=? ORDER BY symbol",
            (cfg["index_name"],),
        )
        symbols = [r[0] for r in cur.fetchall()]

    elif cfg["type"] == "symbols":
        cur.execute(
            "SELECT symbol FROM symbols WHERE active=1 ORDER BY symbol"
        )
        symbols = [r[0] for r in cur.fetchall()]

    elif cfg["type"] == "custom":
        symbols = cfg.get("symbols", [])

    else:
        symbols = []

    conn.close()

    return {
        "universe": universe,
        "count": len(symbols),
        "symbols": symbols,
    }

# =============================================================================
# SCAN HELPERS
# =============================================================================

def get_symbols_by_universe(universe: str):
    cfg = UNIVERSES.get(universe)
    if not cfg:
        return []

    conn = get_connection()
    cur = conn.cursor()

    if cfg["type"] == "index":
        cur.execute(
            "SELECT symbol FROM index_members WHERE index_name=?",
            (cfg["index_name"],),
        )
        symbols = [r[0] for r in cur.fetchall()]

    elif cfg["type"] == "symbols":
        cur.execute("SELECT symbol FROM symbols WHERE active=1")
        symbols = [r[0] for r in cur.fetchall()]

    elif cfg["type"] == "custom":
        symbols = cfg.get("symbols", [])

    else:
        symbols = []

    conn.close()
    return symbols

# =============================================================================
# SCAN API
# =============================================================================

@app.post("/scan")
def scan_stocks(payload: dict):
    universe = payload.get("universe")
    rule_json = payload.get("rule")
    timeframe = payload.get("timeframe", "1D")
    indicators = payload.get("indicators", {})

    if not universe or not rule_json:
        return {"count": 0, "symbols": []}

    validate_rule(rule_json)

    symbols = get_symbols_by_universe(universe)
    rule_fn = build_rule(rule_json)

    min_bars = max(required_bars(indicators), 50)

    results = run_scan(
        symbols=symbols,
        timeframe=timeframe,
        indicator_config=indicators,
        rule_fn=rule_fn,
        min_bars=min_bars,
    )

    return {
        "universe": universe,
        "timeframe": timeframe,
        "count": len(results),
        "symbols": results,
    }
