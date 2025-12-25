import yfinance as yf
import pandas as pd
import sqlite3
import os
import logging
from datetime import timedelta, datetime

# =====================================================
# LOGGER SETUP
# =====================================================
# This captures logs and sends them to the Uvicorn terminal
logger = logging.getLogger("uvicorn.error")

# =====================================================
# PATH SETUP
# =====================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
DB_PATH = os.path.join(DATA_DIR, "stocks.db")
SYMBOL_FILE = os.path.join(DATA_DIR, "nse_symbols.txt")

TODAY = datetime.today().date()

# =====================================================
# DATABASE HELPERS
# =====================================================
def get_last_date(symbol):
    """Return valid YYYY-MM-DD or None (never crashes)"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT last_date FROM stock_meta WHERE symbol = ?", (symbol,))
    row = cur.fetchone()
    conn.close()

    if not row or not row[0]:
        return None

    try:
        pd.to_datetime(row[0], format="%Y-%m-%d")
        return row[0]
    except Exception:
        logger.warning(f"  ⚠️ Invalid last_date for {symbol}: {row[0]} → reset")
        return None

def update_last_date(symbol, last_date):
    """Write only valid dates"""
    try:
        pd.to_datetime(last_date, format="%Y-%m-%d")
    except Exception:
        logger.error(f"  ⚠️ Skipping invalid last_date write for {symbol}: {last_date}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO stock_meta (symbol, last_date)
        VALUES (?, ?)
        ON CONFLICT(symbol)
        DO UPDATE SET last_date = excluded.last_date
    """, (symbol, last_date))
    conn.commit()
    conn.close()

# =====================================================
# SAVE DATA TO DATABASE
# =====================================================
def save_to_db(symbol, df):
    symbol_clean = symbol.replace(".NS", "")
    df = df.copy()
    df.reset_index(inplace=True)

    # Flatten MultiIndex if Yahoo returns it
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df.rename(columns={df.columns[0]: "Date"}, inplace=True)

    # Hard validation
    df = df.dropna(subset=["Date", "Open", "High", "Low", "Close", "Volume"])
    df = df[
        (df["Open"] > 0) &
        (df["High"] > 0) &
        (df["Low"] > 0) &
        (df["Close"] > 0) &
        (df["Volume"] >= 0)
    ]

    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df = df.drop_duplicates(subset=["date"])

    records = [
        (
            symbol_clean,
            r.date,
            float(r.Open),
            float(r.High),
            float(r.Low),
            float(r.Close),
            int(r.Volume)
        )
        for r in df.itertuples(index=False)
    ]

    if not records:
        return None

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executemany("""
        INSERT OR IGNORE INTO prices
        (symbol, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    conn.close()

    return records[-1][1]  # last candle date inserted

# =====================================================
# FETCH STOCK DATA
# =====================================================
def fetch_stock(symbol):
    logger.info(f"Fetching {symbol} ...")

    symbol_clean = symbol.replace(".NS", "")
    last_date = get_last_date(symbol_clean)

    df = None

    # ---------- INCREMENTAL ----------
    if last_date:
        try:
            start_date = (
                pd.to_datetime(last_date) + timedelta(days=1)
            ).date()
        except Exception:
            logger.error("  ⚠️ last_date corrupted → full fetch")
            last_date = None

        if last_date and start_date > TODAY:
            logger.info(f"  {symbol_clean} is already up to date")
            return None

        if last_date:
            logger.info(f"  Incremental from {start_date}")
            df = yf.download(
                symbol,
                start=start_date.strftime("%Y-%m-%d"),
                interval="1d",
                auto_adjust=False,
                progress=False
            )

    # ---------- FULL FETCH ----------
    if df is None or df.empty:
        logger.info(f"  Full history fetch for {symbol}")
        df = yf.download(
            symbol,
            period="max",
            interval="1d",
            auto_adjust=False,
            progress=False
        )

    if df is None or df.empty:
        logger.warning(f"  No data available for {symbol}")
        return None

    last_inserted = save_to_db(symbol, df)

    if last_inserted:
        update_last_date(symbol_clean, last_inserted)
        logger.info(f"  Successfully updated {symbol} till {last_inserted}")
        return last_inserted

    return None

def sync_symbols_from_prices():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO symbols (symbol, active)
        SELECT DISTINCT symbol, 1 FROM prices
    """)
    conn.commit()
    conn.close()
    logger.info("✅ Symbols table synced")

# =====================================================
# RUNNER FUNCTION (Called by FastAPI)
# =====================================================
def run_fetch_all():
    """Main entry point to be called from the update button"""
    logger.info("🚀 Starting NSE data update process...")
    
    if not os.path.exists(SYMBOL_FILE):
        logger.error(f"❌ Symbol file not found at: {SYMBOL_FILE}")
        return

    with open(SYMBOL_FILE, "r") as f:
        stocks_list = [line.strip() for line in f if line.strip()]

    max_updated_date = None

    for stock in stocks_list:
        try:
            updated_date = fetch_stock(stock)
            if updated_date:
                if not max_updated_date or updated_date > max_updated_date:
                    max_updated_date = updated_date
        except Exception as e:
            logger.error(f"  ❌ Critical error fetching {stock}: {e}")

    sync_symbols_from_prices()

    if max_updated_date:
        logger.info(f"📅 Latest market data updated till: {max_updated_date}")

    logger.info("✅ Full fetch completed successfully")
    return max_updated_date

# =====================================================
# STANDALONE EXECUTION
# =====================================================
if __name__ == "__main__":
    # If running directly (not through FastAPI), configure basic logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    run_fetch_all()