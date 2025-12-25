import pandas as pd
import sqlite3
import os
from datetime import datetime

# ---------------- PATHS ----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
DB_PATH = os.path.join(DATA_DIR, "stocks.db")
SYMBOL_FILE = os.path.join(DATA_DIR, "nse_symbols.txt")

NSE_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

print("🔄 Updating NSE symbol universe...")

# ---------------- LOAD NSE LIST ----------------
df = pd.read_csv(NSE_URL)
df.columns = df.columns.str.strip()
df = df[df["SERIES"] == "EQ"]

nse_symbols = set(df["SYMBOL"].astype(str).str.strip())

# ---------------- LOAD EXISTING SYMBOLS ----------------
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("SELECT symbol FROM symbols WHERE active = 1")
db_symbols = set(row[0] for row in cursor.fetchall())

today = datetime.today().strftime("%Y-%m-%d")

# ---------------- FIND CHANGES ----------------
new_symbols = nse_symbols - db_symbols
removed_symbols = db_symbols - nse_symbols

# ---------------- INSERT NEW SYMBOLS ----------------
for sym in new_symbols:
    cursor.execute("""
        INSERT OR IGNORE INTO symbols (symbol, active, added_on)
        VALUES (?, 1, ?)
    """, (sym, today))

# ---------------- MARK REMOVED SYMBOLS ----------------
for sym in removed_symbols:
    cursor.execute("""
        UPDATE symbols
        SET active = 0, removed_on = ?
        WHERE symbol = ?
    """, (today, sym))

conn.commit()
conn.close()

# ---------------- WRITE UPDATED FILE ----------------
with open(SYMBOL_FILE, "w") as f:
    for sym in sorted(nse_symbols):
        f.write(sym + ".NS\n")

# ---------------- REPORT ----------------
print(f"✅ Total NSE EQ symbols: {len(nse_symbols)}")
print(f"🆕 New symbols added: {len(new_symbols)}")
print(f"❌ Symbols marked inactive: {len(removed_symbols)}")
print("📁 nse_symbols.txt updated")
