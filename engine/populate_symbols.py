import sqlite3
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "stocks.db")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Ensure symbols table exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS symbols (
    symbol TEXT PRIMARY KEY,
    active INTEGER DEFAULT 1,
    added_on TEXT,
    removed_on TEXT
)
""")

today = datetime.today().strftime("%Y-%m-%d")

# Populate symbols from prices table
cursor.execute("""
INSERT OR IGNORE INTO symbols (symbol, active, added_on)
SELECT DISTINCT symbol, 1, ?
FROM prices
""", (today,))

conn.commit()
conn.close()

print("✅ symbols table populated from prices")
