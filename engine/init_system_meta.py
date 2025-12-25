import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "stocks.db")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS system_meta (
    key TEXT PRIMARY KEY,
    value TEXT
)
""")

conn.commit()
conn.close()

print("✅ system_meta table created (or already exists)")
