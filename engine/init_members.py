import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "stocks.db")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS index_members (
    index_name TEXT,
    symbol TEXT,
    PRIMARY KEY (index_name, symbol)
)
""")

conn.commit()
conn.close()

print("✅ index_members table ready")
