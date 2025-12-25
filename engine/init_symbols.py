import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "stocks.db")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS symbols (
    symbol TEXT PRIMARY KEY,
    active INTEGER DEFAULT 1,
    added_on TEXT,
    removed_on TEXT
)
""")

conn.commit()
conn.close()

print("✅ symbols master table ready")
