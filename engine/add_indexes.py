import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "stocks.db")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON prices(symbol)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON prices(date)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_symbol_date ON prices(symbol, date)")

conn.commit()
conn.close()

print("✅ Indexes created")
