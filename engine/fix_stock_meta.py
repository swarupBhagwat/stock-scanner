import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "stocks.db")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Reset invalid dates
cursor.execute("""
UPDATE stock_meta
SET last_date = NULL
WHERE last_date IS NOT NULL
AND last_date NOT LIKE '____-__-__'
""")

conn.commit()

# Show how many rows are fixed
cursor.execute("""
SELECT COUNT(*) FROM stock_meta WHERE last_date IS NULL
""")
count = cursor.fetchone()[0]

conn.close()

print(f"✅ stock_meta cleaned. NULL last_date rows: {count}")
