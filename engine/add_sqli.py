import sqlite3
conn = sqlite3.connect("data/stocks.db")
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS system_meta (
    key TEXT PRIMARY KEY,
    value TEXT
)
""")
conn.commit()
conn.close()
