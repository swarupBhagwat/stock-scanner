from db import get_connection

def get_all_symbols(limit=100):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM symbols LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]

symbols = get_all_symbols(200)
