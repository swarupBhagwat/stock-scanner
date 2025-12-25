import sqlite3
import pandas as pd
import os
import yfinance as yf
from datetime import datetime

# ================= CONFIGURATION =================
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "stocks.db")

class DBVisualizer:
    @staticmethod
    def section(title):
        print(f"\n{'='*60}")
        print(f"  📊 {title}")
        print(f"{'='*60}")

def get_latest_market_date():
    """Fetch the real-world latest trading date from NSE."""
    try:
        idx = yf.download("^NSEI", period="5d", progress=False, auto_adjust=False)
        return idx.index[-1].strftime("%Y-%m-%d")
    except:
        return None

def audit_database():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found at: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    viz = DBVisualizer()
    
    # 1. GET REFERENCE DATE
    print("🔍 Fetching live market status...")
    market_date = get_latest_market_date()
    
    # 2. OVERALL STATS
    viz.section("DATABASE SYNC AUDIT")
    if market_date:
        print(f"Current NSE Market Date: {market_date}")
    else:
        print("⚠️ Could not fetch live market date. Comparing symbols relatively.")

    # 3. SYMBOL SYNC STATUS
    # We join 'symbols' with 'stock_meta' to see who is lagging
    query = """
        SELECT 
            s.symbol,
            m.last_date,
            CASE 
                WHEN m.last_date IS NULL THEN '🔴 NEVER UPDATED'
                WHEN m.last_date < ? THEN '🟡 LAGGING'
                WHEN m.last_date = ? THEN '🟢 UP TO DATE'
                ELSE '🔵 FUTURE DATA (Check Clock)'
            END as status
        FROM symbols s
        LEFT JOIN stock_meta m ON s.symbol = m.symbol
    """
    
    df_sync = pd.read_sql_query(query, conn, params=(market_date, market_date))
    
    # Summary Table
    status_summary = df_sync['status'].value_counts().to_frame().reset_index()
    status_summary.columns = ['Status', 'Count']
    print("\n[ SUMMARY BY STATUS ]")
    print(status_summary.to_string(index=False))

    # 4. SHOW LAGGING SYMBOLS
    lagging = df_sync[df_sync['status'] == '🟡 LAGGING']
    never = df_sync[df_sync['status'] == '🔴 NEVER UPDATED']

    if not never.empty:
        viz.section(f"MISSING SYMBOLS ({len(never)})")
        print("These symbols are in your list but have NO data in the DB:")
        print(", ".join(never['symbol'].tolist()))

    if not lagging.empty:
        viz.section(f"LAGGING SYMBOLS ({len(lagging)})")
        print("These symbols are behind the current market date:")
        # Show a sample of 15 if there are many
        sample_size = 15
        print(lagging[['symbol', 'last_date']].head(sample_size).to_string(index=False))
        if len(lagging) > sample_size:
            print(f"... and {len(lagging) - sample_size} more.")

    # 5. DATA VOLUME PER SYMBOL
    viz.section("DATA DENSITY (Row Count per Symbol)")
    density = pd.read_sql_query("""
        SELECT symbol, COUNT(*) as rows 
        FROM prices 
        GROUP BY symbol 
        ORDER BY rows ASC 
        LIMIT 10
    """, conn)
    print("Symbols with the least data (potential errors or new listings):")
    print(density.to_string(index=False))

    conn.close()
    print(f"\n{'='*60}\nAudit Complete: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

if __name__ == "__main__":
    audit_database()