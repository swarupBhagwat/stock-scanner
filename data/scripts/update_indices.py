"""
===============================================================================
CLEANUP SYMBOLS TABLE
===============================================================================

PURPOSE
-------
This script removes non-stock symbols (index / universe names)
from the `symbols` table.

WHY NEEDED
----------
Some NSE fetch/import processes insert index names like:
- NIFTY50
- BANKNIFTY
- HEALTHCARE

These must NOT appear as tradable stocks.

WHAT IT DOES
------------
1. Deletes symbols starting with 'NIFTY'
2. Deletes symbols that match index names in index_members
3. Leaves real equity symbols untouched

SAFE TO RUN MULTIPLE TIMES ✅

RUN
---
python data/scripts/cleanup_symbols.py
===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import os
import sqlite3

# =============================================================================
# CONFIG
# =============================================================================

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "stocks.db")

# =============================================================================
# CONNECT DB
# =============================================================================

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("🔍 Connected to database")

# =============================================================================
# PREVIEW WHAT WILL BE DELETED (OPTIONAL BUT SAFE)
# =============================================================================

print("\n📋 Universe / Index symbols found in `symbols` table:")

cur.execute("""
SELECT symbol FROM symbols
WHERE symbol LIKE 'NIFTY%'
   OR symbol IN (
       SELECT DISTINCT index_name FROM index_members
   )
""")

rows = cur.fetchall()

if not rows:
    print("✅ No invalid symbols found. Database is clean.")
else:
    for r in rows:
        print("  ❌", r[0])

# =============================================================================
# DELETE INVALID SYMBOLS
# =============================================================================

print("\n🧹 Cleaning symbols table...")

cur.execute("""
DELETE FROM symbols
WHERE symbol LIKE 'NIFTY%'
   OR symbol IN (
       SELECT DISTINCT index_name FROM index_members
   )
""")

deleted = cur.rowcount

conn.commit()
conn.close()

# =============================================================================
# RESULT
# =============================================================================

print(f"\n✅ Cleanup complete")
print(f"🗑️  Removed {deleted} invalid symbol(s)")
print("🎯 symbols table now contains ONLY real stocks")
