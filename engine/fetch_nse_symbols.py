import pandas as pd
import os

# ---------- PATH SETUP ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_FILE = os.path.join(DATA_DIR, "nse_symbols.txt")

# ---------- NSE SYMBOL SOURCE ----------
URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

print("Downloading NSE symbol list...")

try:
    df = pd.read_csv(URL)
except Exception as e:
    print("❌ Failed to download NSE file")
    print(e)
    exit(1)

# ---------- FILTER ONLY EQ SERIES ----------
df.columns = df.columns.str.strip()   # VERY IMPORTANT
df = df[df["SERIES"] == "EQ"]

symbols = df["SYMBOL"].astype(str).tolist()
symbols = [s.strip() + ".NS" for s in symbols]

# ---------- SAVE FILE ----------
with open(OUTPUT_FILE, "w") as f:
    for s in symbols:
        f.write(s + "\n")

print(f"✅ Saved {len(symbols)} NSE symbols")
print(f"📁 File location: {OUTPUT_FILE}")
