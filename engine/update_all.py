import subprocess
import os

# Absolute paths (CRITICAL)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))      # engine/
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))

FETCH_PRICES = os.path.join(PROJECT_ROOT, "engine", "fetch_data.py")
FETCH_SYMBOLS = os.path.join(PROJECT_ROOT, "engine", "fetch_nse_symbols.py")

# Run updates in correct order
subprocess.run(["python", FETCH_PRICES], check=True)
subprocess.run(["python", FETCH_SYMBOLS], check=True)

print("🚀 FULL UPDATE COMPLETED")
