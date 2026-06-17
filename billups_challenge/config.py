import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

DATA_DIR = Path(os.getenv("BILLUPS_DATA_DIR", str(PROJECT_ROOT / "data")))

TRANSACTIONS_PATH = DATA_DIR / os.getenv("BILLUPS_TRANSACTIONS_FILE", "historical_transactions.parquet")
MERCHANTS_PATH = DATA_DIR / os.getenv("BILLUPS_MERCHANTS_FILE", "merchants.csv")

OUTPUT_DIR = Path(os.getenv("BILLUPS_OUTPUT_DIR", str(PROJECT_ROOT / "output")))
OUTPUT_FORMAT = os.getenv("BILLUPS_OUTPUT_FORMAT", "csv")

PURCHASE_AMOUNT_MIN = float(os.getenv("BILLUPS_PURCHASE_AMOUNT_MIN", "0.0"))
PURCHASE_AMOUNT_MAX = float(os.getenv("BILLUPS_PURCHASE_AMOUNT_MAX", "1000000.0"))

LOG_FILE = os.getenv("BILLUPS_LOG_FILE", str(OUTPUT_DIR / "pipeline.log"))
