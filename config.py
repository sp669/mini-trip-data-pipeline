from pathlib import Path
from datetime import date

# Base directory = project root (two levels up from this file)
BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
WAREHOUSE_DIR = DATA_DIR / "warehouse"

DUCKDB_PATH = WAREHOUSE_DIR / "trips_warehouse.duckdb"
PROCESSED_FILE = PROCESSED_DIR / "processed_trips.parquet"

# Synthetic data settings
START_DATE = date(2024, 1, 1)
NUM_DAYS = 7
ROWS_PER_DAY = 500
