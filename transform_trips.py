from pathlib import Path

import pandas as pd

from .config import RAW_DIR, PROCESSED_DIR, PROCESSED_FILE


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _parse_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def build_processed_dataset() -> None:
    """Read raw trip CSVs, clean them, and save a processed parquet file."""
    _ensure_dir(PROCESSED_DIR)

    raw_files = sorted(RAW_DIR.glob("trips_*.csv"))
    if not raw_files:
        print("[TRANSFORM] No raw files found in data/raw/. Did you run ingest?")
        return

    frames = []
    for path in raw_files:
        df = pd.read_csv(path)
        df["source_file"] = path.name
        frames.append(df)

    all_trips = pd.concat(frames, ignore_index=True)
    print(f"[TRANSFORM] Loaded {len(all_trips)} raw rows from {len(raw_files)} files")

    # Parse datetimes
    all_trips["pickup_ts"] = _parse_datetime(all_trips["pickup_datetime"])
    all_trips["dropoff_ts"] = _parse_datetime(all_trips["dropoff_datetime"])

    # Compute trip duration in minutes
    duration = (all_trips["dropoff_ts"] - all_trips["pickup_ts"]).dt.total_seconds() / 60.0
    all_trips["trip_duration_min"] = duration

    # Compute total amount
    all_trips["total_amount"] = all_trips["fare_amount"] + all_trips["tip_amount"]

    # Basic cleaning: drop obvious bad rows
    cleaned = all_trips[
        (all_trips["trip_distance_km"] > 0)
        & (all_trips["trip_duration_min"] > 0)
        & all_trips["pickup_ts"].notna()
        & all_trips["dropoff_ts"].notna()
    ].copy()

    # Keep a tidy set of columns in a "fact-like" form
    fact_cols = [
        "trip_id",
        "pickup_ts",
        "dropoff_ts",
        "passenger_count",
        "trip_distance_km",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
        "city_zone",
        "trip_duration_min",
    ]
    fact_trips = cleaned[fact_cols]

    _ensure_dir(PROCESSED_DIR)
    fact_trips.to_parquet(PROCESSED_FILE, index=False)
    print(f"[TRANSFORM] Wrote {len(fact_trips)} cleaned rows to {PROCESSED_FILE}")


if __name__ == "__main__":
    build_processed_dataset()
