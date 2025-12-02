import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from .config import RAW_DIR, START_DATE, NUM_DAYS, ROWS_PER_DAY


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def generate_raw_trips() -> None:
    """Generate synthetic raw trip CSV files for a range of days.

    Files will look like:
      data/raw/trips_YYYY-MM-DD.csv
    """
    _ensure_dir(RAW_DIR)

    zones = ["Downtown", "Uptown", "Airport", "Suburbs"]
    payment_types = ["card", "cash", "wallet"]

    for day_offset in range(NUM_DAYS):
        current_date = START_DATE + timedelta(days=day_offset)
        rows = []

        for i in range(ROWS_PER_DAY):
            trip_id = f"{current_date.strftime('%Y%m%d')}-{i:05d}"

            # Base pickup time somewhere during the day
            pickup_base = datetime(
                current_date.year,
                current_date.month,
                current_date.day,
                random.randint(0, 23),
                random.randint(0, 59),
            )

            trip_minutes = random.randint(5, 60)
            dropoff_time = pickup_base + timedelta(minutes=trip_minutes)

            passenger_count = random.randint(1, 4)
            distance_km = round(random.uniform(0.5, 25.0), 2)

            # Simple fare model
            base_fare = 3.0
            per_km = 1.5
            fare_amount = round(base_fare + per_km * distance_km, 2)

            tip_amount = round(random.uniform(0, fare_amount * 0.3), 2)
            city_zone = random.choice(zones)
            payment_type = random.choice(payment_types)

            rows.append(
                {
                    "trip_id": trip_id,
                    "pickup_datetime": pickup_base.isoformat(),
                    "dropoff_datetime": dropoff_time.isoformat(),
                    "passenger_count": passenger_count,
                    "trip_distance_km": distance_km,
                    "fare_amount": fare_amount,
                    "tip_amount": tip_amount,
                    "payment_type": payment_type,
                    "city_zone": city_zone,
                }
            )

        df = pd.DataFrame(rows)
        output_path = RAW_DIR / f"trips_{current_date.isoformat()}.csv"
        df.to_csv(output_path, index=False)
        print(f"[INGEST] Wrote {len(df)} rows to {output_path}")


if __name__ == "__main__":
    generate_raw_trips()
