from pathlib import Path

import duckdb

from .config import DUCKDB_PATH, PROCESSED_FILE, WAREHOUSE_DIR


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_processed_into_warehouse() -> None:
    """Load the processed parquet dataset into DuckDB and create an aggregate view."""
    if not PROCESSED_FILE.exists():
        print("[LOAD] Processed file does not exist. Did you run the transform step?")
        return

    _ensure_dir(WAREHOUSE_DIR)

    con = duckdb.connect(str(DUCKDB_PATH))
    print(f"[LOAD] Connected to DuckDB at {DUCKDB_PATH}")

    # Create fact table schema
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_trips (
            trip_id           VARCHAR,
            pickup_ts         TIMESTAMP,
            dropoff_ts        TIMESTAMP,
            passenger_count   INTEGER,
            trip_distance_km  DOUBLE,
            fare_amount       DOUBLE,
            tip_amount        DOUBLE,
            total_amount      DOUBLE,
            payment_type      VARCHAR,
            city_zone         VARCHAR,
            trip_duration_min DOUBLE
        );
        """
    )

    # Remove existing data so this step is idempotent
    con.execute("DELETE FROM fact_trips;")

    # Insert fresh data from parquet
    con.execute(
        f"""
        INSERT INTO fact_trips
        SELECT
            trip_id,
            pickup_ts,
            dropoff_ts,
            passenger_count,
            trip_distance_km,
            fare_amount,
            tip_amount,
            total_amount,
            payment_type,
            city_zone,
            trip_duration_min
        FROM read_parquet('{PROCESSED_FILE.as_posix()}');
        """
    )

    print("[LOAD] Loaded processed trips into fact_trips")

    # Create an aggregate view for daily revenue per zone
    con.execute(
        """
        CREATE OR REPLACE VIEW daily_revenue AS
        SELECT
            date_trunc('day', pickup_ts)::DATE AS trip_date,
            city_zone,
            COUNT(*) AS num_trips,
            SUM(total_amount) AS total_revenue
        FROM fact_trips
        GROUP BY trip_date, city_zone
        ORDER BY trip_date, city_zone;
        """
    )

    print("[LOAD] Created/updated daily_revenue view")
    con.close()


if __name__ == "__main__":
    load_processed_into_warehouse()
