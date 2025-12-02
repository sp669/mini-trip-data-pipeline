import argparse

from .ingest_raw_trips import generate_raw_trips
from .transform_trips import build_processed_dataset
from .load_to_warehouse import load_processed_into_warehouse


def run_pipeline(run_ingest: bool, run_transform: bool, run_load: bool) -> None:
    if run_ingest:
        generate_raw_trips()
    if run_transform:
        build_processed_dataset()
    if run_load:
        load_processed_into_warehouse()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mini trip data pipeline: ingest → transform → load."
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="Run only the ingest step (generate raw CSVs).",
    )
    parser.add_argument(
        "--transform",
        action="store_true",
        help="Run only the transform step (build processed parquet).",
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="Run only the load step (load into DuckDB).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # If no flags are given, run the entire pipeline
    if not (args.ingest or args.transform or args.load):
        print("[PIPELINE] No flags given; running full pipeline (ingest, transform, load).")
        run_pipeline(True, True, True)
    else:
        run_pipeline(args.ingest, args.transform, args.load)
