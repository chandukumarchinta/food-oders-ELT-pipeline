"""
Extract Module for Food Orders ELT Pipeline
===========================================
Extracts source data from food_orders.csv in batches of 100 rows.
Preserves original values, missing values, and duplicate records
without any transformations or cleaning.
"""

from typing import Generator, Tuple
from pathlib import Path
import pandas as pd
from config import CSV_PATH, BATCH_SIZE


def count_csv_records(csv_path: str = CSV_PATH) -> int:
    """Returns the total number of records in the source CSV file."""
    path = Path(csv_path)
    if not path.is_file():
        raise FileNotFoundError(f"Source CSV file not found at: {csv_path}")
    df = pd.read_csv(csv_path)
    return len(df)


def extract_batches(
    csv_path: str = CSV_PATH,
    batch_size: int = BATCH_SIZE
) -> Generator[Tuple[int, pd.DataFrame], None, None]:
    """
    Reads food_orders.csv in configurable batches using Pandas.
    
    Yields:
        Tuple[int, pd.DataFrame]: (batch_number, batch_dataframe)
    """
    path = Path(csv_path)
    if not path.is_file():
        raise FileNotFoundError(f"Source CSV file not found at: {csv_path}")

    # Use chunksize to extract exactly batch_size rows at a time
    batch_num = 0
    total_extracted = 0

    reader = pd.read_csv(csv_path, chunksize=batch_size)
    for chunk in reader:
        batch_num += 1
        record_count = len(chunk)
        total_extracted += record_count
        print(f"[EXTRACT] Batch {batch_num:02d}: Extracted {record_count} records "
              f"(Cumulative: {total_extracted} records)")
        yield batch_num, chunk

    print(f"[EXTRACT] Finished extraction: Total {batch_num} batches, {total_extracted} records.")


if __name__ == "__main__":
    print(f"--- Starting Extraction Test (Batch Size: {BATCH_SIZE}) ---")
    batches = list(extract_batches())
    total_records = sum(len(b[1]) for b in batches)
    print(f"Total batches extracted: {len(batches)}")
    print(f"Total records extracted: {total_records}")

