"""
Load Raw Module for Food Orders ELT Pipeline
============================================
Initializes the raw database 'food_orders_raw_db' and 'raw_food_orders' table.
Loads extracted CSV batches into MySQL using database transactions.
Preserves original values, duplicate records, and nulls without overwriting.
"""

from typing import Generator, Tuple, Optional
import mysql.connector
import pandas as pd
import numpy as np

from config import (
    get_base_db_config,
    get_raw_db_config,
    RAW_DB_NAME,
    RAW_TABLE_NAME,
    BATCH_SIZE
)
from extract import extract_batches


def init_raw_database(reset: bool = False) -> None:
    """
    Creates the raw database and raw table if they do not exist.
    If reset=True, drops the existing table to allow a fresh run.
    """
    # Step 1: Ensure database exists
    base_config = get_base_db_config()
    conn = mysql.connector.connect(**base_config)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {RAW_DB_NAME};")
    conn.commit()
    cursor.close()
    conn.close()

    # Step 2: Ensure table exists
    raw_config = get_raw_db_config()
    conn = mysql.connector.connect(**raw_config)
    cursor = conn.cursor()

    if reset:
        cursor.execute(f"DROP TABLE IF EXISTS {RAW_TABLE_NAME};")
        conn.commit()
        print(f"[LOAD RAW] Reset existing table `{RAW_TABLE_NAME}`.")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {RAW_TABLE_NAME} (
        raw_id INT AUTO_INCREMENT PRIMARY KEY,
        order_id VARCHAR(50) NULL,
        customer_id VARCHAR(50) NULL,
        restaurant_id VARCHAR(50) NULL,
        order_date_and_time VARCHAR(100) NULL,
        delivery_date_and_time VARCHAR(100) NULL,
        order_value VARCHAR(50) NULL,
        delivery_fee VARCHAR(50) NULL,
        payment_method VARCHAR(50) NULL,
        discounts_and_offers VARCHAR(100) NULL,
        commission_fee VARCHAR(50) NULL,
        payment_processing_fee VARCHAR(50) NULL,
        refunds_chargebacks VARCHAR(50) NULL,
        weather_condition VARCHAR(50) NULL,
        order_channel VARCHAR(50) NULL,
        batch_number INT NOT NULL,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"[LOAD RAW] Database `{RAW_DB_NAME}` and table `{RAW_TABLE_NAME}` initialized.")


def prepare_raw_records(batch_df: pd.DataFrame, batch_num: int) -> list:
    """
    Converts a pandas DataFrame chunk into a list of tuples formatted for raw table insertion.
    Preserves exact raw values, converting NaN to None (SQL NULL).
    """
    # Replace NaN, NaT, and infinity with None
    clean_df = batch_df.replace({np.nan: None})
    records = []
    for _, row in clean_df.iterrows():
        records.append((
            str(row["Order ID"]) if row["Order ID"] is not None else None,
            str(row["Customer ID"]) if row["Customer ID"] is not None else None,
            str(row["Restaurant ID"]) if row["Restaurant ID"] is not None else None,
            str(row["Order Date and Time"]) if row["Order Date and Time"] is not None else None,
            str(row["Delivery Date and Time"]) if row["Delivery Date and Time"] is not None else None,
            str(row["Order Value"]) if row["Order Value"] is not None else None,
            str(row["Delivery Fee"]) if row["Delivery Fee"] is not None else None,
            str(row["Payment Method"]) if row["Payment Method"] is not None else None,
            str(row["Discounts and Offers"]) if row["Discounts and Offers"] is not None else None,
            str(row["Commission Fee"]) if row["Commission Fee"] is not None else None,
            str(row["Payment Processing Fee"]) if row["Payment Processing Fee"] is not None else None,
            str(row["Refunds/Chargebacks"]) if row["Refunds/Chargebacks"] is not None else None,
            str(row["Weather Condition"]) if row["Weather Condition"] is not None else None,
            str(row["Order Channel"]) if row["Order Channel"] is not None else None,
            batch_num
        ))
    return records


def load_raw_batch(batch_df: pd.DataFrame, batch_num: int, conn) -> int:
    """
    Inserts a single batch into raw_food_orders using a database transaction.
    Returns the number of rows successfully inserted.
    """
    insert_sql = f"""
    INSERT INTO {RAW_TABLE_NAME} (
        order_id,
        customer_id,
        restaurant_id,
        order_date_and_time,
        delivery_date_and_time,
        order_value,
        delivery_fee,
        payment_method,
        discounts_and_offers,
        commission_fee,
        payment_processing_fee,
        refunds_chargebacks,
        weather_condition,
        order_channel,
        batch_number
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    records = prepare_raw_records(batch_df, batch_num)
    cursor = conn.cursor()
    try:
        conn.start_transaction()
        cursor.executemany(insert_sql, records)
        conn.commit()
        inserted_count = cursor.rowcount
        print(f"[LOAD RAW] Batch {batch_num:02d}: Successfully inserted {inserted_count} records via transaction.")
        cursor.close()
        return inserted_count
    except Exception as e:
        conn.rollback()
        cursor.close()
        print(f"[LOAD RAW ERROR] Batch {batch_num:02d} insertion failed: {e}. Transaction rolled back.")
        raise


def load_all_raw(reset: bool = True) -> Tuple[int, int, int]:
    """
    Iterates through all CSV batches and loads them into raw_food_orders.
    
    Returns:
        Tuple[int, int, int]: (total_records_loaded, successful_batches, failed_batches)
    """
    init_raw_database(reset=reset)

    raw_config = get_raw_db_config()
    conn = mysql.connector.connect(**raw_config)

    total_records = 0
    successful_batches = 0
    failed_batches = 0

    try:
        for batch_num, batch_df in extract_batches():
            try:
                count = load_raw_batch(batch_df, batch_num, conn)
                total_records += count
                successful_batches += 1
            except Exception as e:
                failed_batches += 1
                print(f"[LOAD RAW] Skipping batch {batch_num} due to error: {e}")
    finally:
        conn.close()

    print(f"\n[LOAD RAW] Completed raw data loading:")
    print(f"  - Total records loaded: {total_records}")
    print(f"  - Successful batches: {successful_batches}")
    print(f"  - Failed batches: {failed_batches}")

    return total_records, successful_batches, failed_batches


def verify_raw_table_count() -> int:
    """Queries and returns the total record count in raw_food_orders table."""
    raw_config = get_raw_db_config()
    conn = mysql.connector.connect(**raw_config)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {RAW_TABLE_NAME};")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    print(f"[VERIFY RAW] Table `{RAW_TABLE_NAME}` currently contains {count} records.")
    return count


if __name__ == "__main__":
    print("--- Starting Load Raw Process ---")
    loaded, success, failed = load_all_raw(reset=True)
    verified = verify_raw_table_count()
    print(f"Verification Match: {loaded == verified} (Loaded: {loaded}, In DB: {verified})")

