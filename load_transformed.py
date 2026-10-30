"""
Load Transformed Module for Food Orders ELT Pipeline
====================================================
Initializes the analytics database 'food_orders_analytics_db' and 'cleaned_food_orders' table.
Loads validated, transformed records in batches of 100 rows using database transactions.
Ensures batches append without overwriting and verifies row counts.
"""

from typing import List, Tuple
import mysql.connector
import pandas as pd
import numpy as np

from config import (
    get_base_db_config,
    get_analytics_db_config,
    ANALYTICS_DB_NAME,
    ANALYTICS_TABLE_NAME,
    BATCH_SIZE
)


def init_analytics_database(reset: bool = False) -> None:
    """
    Creates the analytics database and cleaned table if they do not exist.
    If reset=True, drops the existing table to allow a fresh run.
    """
    # Step 1: Ensure analytics database exists
    base_config = get_base_db_config()
    conn = mysql.connector.connect(**base_config)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {ANALYTICS_DB_NAME};")
    conn.commit()
    cursor.close()
    conn.close()

    # Step 2: Ensure cleaned_food_orders table exists
    analytics_config = get_analytics_db_config()
    conn = mysql.connector.connect(**analytics_config)
    cursor = conn.cursor()

    if reset:
        cursor.execute(f"DROP TABLE IF EXISTS {ANALYTICS_TABLE_NAME};")
        conn.commit()
        print(f"[LOAD ANALYTICS] Reset existing table `{ANALYTICS_TABLE_NAME}`.")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {ANALYTICS_TABLE_NAME} (
        order_id INT PRIMARY KEY,
        customer_id VARCHAR(50) NOT NULL,
        restaurant_id VARCHAR(50) NOT NULL,
        order_date_time DATETIME NOT NULL,
        delivery_date_time DATETIME NOT NULL,
        order_value DECIMAL(10, 2) NULL,
        delivery_fee DECIMAL(10, 2) NULL,
        payment_method VARCHAR(50) NOT NULL,
        discounts_and_offers VARCHAR(100) NOT NULL,
        commission_fee DECIMAL(10, 2) NULL,
        payment_processing_fee DECIMAL(10, 2) NULL,
        refunds_chargebacks DECIMAL(10, 2) NULL,
        weather_condition VARCHAR(50) NOT NULL,
        order_channel VARCHAR(50) NOT NULL,
        delivery_time_minutes DECIMAL(10, 2) NOT NULL,
        platform_net_revenue DECIMAL(10, 2) NULL,
        order_date DATE NOT NULL,
        order_hour INT NOT NULL,
        batch_number INT NOT NULL,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"[LOAD ANALYTICS] Database `{ANALYTICS_DB_NAME}` and table `{ANALYTICS_TABLE_NAME}` initialized.")


def prepare_analytics_records(valid_df: pd.DataFrame, batch_num: int) -> list:
    """
    Transforms DataFrame rows into tuples with appropriate types for MySQL insertion.
    Converts NaN/None values to Python None for SQL NULL insertion.
    """
    records = []
    for _, row in valid_df.iterrows():
        # Format datetimes
        odt_val = row["order_date_time"].to_pydatetime() if pd.notna(row["order_date_time"]) else None
        ddt_val = row["delivery_date_time"].to_pydatetime() if pd.notna(row["delivery_date_time"]) else None
        odate_val = row["order_date"] if pd.notna(row["order_date"]) else None

        records.append((
            int(row["order_id"]),
            str(row["customer_id"]),
            str(row["restaurant_id"]),
            odt_val,
            ddt_val,
            float(row["order_value"]) if pd.notna(row["order_value"]) else None,
            float(row["delivery_fee"]) if pd.notna(row["delivery_fee"]) else None,
            str(row["payment_method"]),
            str(row["discounts_and_offers"]),
            float(row["commission_fee"]) if pd.notna(row["commission_fee"]) else None,
            float(row["payment_processing_fee"]) if pd.notna(row["payment_processing_fee"]) else None,
            float(row["refunds_chargebacks"]) if pd.notna(row["refunds_chargebacks"]) else None,
            str(row["weather_condition"]),
            str(row["order_channel"]),
            float(row["delivery_time_minutes"]),
            float(row["platform_net_revenue"]) if pd.notna(row["platform_net_revenue"]) else None,
            odate_val,
            int(row["order_hour"]),
            batch_num
        ))
    return records


def load_transformed_batch(valid_df: pd.DataFrame, batch_num: int, conn) -> int:
    """
    Inserts a single batch of validated records into cleaned_food_orders using a transaction.
    """
    if valid_df.empty:
        print(f"[LOAD ANALYTICS] Batch {batch_num:02d}: No valid records to insert (0 rows).")
        return 0

    insert_sql = f"""
    INSERT INTO {ANALYTICS_TABLE_NAME} (
        order_id,
        customer_id,
        restaurant_id,
        order_date_time,
        delivery_date_time,
        order_value,
        delivery_fee,
        payment_method,
        discounts_and_offers,
        commission_fee,
        payment_processing_fee,
        refunds_chargebacks,
        weather_condition,
        order_channel,
        delivery_time_minutes,
        platform_net_revenue,
        order_date,
        order_hour,
        batch_number
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    records = prepare_analytics_records(valid_df, batch_num)
    cursor = conn.cursor()
    try:
        conn.start_transaction()
        cursor.executemany(insert_sql, records)
        conn.commit()
        inserted_count = cursor.rowcount
        print(f"[LOAD ANALYTICS] Batch {batch_num:02d}: Successfully inserted {inserted_count} records via transaction.")
        cursor.close()
        return inserted_count
    except Exception as e:
        conn.rollback()
        cursor.close()
        print(f"[LOAD ANALYTICS ERROR] Batch {batch_num:02d} insertion failed: {e}. Transaction rolled back.")
        raise


def load_all_transformed(
    valid_batches: List[Tuple[int, pd.DataFrame]],
    reset: bool = True
) -> Tuple[int, int, int]:
    """
    Loads all validated batches into the analytics database in batches of 100 rows.
    
    Returns:
        Tuple[int, int, int]: (total_records_loaded, successful_batches, failed_batches)
    """
    init_analytics_database(reset=reset)

    analytics_config = get_analytics_db_config()
    conn = mysql.connector.connect(**analytics_config)

    total_records = 0
    successful_batches = 0
    failed_batches = 0

    try:
        for batch_num, batch_df in valid_batches:
            if batch_df.empty:
                continue
            try:
                count = load_transformed_batch(batch_df, batch_num, conn)
                total_records += count
                successful_batches += 1
            except Exception as e:
                failed_batches += 1
                print(f"[LOAD ANALYTICS] Failed loading batch {batch_num}: {e}")
    finally:
        conn.close()

    print(f"\n[LOAD ANALYTICS] Completed analytics data loading:")
    print(f"  - Total records loaded: {total_records}")
    print(f"  - Successful batches: {successful_batches}")
    print(f"  - Failed batches: {failed_batches}")

    return total_records, successful_batches, failed_batches


def verify_analytics_table_count() -> int:
    """Queries and returns the total record count in cleaned_food_orders table."""
    analytics_config = get_analytics_db_config()
    conn = mysql.connector.connect(**analytics_config)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {ANALYTICS_TABLE_NAME};")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    print(f"[VERIFY ANALYTICS] Table `{ANALYTICS_TABLE_NAME}` currently contains {count} records.")
    return count


if __name__ == "__main__":
    from retrieve_raw import retrieve_raw_batches
    from transform import FoodOrdersTransformer

    print("--- Starting Load Transformed Process Test ---")
    transformer = FoodOrdersTransformer()
    batches_to_load = []

    for b_num, raw_df in retrieve_raw_batches():
        v_df, _ = transformer.transform_batch(raw_df, b_num)
        batches_to_load.append((b_num, v_df))

    loaded, succ, fail = load_all_transformed(batches_to_load, reset=True)
    in_db = verify_analytics_table_count()
    print(f"Verification Match: {loaded == in_db} (Loaded: {loaded}, In DB: {in_db})")

