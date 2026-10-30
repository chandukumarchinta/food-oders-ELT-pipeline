"""
Retrieve Raw Module for Food Orders ELT Pipeline
================================================
Connects to 'food_orders_raw_db' and retrieves data from 'raw_food_orders'
in batches of 100 rows into Pandas DataFrames.
Does not alter or modify the original raw table.
"""

from typing import Generator, Tuple
import mysql.connector
import pandas as pd

from config import (
    get_raw_db_config,
    RAW_TABLE_NAME,
    BATCH_SIZE
)

RAW_COLUMNS = [
    "Order ID",
    "Customer ID",
    "Restaurant ID",
    "Order Date and Time",
    "Delivery Date and Time",
    "Order Value",
    "Delivery Fee",
    "Payment Method",
    "Discounts and Offers",
    "Commission Fee",
    "Payment Processing Fee",
    "Refunds/Chargebacks",
    "Weather Condition",
    "Order Channel"
]

SELECT_COLUMNS_SQL = """
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
    order_channel
"""


def get_raw_table_count(conn) -> int:
    """Returns the total number of records currently in the raw table."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {RAW_TABLE_NAME};")
    count = cursor.fetchone()[0]
    cursor.close()
    return count


def retrieve_raw_batches(
    batch_size: int = BATCH_SIZE
) -> Generator[Tuple[int, pd.DataFrame], None, None]:
    """
    Retrieves records from raw_food_orders in batches of batch_size rows.
    Converts each batch into a Pandas DataFrame.
    
    Yields:
        Tuple[int, pd.DataFrame]: (batch_number, batch_dataframe)
    """
    raw_config = get_raw_db_config()
    conn = mysql.connector.connect(**raw_config)

    try:
        total_records = get_raw_table_count(conn)
        print(f"[RETRIEVE RAW] Total records available in `{RAW_TABLE_NAME}`: {total_records}")

        batch_num = 0
        offset = 0

        while offset < total_records:
            batch_num += 1
            query = f"""
                SELECT {SELECT_COLUMNS_SQL}
                FROM {RAW_TABLE_NAME}
                ORDER BY raw_id ASC
                LIMIT %s OFFSET %s;
            """
            cursor = conn.cursor()
            cursor.execute(query, (batch_size, offset))
            rows = cursor.fetchall()
            cursor.close()

            # Construct DataFrame with original column names
            batch_df = pd.DataFrame(rows, columns=RAW_COLUMNS)

            print(f"[RETRIEVE RAW] Batch {batch_num:02d}: Retrieved {len(batch_df)} records "
                  f"(Rows {offset + 1} to {offset + len(batch_df)})")

            yield batch_num, batch_df
            offset += batch_size

        print(f"[RETRIEVE RAW] Finished retrieval: Total {batch_num} batches, {total_records} records.")

    finally:
        conn.close()


if __name__ == "__main__":
    print("--- Starting Retrieve Raw Process Test ---")
    batches = list(retrieve_raw_batches())
    total_retrieved = sum(len(b[1]) for b in batches)
    print(f"Total batches retrieved: {len(batches)}")
    print(f"Total records retrieved: {total_retrieved}")

