"""
Transform Module for Food Orders ELT Pipeline
==============================================
Performs batch-based data cleaning, type conversion, duplicate removal,
missing value handling, and derived metric calculations using Pandas.

Business & Transformation Rules Documented:
-------------------------------------------
1. Cross-Batch Duplicate Handling:
   - Tracks processed `Order ID`s across all batches in a stateful set.
   - Any record with an already seen `Order ID` is rejected as a duplicate.
   - Preserves exactly the first occurrence of each order.

2. Missing Values:
   - `Payment Method`: Imputed with "Unknown".
   - `Discounts and Offers`: Imputed with "Not Specified".
   - Financial Columns (`Order Value`, `Delivery Fee`, `Commission Fee`,
     `Payment Processing Fee`, `Refunds/Chargebacks`): Retained as NaN (SQL NULL).
     Never automatically replaced with zero to prevent distorting averages and revenue.

3. Timestamps & Durations:
   - `Order Date and Time` and `Delivery Date and Time` converted to datetime.
   - Records with unparseable/missing timestamps are rejected.
   - `Delivery_Time_Minutes` is computed as:
     (Delivery Date and Time - Order Date and Time) in minutes.
   - Records with negative delivery duration (< 0) are rejected as invalid.

4. Platform Net Revenue Formula:
   - Platform Net Revenue = Commission Fee + Delivery Fee - Payment Processing Fee - Refunds/Chargebacks
   - Calculated ONLY when all four financial components are non-null.
   - If any component is missing, Platform_Net_Revenue is set to NaN (SQL NULL).

5. Derived Temporal Features:
   - `Order_Date`: Extracted date (YYYY-MM-DD) for daily analysis.
   - `Order_Hour`: Extracted hour (0-23) for hourly peak analysis.
"""

from typing import Tuple, List, Dict, Any, Optional
import pandas as pd
import numpy as np


class FoodOrdersTransformer:
    """
    Stateful batch transformer for Food Orders data.
    Maintains cross-batch history to ensure duplicates appearing across
    different batches are accurately detected and rejected.
    """

    FINANCIAL_COLUMNS = [
        "Order Value",
        "Delivery Fee",
        "Commission Fee",
        "Payment Processing Fee",
        "Refunds/Chargebacks"
    ]

    REVENUE_COMPONENTS = [
        "Commission Fee",
        "Delivery Fee",
        "Payment Processing Fee",
        "Refunds/Chargebacks"
    ]

    def __init__(self):
        # Set of seen Order IDs across all batches
        self.seen_order_ids = set()
        self.total_processed_records = 0
        self.total_valid_records = 0
        self.total_rejected_records = 0
        self.all_rejected_records: List[Dict[str, Any]] = []

    def reset_state(self):
        """Resets the state of the transformer."""
        self.seen_order_ids.clear()
        self.total_processed_records = 0
        self.total_valid_records = 0
        self.total_rejected_records = 0
        self.all_rejected_records.clear()

    def transform_batch(
        self,
        batch_df: pd.DataFrame,
        batch_num: int
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Transforms a single batch of food order records.
        
        Args:
            batch_df: DataFrame containing the raw batch from MySQL.
            batch_num: Current batch number.
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (valid_df, rejected_df)
        """
        valid_records: List[Dict[str, Any]] = []
        batch_rejected: List[Dict[str, Any]] = []

        for _, row in batch_df.iterrows():
            self.total_processed_records += 1
            raw_order_id = row.get("Order ID")

            # Check 1: Missing or invalid Order ID
            if pd.isna(raw_order_id) or str(raw_order_id).strip() == "":
                reject_info = {
                    "Batch_Number": batch_num,
                    "Order_ID": None,
                    "Customer_ID": row.get("Customer ID"),
                    "Rejection_Reason": "Missing or Empty Order ID"
                }
                batch_rejected.append(reject_info)
                self.all_rejected_records.append(reject_info)
                continue

            try:
                order_id_int = int(float(raw_order_id))
            except (ValueError, TypeError):
                reject_info = {
                    "Batch_Number": batch_num,
                    "Order_ID": raw_order_id,
                    "Customer_ID": row.get("Customer ID"),
                    "Rejection_Reason": f"Invalid Non-Numeric Order ID: {raw_order_id}"
                }
                batch_rejected.append(reject_info)
                self.all_rejected_records.append(reject_info)
                continue

            # Check 2: Cross-batch and Intra-batch Duplicate Detection
            if order_id_int in self.seen_order_ids:
                reject_info = {
                    "Batch_Number": batch_num,
                    "Order_ID": order_id_int,
                    "Customer_ID": row.get("Customer ID"),
                    "Rejection_Reason": f"Duplicate Order ID: {order_id_int} already processed"
                }
                batch_rejected.append(reject_info)
                self.all_rejected_records.append(reject_info)
                continue

            # Record Order ID as seen
            self.seen_order_ids.add(order_id_int)

            # Check 3: Timestamp Parsing & Validation
            order_dt = pd.to_datetime(row.get("Order Date and Time"), errors="coerce")
            delivery_dt = pd.to_datetime(row.get("Delivery Date and Time"), errors="coerce")

            if pd.isna(order_dt) or pd.isna(delivery_dt):
                reject_info = {
                    "Batch_Number": batch_num,
                    "Order_ID": order_id_int,
                    "Customer_ID": row.get("Customer ID"),
                    "Rejection_Reason": "Invalid or Unparseable Timestamp"
                }
                batch_rejected.append(reject_info)
                self.all_rejected_records.append(reject_info)
                continue

            # Check 4: Delivery Duration Calculation and Negative Duration Validation
            delivery_minutes = (delivery_dt - order_dt).total_seconds() / 60.0
            if delivery_minutes < 0:
                reject_info = {
                    "Batch_Number": batch_num,
                    "Order_ID": order_id_int,
                    "Customer_ID": row.get("Customer ID"),
                    "Rejection_Reason": f"Negative Delivery Duration ({delivery_minutes:.2f} mins)"
                }
                batch_rejected.append(reject_info)
                self.all_rejected_records.append(reject_info)
                continue

            # Parse Numeric Financial Columns
            fin_vals = {}
            for col in self.FINANCIAL_COLUMNS:
                val = row.get(col)
                if pd.isna(val) or str(val).strip() == "" or str(val).lower() == "nan" or str(val).lower() == "none":
                    fin_vals[col] = np.nan
                else:
                    try:
                        fin_vals[col] = float(val)
                    except (ValueError, TypeError):
                        fin_vals[col] = np.nan

            # Missing value business rules for categorical columns
            raw_pm = row.get("Payment Method")
            payment_method = "Unknown" if pd.isna(raw_pm) or str(raw_pm).strip() == "" or str(raw_pm).lower() in ["nan", "none"] else str(raw_pm).strip()

            raw_do = row.get("Discounts and Offers")
            discounts_offers = "Not Specified" if pd.isna(raw_do) or str(raw_do).strip() == "" or str(raw_do).lower() in ["nan", "none"] else str(raw_do).strip()

            # Platform Net Revenue Calculation
            # Revenue = Commission Fee + Delivery Fee - Payment Processing Fee - Refunds/Chargebacks
            # Only calculated when ALL four components are present
            has_all_revenue_components = all(
                not pd.isna(fin_vals[col]) for col in self.REVENUE_COMPONENTS
            )

            if has_all_revenue_components:
                net_revenue = (
                    fin_vals["Commission Fee"]
                    + fin_vals["Delivery Fee"]
                    - fin_vals["Payment Processing Fee"]
                    - fin_vals["Refunds/Chargebacks"]
                )
                net_revenue = round(net_revenue, 2)
            else:
                net_revenue = np.nan

            valid_record = {
                "order_id": order_id_int,
                "customer_id": str(row.get("Customer ID")).strip(),
                "restaurant_id": str(row.get("Restaurant ID")).strip(),
                "order_date_time": order_dt,
                "delivery_date_time": delivery_dt,
                "order_value": round(fin_vals["Order Value"], 2) if not pd.isna(fin_vals["Order Value"]) else None,
                "delivery_fee": round(fin_vals["Delivery Fee"], 2) if not pd.isna(fin_vals["Delivery Fee"]) else None,
                "payment_method": payment_method,
                "discounts_and_offers": discounts_offers,
                "commission_fee": round(fin_vals["Commission Fee"], 2) if not pd.isna(fin_vals["Commission Fee"]) else None,
                "payment_processing_fee": round(fin_vals["Payment Processing Fee"], 2) if not pd.isna(fin_vals["Payment Processing Fee"]) else None,
                "refunds_chargebacks": round(fin_vals["Refunds/Chargebacks"], 2) if not pd.isna(fin_vals["Refunds/Chargebacks"]) else None,
                "weather_condition": str(row.get("Weather Condition")).strip(),
                "order_channel": str(row.get("Order Channel")).strip(),
                "delivery_time_minutes": round(delivery_minutes, 2),
                "platform_net_revenue": net_revenue if not pd.isna(net_revenue) else None,
                "order_date": order_dt.date(),
                "order_hour": int(order_dt.hour)
            }
            valid_records.append(valid_record)

        valid_df = pd.DataFrame(valid_records)
        rejected_df = pd.DataFrame(batch_rejected)

        self.total_valid_records += len(valid_records)
        self.total_rejected_records += len(batch_rejected)

        print(f"[TRANSFORM] Batch {batch_num:02d}: Transformed {len(batch_df)} rows -> "
              f"{len(valid_df)} Valid, {len(rejected_df)} Rejected")

        return valid_df, rejected_df

    def get_all_rejected_df(self) -> pd.DataFrame:
        """Returns all rejected records across all processed batches as a DataFrame."""
        if not self.all_rejected_records:
            return pd.DataFrame(columns=["Batch_Number", "Order_ID", "Customer_ID", "Rejection_Reason"])
        return pd.DataFrame(self.all_rejected_records)


if __name__ == "__main__":
    from retrieve_raw import retrieve_raw_batches
    print("--- Starting Transform Test ---")
    transformer = FoodOrdersTransformer()
    all_valid = []
    
    for batch_num, raw_df in retrieve_raw_batches():
        valid_df, rej_df = transformer.transform_batch(raw_df, batch_num)
        all_valid.append(valid_df)

    combined_valid = pd.concat(all_valid, ignore_index=True)
    combined_rejected = transformer.get_all_rejected_df()

    print(f"\n[TRANSFORM SUMMARY]")
    print(f"Total Valid Records: {len(combined_valid)}")
    print(f"Total Rejected Records: {len(combined_rejected)}")
    print(f"Valid Shape: {combined_valid.shape}")
    print("\nRejected Records Sample:")
    print(combined_rejected)

