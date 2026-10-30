"""
Main ELT Batch Pipeline Orchestrator
====================================
Orchestrates the complete batch-based ELT workflow:
1. Extract: Reads source data from CSV in batches of 100.
2. Load Raw: Loads original records into MySQL `food_orders_raw_db.raw_food_orders`.
3. Retrieve Raw: Queries raw records from MySQL into Pandas in batches of 100.
4. Transform: Cleans data, deduplicates across batches, handles missing values,
   calculates delivery duration and platform net revenue.
5. Validate: Verifies data quality, saves validation report and rejected records.
6. Load Transformed: Inserts validated data into `food_orders_analytics_db.cleaned_food_orders`.
7. Business Analysis: Executes the 6 required SQL analytical queries.
8. Execution Report: Displays comprehensive pipeline metrics and validation status.
"""

import sys
import time
from datetime import datetime
from pathlib import Path
import pandas as pd

from config import (
    CSV_PATH,
    BATCH_SIZE,
    RAW_DB_NAME,
    ANALYTICS_DB_NAME,
    RAW_TABLE_NAME,
    ANALYTICS_TABLE_NAME,
    VALIDATION_REPORT_PATH,
    REJECTED_RECORDS_PATH,
    FINAL_REPORT_PATH,
    SQL_ANALYSIS_PATH
)

from extract import count_csv_records, extract_batches
from load_raw import load_all_raw, verify_raw_table_count
from retrieve_raw import retrieve_raw_batches
from transform import FoodOrdersTransformer
from validate import DataValidator
from load_transformed import load_all_transformed, verify_analytics_table_count
from business_analysis import run_business_analysis


def run_pipeline() -> bool:
    """
    Executes the complete end-to-end ELT batch pipeline.
    
    Returns:
        bool: True if the pipeline succeeded with all validations passing, False otherwise.
    """
    start_time = time.time()
    print("=" * 80)
    print("      FOOD DELIVERY COMPANY - BATCH-BASED ELT DATA PIPELINE")
    print(f"      Started At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # -------------------------------------------------------------
    # STAGE 1 & 2: EXTRACT SOURCE CSV & LOAD RAW TO MYSQL
    # -------------------------------------------------------------
    print("\n>>> [STAGE 1 & 2] EXTRACTING SOURCE CSV & LOADING RAW RECORDS INTO MYSQL")
    total_csv_records = count_csv_records(CSV_PATH)
    print(f"Source CSV file located: {CSV_PATH} ({total_csv_records} records)")

    raw_loaded, raw_succ_batches, raw_fail_batches = load_all_raw(reset=True)
    verified_raw = verify_raw_table_count()

    if raw_loaded != total_csv_records or verified_raw != total_csv_records:
        print(f"[ERROR] Discrepancy in raw load! CSV: {total_csv_records}, Loaded: {raw_loaded}, DB: {verified_raw}")
        return False

    # -------------------------------------------------------------
    # STAGE 3 & 4: RETRIEVE RAW FROM MYSQL & TRANSFORM WITH PANDAS
    # -------------------------------------------------------------
    print("\n>>> [STAGE 3 & 4] RETRIEVING RAW DATA FROM MYSQL & TRANSFORMING VIA PANDAS")
    transformer = FoodOrdersTransformer()
    valid_batches = []
    total_raw_retrieved = 0
    retrieval_batch_count = 0

    for batch_num, raw_df in retrieve_raw_batches(batch_size=BATCH_SIZE):
        retrieval_batch_count += 1
        total_raw_retrieved += len(raw_df)
        v_df, _ = transformer.transform_batch(raw_df, batch_num)
        valid_batches.append((batch_num, v_df))

    all_valid_df = pd.concat([b[1] for b in valid_batches if not b[1].empty], ignore_index=True)
    all_rejected_df = transformer.get_all_rejected_df()

    total_valid = len(all_valid_df)
    total_rejected = len(all_rejected_df)
    print(f"\n[TRANSFORMATION SUMMARY]")
    print(f"  - Total Raw Records Retrieved:  {total_raw_retrieved}")
    print(f"  - Total Valid Records:          {total_valid}")
    print(f"  - Total Rejected Records:       {total_rejected}")

    # -------------------------------------------------------------
    # STAGE 5: VALIDATE TRANSFORMED DATA
    # -------------------------------------------------------------
    print("\n>>> [STAGE 5] PERFORMING DATA VALIDATION CHECKS")
    validator = DataValidator()
    validation_passed, validation_report = validator.validate_dataset(
        all_valid_df,
        all_rejected_df,
        total_raw_records=total_raw_retrieved
    )
    validator.export_validation_outputs(
        validation_report,
        all_rejected_df,
        report_path=VALIDATION_REPORT_PATH,
        rejected_path=REJECTED_RECORDS_PATH
    )
    print(f"Validation Checks Passed: {validation_passed}")

    # -------------------------------------------------------------
    # STAGE 6: LOAD TRANSFORMED DATA INTO ANALYTICS DATABASE
    # -------------------------------------------------------------
    print("\n>>> [STAGE 6] LOADING TRANSFORMED RECORDS INTO ANALYTICS DATABASE")
    analytics_loaded, analytics_succ_batches, analytics_fail_batches = load_all_transformed(
        valid_batches,
        reset=True
    )
    verified_analytics = verify_analytics_table_count()

    # -------------------------------------------------------------
    # STAGE 7: BUSINESS ANALYSIS USING SQL
    # -------------------------------------------------------------
    print("\n>>> [STAGE 7] EXECUTING BUSINESS ANALYSIS SQL QUERIES")
    analysis_report = run_business_analysis()
    print("\n" + analysis_report)

    # -------------------------------------------------------------
    # STAGE 8: FINAL EXECUTION REPORT
    # -------------------------------------------------------------
    total_batches_processed = raw_succ_batches + raw_fail_batches
    total_successful_db_batches = raw_succ_batches + analytics_succ_batches
    total_failed_db_batches = raw_fail_batches + analytics_fail_batches
    elapsed = time.time() - start_time

    final_report_lines = [
        "=" * 70,
        "                 FINAL PIPELINE EXECUTION REPORT",
        "=" * 70,
        f"Execution Timestamp:                 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total Pipeline Runtime:              {elapsed:.2f} seconds",
        "-" * 70,
        f"1. Total Records Extracted from CSV: {total_csv_records}",
        f"2. Total Number of Batches Processed: {total_batches_processed}",
        f"3. Total Raw Records Loaded to MySQL: {raw_loaded}",
        f"4. Total Raw Records Retrieved from MySQL: {total_raw_retrieved}",
        f"5. Total Valid Records Transformed:   {total_valid}",
        f"6. Total Rejected Records:           {total_rejected}",
        f"7. Total Transformed Records in DB:  {verified_analytics}",
        f"8. Successful DB Batches:            {total_successful_db_batches}",
        f"   Failed DB Batches:                {total_failed_db_batches}",
        f"9. Final Validation Status:          {'PASSED [SUCCESS]' if validation_passed else 'FAILED'}",
        "=" * 70
    ]
    final_report_text = "\n".join(final_report_lines)

    # Display to console
    print("\n" + final_report_text)

    # Save to disk
    Path(FINAL_REPORT_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(FINAL_REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(final_report_text)
    print(f"\n[REPORT] Final execution report saved to: {FINAL_REPORT_PATH}")

    return validation_passed and (verified_analytics == total_valid)


if __name__ == "__main__":
    success = run_pipeline()
    if not success:
        sys.exit(1)

