"""
Validate Module for Food Orders ELT Pipeline
============================================
Performs comprehensive quality assurance and validation checks on transformed data:
- Missing value integrity checks
- Primary key and duplicate checks
- Column data type verification
- Delivery duration formula verification
- Platform Net Revenue formula verification
- Rejected records recording and export
- Generation of the comprehensive Validation Report
"""

from typing import Tuple, Dict, Any, List
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np

from config import (
    VALIDATION_REPORT_PATH,
    REJECTED_RECORDS_PATH
)


class DataValidator:
    """
    Validates transformed food order records and produces validation reports.
    """

    EXPECTED_TYPES = {
        "order_id": "int64",
        "customer_id": "object",
        "restaurant_id": "object",
        "order_date_time": "datetime64[ns]",
        "delivery_date_time": "datetime64[ns]",
        "order_value": "float64",
        "delivery_fee": "float64",
        "payment_method": "object",
        "discounts_and_offers": "object",
        "commission_fee": "float64",
        "payment_processing_fee": "float64",
        "refunds_chargebacks": "float64",
        "weather_condition": "object",
        "order_channel": "object",
        "delivery_time_minutes": "float64",
        "platform_net_revenue": "float64",
        "order_date": "object",
        "order_hour": "int64"
    }

    REQUIRED_NON_NULL_COLUMNS = [
        "order_id",
        "customer_id",
        "restaurant_id",
        "order_date_time",
        "delivery_date_time",
        "payment_method",
        "discounts_and_offers",
        "weather_condition",
        "order_channel",
        "delivery_time_minutes",
        "order_date",
        "order_hour"
    ]

    def __init__(self):
        self.validation_results: Dict[str, Any] = {}

    def validate_dataset(
        self,
        valid_df: pd.DataFrame,
        rejected_df: pd.DataFrame,
        total_raw_records: int
    ) -> Tuple[bool, str]:
        """
        Runs all validation suites against the dataset and returns (passed, report_text).
        """
        checks_passed = True
        report_lines: List[str] = []

        report_lines.append("=" * 70)
        report_lines.append("FOOD ORDERS DATA PIPELINE - DATA QUALITY & VALIDATION REPORT")
        report_lines.append("=" * 70)
        report_lines.append(f"Generated At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Total Raw Records Ingested:    {total_raw_records}")
        report_lines.append(f"Total Transformed Records:     {len(valid_df)}")
        report_lines.append(f"Total Rejected Records:        {len(rejected_df)}")
        report_lines.append("-" * 70)

        # -------------------------------------------------------------
        # CHECK 1: Duplicate Records
        # -------------------------------------------------------------
        dup_rows = valid_df.duplicated().sum()
        dup_ids = valid_df["order_id"].duplicated().sum() if "order_id" in valid_df.columns else 0
        check1_pass = (dup_rows == 0) and (dup_ids == 0)
        if not check1_pass:
            checks_passed = False

        report_lines.append(f"\n[CHECK 1] DUPLICATE RECORDS VERIFICATION: {'[PASS]' if check1_pass else '[FAIL]'}")
        report_lines.append(f"  - Duplicate Order IDs:       {dup_ids}")
        report_lines.append(f"  - Full Duplicate Rows:       {dup_rows}")
        report_lines.append(f"  - Cross-Batch Duplicates Filtered: {len(rejected_df)}")

        # -------------------------------------------------------------
        # CHECK 2: Mandatory Non-Null Columns
        # -------------------------------------------------------------
        missing_mandatory = {}
        for col in self.REQUIRED_NON_NULL_COLUMNS:
            if col in valid_df.columns:
                n_miss = valid_df[col].isna().sum()
                if n_miss > 0:
                    missing_mandatory[col] = n_miss
            else:
                missing_mandatory[col] = "COLUMN MISSING"

        check2_pass = len(missing_mandatory) == 0
        if not check2_pass:
            checks_passed = False

        report_lines.append(f"\n[CHECK 2] MANDATORY NON-NULL INTEGRITY: {'[PASS]' if check2_pass else '[FAIL]'}")
        if check2_pass:
            report_lines.append("  - All required fields (IDs, Timestamps, Channels) have 0 missing values.")
        else:
            for c, err in missing_mandatory.items():
                report_lines.append(f"  - CRITICAL: {c} has {err} null values!")

        # -------------------------------------------------------------
        # CHECK 3: Data Types Verification
        # -------------------------------------------------------------
        type_mismatches = []
        for col, expected_type in self.EXPECTED_TYPES.items():
            if col in valid_df.columns:
                actual_type = str(valid_df[col].dtype)
                # allow datetime64[ns] or similar
                if "datetime" in expected_type and "datetime" in actual_type:
                    continue
                elif expected_type == "int64" and ("int" in actual_type):
                    continue
                elif expected_type == "float64" and ("float" in actual_type or "int" in actual_type):
                    continue
                elif expected_type in ["object", "str"] and actual_type in ["object", "str", "string"]:
                    continue
                else:
                    type_mismatches.append((col, expected_type, actual_type))
            else:
                type_mismatches.append((col, expected_type, "NOT IN DATAFRAME"))

        check3_pass = len(type_mismatches) == 0
        if not check3_pass:
            checks_passed = False

        report_lines.append(f"\n[CHECK 3] COLUMN DATA TYPES VERIFICATION: {'[PASS]' if check3_pass else '[FAIL]'}")
        if check3_pass:
            report_lines.append("  - All columns match required analytical database types.")
        else:
            for col, exp, act in type_mismatches:
                report_lines.append(f"  - MISMATCH: Column '{col}' expected {exp}, got {act}")

        # -------------------------------------------------------------
        # CHECK 4: Delivery Duration Calculation Validation
        # -------------------------------------------------------------
        # Formula: (delivery_date_time - order_date_time) in minutes
        computed_duration = (
            valid_df["delivery_date_time"] - valid_df["order_date_time"]
        ).dt.total_seconds() / 60.0

        duration_diff = np.abs(valid_df["delivery_time_minutes"] - computed_duration)
        formula_correct = (duration_diff < 0.01).all()
        neg_durations = (valid_df["delivery_time_minutes"] < 0).sum()

        check4_pass = formula_correct and (neg_durations == 0)
        if not check4_pass:
            checks_passed = False

        report_lines.append(f"\n[CHECK 4] DELIVERY DURATION VALIDATION: {'[PASS]' if check4_pass else '[FAIL]'}")
        report_lines.append(f"  - Formula Accuracy:          {'100% Correct' if formula_correct else 'INACCURATE'}")
        report_lines.append(f"  - Negative Durations (< 0):  {neg_durations}")
        report_lines.append(f"  - Min Delivery Duration:     {valid_df['delivery_time_minutes'].min():.2f} minutes")
        report_lines.append(f"  - Max Delivery Duration:     {valid_df['delivery_time_minutes'].max():.2f} minutes")
        report_lines.append(f"  - Mean Delivery Duration:    {valid_df['delivery_time_minutes'].mean():.2f} minutes")
        report_lines.append(f"  - Median Delivery Duration:  {valid_df['delivery_time_minutes'].median():.2f} minutes")

        # -------------------------------------------------------------
        # CHECK 5: Platform Net Revenue Calculation Validation
        # -------------------------------------------------------------
        # Formula: Commission Fee + Delivery Fee - Payment Processing Fee - Refunds/Chargebacks
        rev_components = ["commission_fee", "delivery_fee", "payment_processing_fee", "refunds_chargebacks"]
        complete_mask = valid_df[rev_components].notna().all(axis=1)

        expected_revenue = np.where(
            complete_mask,
            (
                valid_df["commission_fee"]
                + valid_df["delivery_fee"]
                - valid_df["payment_processing_fee"]
                - valid_df["refunds_chargebacks"]
            ),
            np.nan
        )

        valid_rev_mask = valid_df["platform_net_revenue"].notna()
        rev_match = np.allclose(
            valid_df.loc[complete_mask, "platform_net_revenue"],
            expected_revenue[complete_mask],
            atol=0.01
        )
        non_complete_are_null = valid_df.loc[~complete_mask, "platform_net_revenue"].isna().all()

        check5_pass = rev_match and non_complete_are_null
        if not check5_pass:
            checks_passed = False

        total_net_rev = valid_df["platform_net_revenue"].sum()
        rev_records_count = valid_rev_mask.sum()
        null_rev_count = (~valid_rev_mask).sum()

        report_lines.append(f"\n[CHECK 5] PLATFORM NET REVENUE VALIDATION: {'[PASS]' if check5_pass else '[FAIL]'}")
        report_lines.append(f"  - Formula Accuracy:          {'100% Correct' if rev_match else 'INACCURATE'}")
        report_lines.append(f"  - Incomplete Records as NULL: {'100% Correct' if non_complete_are_null else 'INCORRECT'}")
        report_lines.append(f"  - Records with Valid Revenue:{rev_records_count}")
        report_lines.append(f"  - Records with NULL Revenue: {null_rev_count}")
        report_lines.append(f"  - Total Platform Net Revenue: ${total_net_rev:,.2f}")

        # -------------------------------------------------------------
        # CHECK 6: Optional Financial Columns Missing Values Overview
        # -------------------------------------------------------------
        report_lines.append("\n[FINANCIAL DATA COMPLETENESS SUMMARY]")
        for col in ["order_value", "delivery_fee", "commission_fee", "payment_processing_fee", "refunds_chargebacks"]:
            n_null = valid_df[col].isna().sum()
            pct_null = (n_null / len(valid_df)) * 100
            report_lines.append(f"  - {col:<24}: {n_null:>3} missing ({pct_null:.1f}%)")

        # -------------------------------------------------------------
        # CHECK 7: Rejected Records Summary
        # -------------------------------------------------------------
        report_lines.append("\n[REJECTED RECORDS AUDIT]")
        if len(rejected_df) == 0:
            report_lines.append("  - No records were rejected during processing.")
        else:
            reason_counts = rejected_df["Rejection_Reason"].value_counts()
            for reason, cnt in reason_counts.items():
                report_lines.append(f"  - {reason}: {cnt} records")

        # -------------------------------------------------------------
        # FINAL VERDICT
        # -------------------------------------------------------------
        report_lines.append("\n" + "=" * 70)
        report_lines.append(f"FINAL DATA QUALITY VERDICT: {'ALL CHECKS PASSED [PASSED]' if checks_passed else 'FAILED [ACTION REQUIRED]'}")
        report_lines.append("=" * 70)

        report_text = "\n".join(report_lines)
        return checks_passed, report_text

    def export_validation_outputs(
        self,
        report_text: str,
        rejected_df: pd.DataFrame,
        report_path: str = VALIDATION_REPORT_PATH,
        rejected_path: str = REJECTED_RECORDS_PATH
    ) -> None:
        """Saves the validation report text and rejected records CSV to disk."""
        # Save Validation Report
        Path(report_path).parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_text)
        print(f"[VALIDATE] Validation report written to: {report_path}")

        # Save Rejected Records CSV
        Path(rejected_path).parent.mkdir(parents=True, exist_ok=True)
        rejected_df.to_csv(rejected_path, index=False)
        print(f"[VALIDATE] Rejected records exported to: {rejected_path} ({len(rejected_df)} rows)")


if __name__ == "__main__":
    from retrieve_raw import retrieve_raw_batches
    from transform import FoodOrdersTransformer

    print("--- Starting Validation Suite Test ---")
    transformer = FoodOrdersTransformer()
    valid_batches = []

    for b_num, raw_df in retrieve_raw_batches():
        v_df, r_df = transformer.transform_batch(raw_df, b_num)
        valid_batches.append(v_df)

    all_valid = pd.concat(valid_batches, ignore_index=True)
    all_rejected = transformer.get_all_rejected_df()

    validator = DataValidator()
    passed, report = validator.validate_dataset(all_valid, all_rejected, total_raw_records=1010)
    print("\n" + report)
    validator.export_validation_outputs(report, all_rejected)
    print(f"\nValidation Result: {'SUCCESS' if passed else 'FAILURE'}")
