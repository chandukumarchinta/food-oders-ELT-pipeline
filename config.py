"""
Configuration Module for Food Orders ELT Pipeline
=================================================
Manages environment variables, database connection parameters,
batch processing settings, and file paths.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Base directory for the project
BASE_DIR = Path(__file__).resolve().parent

# Load environment variables from .env file if it exists
load_dotenv(BASE_DIR / ".env")

# Source data configuration
CSV_PATH = os.getenv("CSV_PATH", str(BASE_DIR / "food_orders.csv"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

# Database connection credentials
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Database names
RAW_DB_NAME = os.getenv("RAW_DB_NAME", "food_orders_raw_db")
ANALYTICS_DB_NAME = os.getenv("ANALYTICS_DB_NAME", "food_orders_analytics_db")

# Table names
RAW_TABLE_NAME = "raw_food_orders"
ANALYTICS_TABLE_NAME = "cleaned_food_orders"

# Output report file paths
VALIDATION_REPORT_PATH = os.getenv("VALIDATION_REPORT_PATH", str(BASE_DIR / "validation_report.txt"))
REJECTED_RECORDS_PATH = os.getenv("REJECTED_RECORDS_PATH", str(BASE_DIR / "rejected_records.csv"))
FINAL_REPORT_PATH = os.getenv("FINAL_REPORT_PATH", str(BASE_DIR / "final_execution_report.txt"))
SQL_ANALYSIS_PATH = os.getenv("SQL_ANALYSIS_PATH", str(BASE_DIR / "sql_analysis_results.txt"))


def get_base_db_config() -> dict:
    """Returns database configuration without a specific database selected."""
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "autocommit": False
    }


def get_raw_db_config() -> dict:
    """Returns database configuration for the raw food orders database."""
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": RAW_DB_NAME,
        "autocommit": False
    }


def get_analytics_db_config() -> dict:
    """Returns database configuration for the analytics food orders database."""
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": ANALYTICS_DB_NAME,
        "autocommit": False
    }

