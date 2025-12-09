"""Utilities to clean daily transaction files for the medallion pipeline.

This module performs the Bronze-level cleaning, enforcing schema,
handling invalid values, and producing a high-quality normalized parquet
for downstream dbt (Silver/Gold).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
import pandas as pd

RAW_FILE_TEMPLATE = "transactions_{ds_nodash}.csv"
CLEAN_FILE_TEMPLATE = "transactions_{ds_nodash}_clean.parquet"


def _coerce_amount(value: pd.Series) -> pd.Series:
    """Normalize numeric fields and drop non-coercible entries."""
    return pd.to_numeric(value, errors="coerce")


def _normalize_status(value: pd.Series) -> pd.Series:
    """
    Normalize status to lowercase canonical values.
    Unknown statuses → NaN, so they get removed in validation.
    """
    normalized = value.fillna("").str.strip().str.lower()
    mapping = {
        "completed": "completed",
        "pending": "pending",
        "failed": "failed",
    }
    return normalized.map(mapping)


def clean_daily_transactions(
    execution_date: date,
    raw_dir: Path,
    clean_dir: Path,
    raw_template: str = RAW_FILE_TEMPLATE,
    clean_template: str = CLEAN_FILE_TEMPLATE,
) -> Path:
    """
    Bronze cleaning step.

    Enhancements included:
    - Validates presence of required columns.
    - Normalizes numeric and categorical fields.
    - Removes invalid statuses.
    - Parses timestamps safely and removes future timestamps.
    - Drops duplicate rows.
    - Ensures a minimum number of valid rows before writing output.
    - Produces deterministic parquet naming.
    """

    ds_nodash = execution_date.strftime("%Y%m%d")
    input_path = raw_dir / raw_template.format(ds_nodash=ds_nodash)
    output_path = clean_dir / clean_template.format(ds_nodash=ds_nodash)

    # ----------------------------------------------------------------------
    # File existence validation
    # ----------------------------------------------------------------------
    if not input_path.exists():
        raise FileNotFoundError(f"Raw data not found for {execution_date}: {input_path}")

    clean_dir.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------------------
    # Read and normalize columns
    # ----------------------------------------------------------------------
    df = pd.read_csv(input_path)
    df.columns = [col.strip().lower() for col in df.columns]
    df = df.drop_duplicates()

    # ----------------------------------------------------------------------
    # Column validation
    # ----------------------------------------------------------------------
    required_cols = {
        "transaction_id",
        "customer_id",
        "amount",
        "status",
        "transaction_ts",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in raw file: {missing}")

    # ----------------------------------------------------------------------
    # Cleanup: normalize fields
    # ----------------------------------------------------------------------
    df["amount"] = _coerce_amount(df["amount"])
    df["status"] = _normalize_status(df["status"])

    # Remove rows with invalid critical fields
    df = df.dropna(subset=["transaction_id", "customer_id", "amount", "status"])

    # ----------------------------------------------------------------------
    # Timestamp parsing & validation
    # ----------------------------------------------------------------------
    df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["transaction_ts"])

    # Convert to naive datetime (removes timezone info)
    df["transaction_ts"] = df["transaction_ts"].dt.tz_localize(None)

    # Remove future timestamps
    now_utc = pd.Timestamp.utcnow().replace(tzinfo=None)
    df = df[df["transaction_ts"] <= now_utc]

    # Derived attribute
    df["transaction_date"] = df["transaction_ts"].dt.date

    # ----------------------------------------------------------------------
    # Final validation: ensure at least 1 valid row
    # ----------------------------------------------------------------------
    if df.empty:
        raise ValueError(
            f"After Bronze cleaning, no valid rows remain for {execution_date}. "
            "Raw file may contain only invalid or future-dated rows."
        )

    # ----------------------------------------------------------------------
    # Write output parquet
    # ----------------------------------------------------------------------
    df.to_parquet(output_path, index=False)
    return output_path
