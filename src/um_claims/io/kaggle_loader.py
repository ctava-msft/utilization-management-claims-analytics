"""Kaggle Enhanced Health Insurance Claims Dataset loader.

Loads the Kaggle CSV, normalizes column names and types, and maps
to the repo's canonical schema via the schema adapter.

Spec: Data ingestion for Kaggle "Enhanced Health Insurance Claims Dataset".
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from um_claims.io.kaggle_schema_adapter import adapt_kaggle_to_canonical

# Expected Kaggle CSV column names (case-insensitive matching applied)
KAGGLE_EXPECTED_COLUMNS = {
    "ClaimID",
    "PatientID",
    "ProviderID",
    "ClaimAmount",
    "ClaimDate",
    "DiagnosisCode",
    "ProcedureCode",
    "ProviderSpecialty",
    "ClaimType",
    "ClaimStatus",
}


def load_kaggle_claims(path: str | Path) -> pl.DataFrame:
    """Load and normalize the Kaggle Enhanced Health Insurance Claims CSV.

    Reads the CSV, normalizes column names/types to match the Kaggle schema,
    then adapts to the repo's canonical schema for downstream stages.

    Args:
        path: Path to the Kaggle CSV file.

    Returns:
        Polars DataFrame in the repo's canonical schema format.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If required Kaggle columns are missing.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Kaggle CSV not found: {path}")

    # Read CSV with all columns as strings initially for safe parsing
    try:
        raw = pl.read_csv(path, infer_schema_length=0)
    except Exception as e:
        raise ValueError(f"Failed to read CSV file {path}: {e}") from e

    # Normalize column names: strip whitespace
    raw = raw.rename({c: c.strip() for c in raw.columns})

    # Validate required columns are present
    raw_cols = set(raw.columns)
    missing = KAGGLE_EXPECTED_COLUMNS - raw_cols
    if missing:
        raise ValueError(
            f"Missing required Kaggle columns: {sorted(missing)}. "
            f"Found columns: {sorted(raw_cols)}"
        )

    # Cast types for Kaggle columns
    normalized = raw.with_columns(
        pl.col("ClaimID").cast(pl.Utf8).alias("ClaimID"),
        pl.col("PatientID").cast(pl.Utf8).alias("PatientID"),
        pl.col("ProviderID").cast(pl.Utf8).alias("ProviderID"),
        pl.col("ClaimAmount").cast(pl.Float64).alias("ClaimAmount"),
        pl.col("ClaimDate").str.to_date(format="%Y-%m-%d", strict=False).alias("ClaimDate"),
        pl.col("DiagnosisCode").cast(pl.Utf8).alias("DiagnosisCode"),
        pl.col("ProcedureCode").cast(pl.Utf8).alias("ProcedureCode"),
        pl.col("ProviderSpecialty").cast(pl.Utf8).alias("ProviderSpecialty"),
        pl.col("ClaimType").cast(pl.Utf8).alias("ClaimType"),
        pl.col("ClaimStatus").cast(pl.Utf8).alias("ClaimStatus"),
    )

    # Adapt to canonical schema
    return adapt_kaggle_to_canonical(normalized)
