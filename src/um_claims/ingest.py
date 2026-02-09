"""Data ingestion module.

Loads claims data from Parquet files into Polars DataFrames.
Spec: SR-1, SR-9
"""

from __future__ import annotations

from pathlib import Path

import polars as pl


def load_claims(path: Path) -> pl.DataFrame:
    """Load claims from a Parquet file.

    Args:
        path: Path to the Parquet file.

    Returns:
        Polars DataFrame with claims data.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file cannot be read as Parquet.
    """
    if not path.exists():
        raise FileNotFoundError(f"Claims file not found: {path}")

    try:
        df = pl.read_parquet(path)
    except Exception as e:
        raise ValueError(f"Failed to read Parquet file {path}: {e}") from e

    return df


def save_claims(df: pl.DataFrame, path: Path) -> Path:
    """Save claims DataFrame to Parquet.

    Args:
        df: Claims DataFrame.
        path: Output file path.

    Returns:
        The path written to.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path
