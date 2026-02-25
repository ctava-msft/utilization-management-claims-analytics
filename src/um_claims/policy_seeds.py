"""Policy seed clustering module.

Groups claims into "policy-shaped" bundles by (procedure_code, claim_type, specialty)
and computes summary statistics per group to serve as inputs to policy generation.

Each cluster captures the CPT-context of a group of claims and provides a
deterministic, reproducible summary for downstream policy authoring workflows.

Spec: Features — Policy Seeds (issue: Build claims clusters for CPT-context groupings)
"""

from __future__ import annotations

import contextlib
import json
from collections import Counter
from pathlib import Path  # noqa: TC003

import polars as pl


def _compute_top_dx_codes(codes_json_list: pl.Series | list[str | None], top_n: int = 5) -> str:
    """Compute top N diagnosis codes from a list of JSON-encoded code lists.

    Args:
        codes_json_list: A Polars Series or list of JSON strings, each encoding
            a list of diagnosis codes.
        top_n: Number of top codes to return.

    Returns:
        JSON string array with objects ``{"code": str, "count": int}``.
    """
    counter: Counter[str] = Counter()
    items: list[str | None] = (
        codes_json_list.to_list()
        if isinstance(codes_json_list, pl.Series)
        else list(codes_json_list)
    )
    for codes_json in items:
        if codes_json:
            try:
                codes = json.loads(codes_json)
                counter.update(codes)
            except (json.JSONDecodeError, TypeError):
                pass
    top = counter.most_common(top_n)
    return json.dumps([{"code": c, "count": n} for c, n in top])


def build_policy_seeds(
    df: pl.DataFrame,
    min_claims: int = 30,
    top_n_dx: int = 5,
) -> pl.DataFrame:
    """Group claims into policy-shaped clusters by (procedure_code, claim_type, specialty).

    Args:
        df: Claims DataFrame in canonical schema format.
        min_claims: Minimum number of claims per cluster; groups below this are excluded.
        top_n_dx: Number of top diagnosis codes to include per cluster.

    Returns:
        DataFrame with one row per cluster, sorted deterministically by
        (procedure_code, claim_type, specialty).

        Columns:
            - procedure_code, claim_type, specialty: grouping keys
            - n_claims: number of claims in cluster
            - approval_rate: fraction of non-denied claims
            - denial_rate: fraction of denied claims
            - avg_claim_amount: mean allowed_amount
            - p50_claim_amount: median (50th percentile) allowed_amount
            - p90_claim_amount: 90th percentile allowed_amount
            - top_diagnosis_codes: JSON string with top N diagnosis codes and counts
    """
    seeds = (
        df.group_by(["procedure_code", "claim_type", "specialty"])
        .agg(
            pl.len().alias("n_claims"),
            (pl.col("denial_flag") == "N").mean().alias("approval_rate"),
            (pl.col("denial_flag") == "Y").mean().alias("denial_rate"),
            pl.col("allowed_amount").mean().alias("avg_claim_amount"),
            pl.col("allowed_amount").quantile(0.5).alias("p50_claim_amount"),
            pl.col("allowed_amount").quantile(0.9).alias("p90_claim_amount"),
            pl.col("diagnosis_codes").alias("_dx_codes_raw"),
        )
        .filter(pl.col("n_claims") >= min_claims)
        .sort(["procedure_code", "claim_type", "specialty"])
        .with_columns(
            pl.col("_dx_codes_raw")
            .map_elements(
                lambda x: _compute_top_dx_codes(x, top_n=top_n_dx),
                return_dtype=pl.Utf8,
            )
            .alias("top_diagnosis_codes")
        )
        .drop("_dx_codes_raw")
    )
    return seeds


def write_policy_seeds(df: pl.DataFrame, output_dir: Path) -> tuple[Path, Path]:
    """Write policy seeds to parquet and JSONL files.

    Args:
        df: Policy seeds DataFrame from :func:`build_policy_seeds`.
        output_dir: Directory to write output files.

    Returns:
        Tuple of ``(parquet_path, jsonl_path)``.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "policy_seeds.parquet"
    jsonl_path = output_dir / "policy_seeds.jsonl"

    df.write_parquet(parquet_path)

    # Write JSONL — one JSON object per row
    rows = df.to_dicts()
    with jsonl_path.open("w") as f:
        for row in rows:
            # Deserialise top_diagnosis_codes so it embeds as a proper JSON array
            if "top_diagnosis_codes" in row and isinstance(row["top_diagnosis_codes"], str):
                with contextlib.suppress(json.JSONDecodeError, TypeError):
                    row["top_diagnosis_codes"] = json.loads(row["top_diagnosis_codes"])
            f.write(json.dumps(row) + "\n")

    return parquet_path, jsonl_path
