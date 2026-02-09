"""Feature engineering module.

Computes UM-relevant features from validated claims:
- Provider-level aggregates (cost, volume, OON rate, denial rate, entity age)
- Temporal aggregates (weekly/monthly volumes, rolling averages)
- Service category tagging
- Cost-per-unit and allowed-to-billed ratio

All transforms use vectorized Polars operations — no Python loops.
Spec: SR-3
"""

from __future__ import annotations

from datetime import date

import polars as pl

from um_claims.config import get_service_category


def tag_service_categories(df: pl.DataFrame) -> pl.DataFrame:
    """Add a 'service_category' column derived from procedure_code.

    Uses prefix-based mapping defined in config.SERVICE_CATEGORIES.
    """
    return df.with_columns(
        pl.col("procedure_code")
        .map_elements(get_service_category, return_dtype=pl.Utf8)
        .alias("service_category")
    )


def compute_provider_features(df: pl.DataFrame) -> pl.DataFrame:
    """Compute per-provider aggregate features.

    Features:
    - total_claims: number of claims
    - total_allowed: sum of allowed_amount
    - avg_allowed: mean allowed_amount
    - total_billed: sum of billed_amount
    - avg_units: mean units
    - oon_rate: fraction of OON claims
    - denial_rate: fraction of denied claims
    - appeal_rate: fraction of claims with appeals
    - dme_rate: fraction of DME claims
    - first_claim_date: earliest service_date
    - last_claim_date: latest service_date
    - entity_age_days: days between first and last claim
    - avg_billed_to_allowed_ratio: mean(billed / allowed)
    - cost_per_unit: total_allowed / total_units
    - unique_members: count of distinct members
    - unique_procedure_codes: count of distinct procedure codes
    """
    return (
        df.group_by("provider_id")
        .agg(
            pl.len().alias("total_claims"),
            pl.col("allowed_amount").sum().alias("total_allowed"),
            pl.col("allowed_amount").mean().alias("avg_allowed"),
            pl.col("billed_amount").sum().alias("total_billed"),
            pl.col("units").mean().alias("avg_units"),
            pl.col("units").sum().alias("total_units"),
            (pl.col("network_status") == "OON").mean().alias("oon_rate"),
            (pl.col("denial_flag") == "Y").mean().alias("denial_rate"),
            (pl.col("appeal_flag") == "Y").mean().alias("appeal_rate"),
            (pl.col("dme_flag") == "Y").mean().alias("dme_rate"),
            pl.col("service_date").min().alias("first_claim_date"),
            pl.col("service_date").max().alias("last_claim_date"),
            pl.col("member_id").n_unique().alias("unique_members"),
            pl.col("procedure_code").n_unique().alias("unique_procedure_codes"),
            pl.col("specialty").first().alias("specialty"),
            pl.col("geography_state").first().alias("geography_state"),
            pl.col("geography_region").first().alias("geography_region"),
        )
        .with_columns(
            (pl.col("last_claim_date") - pl.col("first_claim_date"))
            .dt.total_days()
            .alias("entity_age_days"),
            (pl.col("total_billed") / pl.col("total_allowed"))
            .alias("avg_billed_to_allowed_ratio"),
            (pl.col("total_allowed") / pl.col("total_units")).alias("cost_per_unit"),
        )
    )


def compute_temporal_features(df: pl.DataFrame) -> pl.DataFrame:
    """Compute weekly and monthly claim volumes with rolling averages.

    Returns a DataFrame with columns:
    - period_start: start of the week/month
    - period_type: 'weekly' or 'monthly'
    - total_claims, total_allowed, total_billed, denial_count, oon_count
    - rolling_4w_claims: 4-period rolling average of total_claims (weekly only)
    """
    # Weekly aggregation
    weekly = (
        df.with_columns(pl.col("service_date").dt.truncate("1w").alias("period_start"))
        .group_by("period_start")
        .agg(
            pl.len().alias("total_claims"),
            pl.col("allowed_amount").sum().alias("total_allowed"),
            pl.col("billed_amount").sum().alias("total_billed"),
            (pl.col("denial_flag") == "Y").sum().alias("denial_count"),
            (pl.col("network_status") == "OON").sum().alias("oon_count"),
        )
        .sort("period_start")
        .with_columns(
            pl.lit("weekly").alias("period_type"),
            pl.col("total_claims")
            .rolling_mean(window_size=4, min_samples=1)
            .alias("rolling_4w_claims"),
        )
    )

    # Monthly aggregation
    monthly = (
        df.with_columns(pl.col("service_date").dt.truncate("1mo").alias("period_start"))
        .group_by("period_start")
        .agg(
            pl.len().alias("total_claims"),
            pl.col("allowed_amount").sum().alias("total_allowed"),
            pl.col("billed_amount").sum().alias("total_billed"),
            (pl.col("denial_flag") == "Y").sum().alias("denial_count"),
            (pl.col("network_status") == "OON").sum().alias("oon_count"),
        )
        .sort("period_start")
        .with_columns(
            pl.lit("monthly").alias("period_type"),
            pl.col("total_claims")
            .rolling_mean(window_size=3, min_samples=1)
            .alias("rolling_4w_claims"),
        )
    )

    return pl.concat([weekly, monthly])


def compute_service_category_features(df: pl.DataFrame) -> pl.DataFrame:
    """Compute features aggregated by service category.

    Requires 'service_category' column (call tag_service_categories first).
    """
    if "service_category" not in df.columns:
        df = tag_service_categories(df)

    return (
        df.group_by("service_category")
        .agg(
            pl.len().alias("total_claims"),
            pl.col("allowed_amount").sum().alias("total_allowed"),
            pl.col("allowed_amount").mean().alias("avg_allowed"),
            pl.col("billed_amount").sum().alias("total_billed"),
            (pl.col("denial_flag") == "Y").mean().alias("denial_rate"),
            (pl.col("network_status") == "OON").mean().alias("oon_rate"),
            pl.col("units").sum().alias("total_units"),
        )
        .with_columns(
            (pl.col("total_allowed") / pl.col("total_units")).alias("cost_per_unit"),
        )
    )


def compute_all_features(
    df: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Compute all feature sets from a validated claims DataFrame.

    Returns:
        Dictionary with keys: 'provider', 'temporal', 'service_category',
        and the enriched 'claims' DataFrame with service_category tagged.
    """
    enriched = tag_service_categories(df)

    return {
        "claims": enriched,
        "provider": compute_provider_features(enriched),
        "temporal": compute_temporal_features(enriched),
        "service_category": compute_service_category_features(enriched),
    }
