"""Appeals and grievances analytics module.

Analyzes denial-to-appeal funnels, top denial categories driving admin burden,
and provider-level appeal patterns.

Spec: SR-6
"""

from __future__ import annotations

from pydantic import BaseModel, Field
import polars as pl


class DenialCategory(BaseModel):
    """Metrics for a single denial reason category."""

    category: str
    denial_count: int
    appeal_count: int
    appeal_rate: float = Field(description="appeal_count / denial_count")
    grievance_count: int
    total_billed: float
    total_allowed: float


class ProviderAppealProfile(BaseModel):
    """Provider-level appeal analysis."""

    provider_id: str
    total_denials: int
    total_appeals: int
    appeal_rate: float
    top_denial_reason: str
    total_billed_denied: float


class AppealsReport(BaseModel):
    """Full appeals and grievances analysis report."""

    total_claims: int = 0
    total_denials: int = 0
    total_appeals: int = 0
    total_grievances: int = 0
    overall_denial_rate: float = 0.0
    overall_appeal_rate: float = 0.0
    estimated_admin_cost: float = Field(
        default=0.0, description="appeals × cost_per_appeal"
    )
    categories: list[DenialCategory] = Field(default_factory=list)
    top_appeal_providers: list[ProviderAppealProfile] = Field(default_factory=list)


def analyze_appeals(
    df: pl.DataFrame,
    cost_per_appeal: float = 350.0,
    top_n_providers: int = 10,
) -> AppealsReport:
    """Analyze denial-to-appeal funnels and admin burden.

    Args:
        df: Claims DataFrame.
        cost_per_appeal: Estimated admin cost per appeal (USD).
        top_n_providers: Number of top providers to profile by appeal volume.

    Returns:
        AppealsReport with funnel metrics, category breakdown, and provider profiles.
    """
    total_claims = df.height
    total_denials = df.filter(pl.col("denial_flag") == "Y").height
    total_appeals = df.filter(pl.col("appeal_flag") == "Y").height
    total_grievances = df.filter(pl.col("grievance_flag") == "Y").height

    overall_denial_rate = total_denials / total_claims if total_claims > 0 else 0.0
    overall_appeal_rate = total_appeals / total_denials if total_denials > 0 else 0.0
    estimated_admin_cost = total_appeals * cost_per_appeal

    # --- Category-level breakdown ---
    denied = df.filter(pl.col("denial_flag") == "Y")
    categories: list[DenialCategory] = []

    if denied.height > 0:
        cat_agg = (
            denied.group_by("denial_reason_category")
            .agg(
                pl.len().alias("denial_count"),
                (pl.col("appeal_flag") == "Y").sum().alias("appeal_count"),
                (pl.col("grievance_flag") == "Y").sum().alias("grievance_count"),
                pl.col("billed_amount").sum().alias("total_billed"),
                pl.col("allowed_amount").sum().alias("total_allowed"),
            )
            .filter(pl.col("denial_reason_category").is_not_null())
            .sort("denial_count", descending=True)
        )

        for row in cat_agg.iter_rows(named=True):
            rate = row["appeal_count"] / row["denial_count"] if row["denial_count"] > 0 else 0.0
            categories.append(
                DenialCategory(
                    category=row["denial_reason_category"],
                    denial_count=row["denial_count"],
                    appeal_count=row["appeal_count"],
                    appeal_rate=round(rate, 4),
                    grievance_count=row["grievance_count"],
                    total_billed=round(row["total_billed"], 2),
                    total_allowed=round(row["total_allowed"], 2),
                )
            )

    # --- Provider-level appeal profiles ---
    providers_with_denials = (
        df.filter(pl.col("denial_flag") == "Y")
        .group_by("provider_id")
        .agg(
            pl.len().alias("total_denials"),
            (pl.col("appeal_flag") == "Y").sum().alias("total_appeals"),
            pl.col("denial_reason_category").mode().first().alias("top_denial_reason"),
            pl.col("billed_amount").sum().alias("total_billed_denied"),
        )
        .with_columns(
            (pl.col("total_appeals") / pl.col("total_denials")).alias("appeal_rate"),
        )
        .sort("total_appeals", descending=True)
        .head(top_n_providers)
    )

    top_providers: list[ProviderAppealProfile] = []
    for row in providers_with_denials.iter_rows(named=True):
        top_providers.append(
            ProviderAppealProfile(
                provider_id=row["provider_id"],
                total_denials=row["total_denials"],
                total_appeals=row["total_appeals"],
                appeal_rate=round(row["appeal_rate"], 4),
                top_denial_reason=row["top_denial_reason"] or "unknown",
                total_billed_denied=round(row["total_billed_denied"], 2),
            )
        )

    return AppealsReport(
        total_claims=total_claims,
        total_denials=total_denials,
        total_appeals=total_appeals,
        total_grievances=total_grievances,
        overall_denial_rate=round(overall_denial_rate, 4),
        overall_appeal_rate=round(overall_appeal_rate, 4),
        estimated_admin_cost=round(estimated_admin_cost, 2),
        categories=categories,
        top_appeal_providers=top_providers,
    )
