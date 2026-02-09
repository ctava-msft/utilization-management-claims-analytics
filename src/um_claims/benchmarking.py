"""Benchmarking module.

Compares internal utilization metrics to synthetic peer baselines.
Flags metrics that exceed variance thresholds.

Spec: SR-7
"""

from __future__ import annotations

from pydantic import BaseModel, Field
import polars as pl

from um_claims.config import BenchmarkBaseline


class BenchmarkComparison(BaseModel):
    """Comparison of a single metric to its peer baseline."""

    metric_name: str
    internal_value: float
    baseline_value: float
    variance: float = Field(description="(internal - baseline) / baseline")
    threshold_pct: float = Field(description="Configured variance threshold")
    exceeds_threshold: bool
    direction: str = Field(description="'above' or 'below' or 'within'")


class BenchmarkReport(BaseModel):
    """Report of all benchmark comparisons."""

    comparisons: list[BenchmarkComparison] = Field(default_factory=list)
    flagged_count: int = 0


def compute_internal_metrics(df: pl.DataFrame) -> dict[str, float]:
    """Compute the internal metrics that can be benchmarked.

    Returns:
        Dictionary mapping metric_name to computed value.
    """
    total_claims = df.height
    if total_claims == 0:
        return {"denial_rate": 0.0, "oon_rate": 0.0, "cost_per_claim": 0.0}

    denial_rate = df.filter(pl.col("denial_flag") == "Y").height / total_claims
    oon_rate = df.filter(pl.col("network_status") == "OON").height / total_claims
    total_allowed = float(df["allowed_amount"].sum())
    cost_per_claim = total_allowed / total_claims

    return {
        "denial_rate": round(denial_rate, 4),
        "oon_rate": round(oon_rate, 4),
        "cost_per_claim": round(cost_per_claim, 2),
    }


def compare_to_benchmarks(
    df: pl.DataFrame,
    baselines: list[BenchmarkBaseline],
) -> BenchmarkReport:
    """Compare internal metrics to peer baselines.

    Args:
        df: Claims DataFrame.
        baselines: List of peer benchmark definitions.

    Returns:
        BenchmarkReport with per-metric comparisons and flag count.
    """
    internal = compute_internal_metrics(df)
    comparisons: list[BenchmarkComparison] = []

    for baseline in baselines:
        internal_value = internal.get(baseline.metric_name, 0.0)
        if baseline.baseline_value == 0:
            variance = 0.0
        else:
            variance = (internal_value - baseline.baseline_value) / baseline.baseline_value

        exceeds = abs(variance) > baseline.threshold_pct

        if variance > baseline.threshold_pct:
            direction = "above"
        elif variance < -baseline.threshold_pct:
            direction = "below"
        else:
            direction = "within"

        comparisons.append(
            BenchmarkComparison(
                metric_name=baseline.metric_name,
                internal_value=round(internal_value, 4),
                baseline_value=baseline.baseline_value,
                variance=round(variance, 4),
                threshold_pct=baseline.threshold_pct,
                exceeds_threshold=exceeds,
                direction=direction,
            )
        )

    flagged_count = sum(1 for c in comparisons if c.exceeds_threshold)

    return BenchmarkReport(comparisons=comparisons, flagged_count=flagged_count)
