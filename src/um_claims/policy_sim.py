"""Policy effectiveness analysis module.

Compares utilization before and after simulated policy changes.
Detects "rebound" patterns where utilization returns to pre-change levels.

Spec: SR-5
"""

from __future__ import annotations

import logging
from datetime import timedelta

from pydantic import BaseModel, Field
import polars as pl

from um_claims.config import DetectionConfig, PolicyChangeEvent

logger = logging.getLogger(__name__)


class PolicyMetrics(BaseModel):
    """Metrics for a single period (pre or post)."""

    volume: int = 0
    total_allowed: float = 0.0
    avg_allowed: float = 0.0
    denial_rate: float = 0.0
    oon_rate: float = 0.0


class PolicyImpact(BaseModel):
    """Impact analysis for a single policy change event."""

    policy_id: str
    description: str
    effective_date: str
    affected_services: list[str]
    pre_metrics: PolicyMetrics
    post_metrics: PolicyMetrics
    volume_change_pct: float = 0.0
    cost_change_pct: float = 0.0
    denial_rate_change: float = 0.0
    oon_rate_change: float = 0.0
    rebound_detected: bool = False
    rebound_detail: str = ""


class PolicyImpactReport(BaseModel):
    """Full policy impact report across all events."""

    impacts: list[PolicyImpact] = Field(default_factory=list)


def _compute_period_metrics(df: pl.DataFrame) -> PolicyMetrics:
    """Compute metrics for a filtered period of claims."""
    if df.height == 0:
        return PolicyMetrics()

    return PolicyMetrics(
        volume=df.height,
        total_allowed=float(df["allowed_amount"].sum()),
        avg_allowed=float(df["allowed_amount"].mean()),  # type: ignore[arg-type]
        denial_rate=float((df["denial_flag"] == "Y").mean()),  # type: ignore[arg-type]
        oon_rate=float((df["network_status"] == "OON").mean()),  # type: ignore[arg-type]
    )


def analyze_policy_impact(
    df: pl.DataFrame,
    events: list[PolicyChangeEvent],
    config: DetectionConfig,
) -> PolicyImpactReport:
    """Analyze the impact of policy change events on utilization.

    For each event:
    1. Filter claims to affected services (by procedure code prefix).
    2. Split into pre-period (same duration before effective_date) and post-period.
    3. Compare volume, cost, denial rate, OON rate.
    4. Detect rebound if post-change utilization returns to pre-change levels.

    Args:
        df: Claims DataFrame with service_date and procedure_code.
        events: List of policy change events.
        config: Detection config with rebound parameters.

    Returns:
        PolicyImpactReport with per-event analysis.
    """
    impacts: list[PolicyImpact] = []

    for event in events:
        # Filter to affected services
        affected_mask = pl.lit(False)
        for prefix in event.affected_procedure_prefixes:
            affected_mask = affected_mask | pl.col("procedure_code").str.starts_with(prefix)

        affected_claims = df.filter(affected_mask)

        logger.debug(
            "Policy %s: prefixes=%s, matched %d/%d claims, unique CPTs=%d",
            event.policy_id,
            event.affected_procedure_prefixes,
            affected_claims.height,
            df.height,
            affected_claims["procedure_code"].n_unique() if affected_claims.height > 0 else 0,
        )

        if affected_claims.height == 0:
            impacts.append(
                PolicyImpact(
                    policy_id=event.policy_id,
                    description=event.description,
                    effective_date=event.effective_date.isoformat(),
                    affected_services=event.affected_procedure_prefixes,
                    pre_metrics=PolicyMetrics(),
                    post_metrics=PolicyMetrics(),
                )
            )
            continue

        # Define pre and post periods
        # Pre: same duration before effective date as the rebound window
        pre_window = timedelta(weeks=config.rebound_window_weeks)
        pre_start = event.effective_date - pre_window
        post_end = event.effective_date + pre_window

        pre_claims = affected_claims.filter(
            (pl.col("service_date") >= pre_start) & (pl.col("service_date") < event.effective_date)
        )
        post_claims = affected_claims.filter(
            (pl.col("service_date") >= event.effective_date)
            & (pl.col("service_date") < post_end)
        )

        pre_metrics = _compute_period_metrics(pre_claims)
        post_metrics = _compute_period_metrics(post_claims)

        # Compute changes
        volume_change_pct = 0.0
        cost_change_pct = 0.0
        if pre_metrics.volume > 0:
            volume_change_pct = (
                (post_metrics.volume - pre_metrics.volume) / pre_metrics.volume
            ) * 100
        if pre_metrics.total_allowed > 0:
            cost_change_pct = (
                (post_metrics.total_allowed - pre_metrics.total_allowed) / pre_metrics.total_allowed
            ) * 100

        denial_rate_change = post_metrics.denial_rate - pre_metrics.denial_rate
        oon_rate_change = post_metrics.oon_rate - pre_metrics.oon_rate

        # Rebound detection: if post volume is >= threshold_pct of pre volume
        # after a "removed" policy (expected to decrease)
        rebound_detected = False
        rebound_detail = ""
        if event.change_type == "removed" and pre_metrics.volume > 0:
            if post_metrics.volume >= pre_metrics.volume * config.rebound_threshold_pct:
                rebound_detected = True
                rebound_detail = (
                    f"Post-removal volume ({post_metrics.volume}) is "
                    f"{post_metrics.volume / pre_metrics.volume:.0%} of pre-removal volume "
                    f"({pre_metrics.volume}). Utilization did not decrease as expected."
                )

        impacts.append(
            PolicyImpact(
                policy_id=event.policy_id,
                description=event.description,
                effective_date=event.effective_date.isoformat(),
                affected_services=event.affected_procedure_prefixes,
                pre_metrics=pre_metrics,
                post_metrics=post_metrics,
                volume_change_pct=round(volume_change_pct, 2),
                cost_change_pct=round(cost_change_pct, 2),
                denial_rate_change=round(denial_rate_change, 4),
                oon_rate_change=round(oon_rate_change, 4),
                rebound_detected=rebound_detected,
                rebound_detail=rebound_detail,
            )
        )

    return PolicyImpactReport(impacts=impacts)
