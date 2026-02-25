"""Report generation module.

Produces a Markdown report with key metrics, top anomalies,
policy impact summaries, and visualizations.

Spec: SR-8
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import polars as pl

from um_claims.appeals import AppealsReport
from um_claims.benchmarking import BenchmarkReport
from um_claims.config import PipelineConfig
from um_claims.detection import Flag
from um_claims.policy_sim import PolicyImpactReport


def _save_cost_distribution(df: pl.DataFrame, output_dir: Path) -> str:
    """Generate and save a cost distribution histogram."""
    fig, ax = plt.subplots(figsize=(10, 6))
    amounts = df["allowed_amount"].to_numpy()

    ax.hist(amounts, bins=100, color="#2196F3", alpha=0.7, edgecolor="white")
    ax.set_xlabel("Allowed Amount ($)")
    ax.set_ylabel("Frequency")
    ax.set_title("Claims Cost Distribution (Allowed Amount)")
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3)

    path = output_dir / "figures" / "cost_distribution.png"
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return "figures/cost_distribution.png"


def _save_utilization_trend(temporal_features: pl.DataFrame, output_dir: Path) -> str:
    """Generate and save a utilization trend chart with control bands."""
    weekly = temporal_features.filter(pl.col("period_type") == "weekly").sort("period_start")

    if weekly.height == 0:
        return ""

    fig, ax = plt.subplots(figsize=(12, 6))

    dates = weekly["period_start"].to_list()
    claims = weekly["total_claims"].to_list()
    rolling = weekly["rolling_4w_claims"].to_list()

    ax.plot(dates, claims, color="#2196F3", alpha=0.5, linewidth=0.8, label="Weekly Claims")
    ax.plot(dates, rolling, color="#FF5722", linewidth=2, label="4-Week Rolling Avg")

    # Control bands: mean ± 2σ of rolling average
    import numpy as np
    rolling_arr = np.array([v if v is not None else 0 for v in rolling])
    mean_val = np.mean(rolling_arr)
    std_val = np.std(rolling_arr)
    ax.axhline(mean_val, color="gray", linestyle="--", alpha=0.5, label="Mean")
    ax.axhline(mean_val + 2 * std_val, color="red", linestyle=":", alpha=0.5, label="Upper Control (+2σ)")
    ax.axhline(max(0, mean_val - 2 * std_val), color="green", linestyle=":", alpha=0.5, label="Lower Control (-2σ)")

    ax.set_xlabel("Week")
    ax.set_ylabel("Claim Volume")
    ax.set_title("Weekly Utilization Trend with Control Bands")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    fig.autofmt_xdate()

    path = output_dir / "figures" / "utilization_trend.png"
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return "figures/utilization_trend.png"


def _save_denial_funnel(appeals_report: AppealsReport, output_dir: Path) -> str:
    """Generate and save a denial-to-appeal funnel chart."""
    if not appeals_report.categories:
        return ""

    fig, ax = plt.subplots(figsize=(10, 6))

    categories = [c.category for c in appeals_report.categories]
    denials = [c.denial_count for c in appeals_report.categories]
    appeals = [c.appeal_count for c in appeals_report.categories]

    x = range(len(categories))
    width = 0.35

    ax.bar([i - width / 2 for i in x], denials, width, label="Denials", color="#F44336", alpha=0.7)
    ax.bar([i + width / 2 for i in x], appeals, width, label="Appeals", color="#FF9800", alpha=0.7)

    ax.set_xlabel("Denial Reason Category")
    ax.set_ylabel("Count")
    ax.set_title("Denial → Appeal Funnel by Category")
    ax.set_xticks(list(x))
    ax.set_xticklabels(categories, rotation=30, ha="right")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    path = output_dir / "figures" / "denial_funnel.png"
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return "figures/denial_funnel.png"


def _render_policy_insights(policy_kpis: list[dict], rank_by: str = "total_amount") -> list[str]:
    """Render a Policy Insights Markdown section from KPI dicts.

    Args:
        policy_kpis: List of per-policy KPI dicts as produced by
            :func:`um_claims.analytics.policy_kpis.compute_policy_kpis`.
        rank_by: Key to sort the table by (``"total_amount"`` or
            ``"denial_rate"``).  Defaults to ``"total_amount"``.

    Returns:
        List of Markdown lines (without trailing newline on each).
    """
    lines: list[str] = []
    lines.append("## Policy Insights\n")

    if not policy_kpis:
        lines.append("No policy KPI data available.\n")
        return lines

    # Sort by chosen key (descending)
    sorted_kpis = sorted(
        policy_kpis,
        key=lambda k: k.get(rank_by, 0),
        reverse=True,
    )

    lines.append(f"Ranked by **{rank_by}** (descending).\n")
    lines.append(
        "| # | Policy ID | Claims | Total Amount | Avg Amount "
        "| Approval Rate | Denial Rate | Top Dx | Top Specialties |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")

    for i, kpi in enumerate(sorted_kpis, 1):
        top_dx = ", ".join(kpi.get("top_dx", [])[:3]) or "—"
        top_spec = ", ".join(kpi.get("top_specialties", [])[:3]) or "—"
        lines.append(
            f"| {i} | {kpi['policy_id']} | {kpi['n_claims']:,} "
            f"| ${kpi['total_amount']:,.2f} | ${kpi['avg_amount']:,.2f} "
            f"| {kpi['approval_rate']:.2%} | {kpi['denial_rate']:.2%} "
            f"| {top_dx} | {top_spec} |"
        )
    lines.append("")
    return lines


def generate_report(
    config: PipelineConfig,
    df: pl.DataFrame,
    flags: list[Flag],
    policy_report: PolicyImpactReport,
    appeals_report: AppealsReport,
    benchmark_report: BenchmarkReport,
    temporal_features: pl.DataFrame,
    output_dir: Path,
    policy_kpis: list[dict] | None = None,
    rank_by: str = "total_amount",
) -> Path:
    """Generate the full Markdown report with visualizations.

    Args:
        config: Pipeline configuration.
        df: Claims DataFrame.
        flags: Detection flags.
        policy_report: Policy impact analysis.
        appeals_report: Appeals analysis.
        benchmark_report: Benchmark comparisons.
        temporal_features: Temporal feature DataFrame.
        output_dir: Output directory.
        policy_kpis: Optional per-policy KPI dicts (from
            :func:`um_claims.analytics.policy_kpis.compute_policy_kpis`).
        rank_by: Sort key for the Policy Insights table
            (``"total_amount"`` or ``"denial_rate"``).

    Returns:
        Path to the generated report file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Generate visualizations
    cost_dist_path = _save_cost_distribution(df, output_dir)
    trend_path = _save_utilization_trend(temporal_features, output_dir)
    funnel_path = _save_denial_funnel(appeals_report, output_dir)

    # Build report content
    lines: list[str] = []
    lines.append("# UM Claims Analytics Report\n")
    lines.append(f"**Generated:** {now}\n")
    lines.append(f"**Seed:** {config.seed} | **Claims:** {len(df):,}\n")
    lines.append("---\n")

    # --- Key Metrics ---
    lines.append("## Key Metrics\n")
    lines.append(f"| Metric | Value |")
    lines.append(f"|---|---|")
    lines.append(f"| Total Claims | {len(df):,} |")
    total_allowed = df["allowed_amount"].sum()
    total_billed = df["billed_amount"].sum()
    lines.append(f"| Total Billed | ${total_billed:,.2f} |")
    lines.append(f"| Total Allowed | ${total_allowed:,.2f} |")
    lines.append(f"| Overall Denial Rate | {appeals_report.overall_denial_rate:.2%} |")
    lines.append(f"| Overall Appeal Rate | {appeals_report.overall_appeal_rate:.2%} |")
    oon_count = df.filter(pl.col("network_status") == "OON").height
    lines.append(f"| OON Rate | {oon_count / len(df):.2%} |")
    lines.append(f"| Total Flags | {len(flags)} |")
    lines.append("")

    # --- Cost Distribution ---
    if cost_dist_path:
        lines.append("## Cost Distribution\n")
        lines.append(f"![Cost Distribution]({cost_dist_path})\n")

    # --- Utilization Trend ---
    if trend_path:
        lines.append("## Utilization Trend\n")
        lines.append(f"![Utilization Trend]({trend_path})\n")

    # --- Top Anomalies ---
    lines.append("## Top Anomalies (Detection Flags)\n")
    if flags:
        lines.append(f"Total flags: **{len(flags)}**\n")
        high_flags = [f for f in flags if f.severity == "high"]
        medium_flags = [f for f in flags if f.severity == "medium"]
        lines.append(f"- **High severity:** {len(high_flags)}")
        lines.append(f"- **Medium severity:** {len(medium_flags)}")
        lines.append("")

        lines.append("### Top 20 Flags\n")
        lines.append("| # | Rule | Entity | Severity | Actual | Threshold | Description |")
        lines.append("|---|---|---|---|---|---|---|")
        for i, flag in enumerate(flags[:20], 1):
            lines.append(
                f"| {i} | {flag.rule_name} | {flag.entity_id} | {flag.severity} | "
                f"{flag.actual_value:.2f} | {flag.threshold:.2f} | {flag.description} |"
            )
        lines.append("")
    else:
        lines.append("No anomalies detected.\n")

    # --- Policy Impact ---
    lines.append("## Policy Impact Analysis\n")
    if policy_report.impacts:
        for impact in policy_report.impacts:
            lines.append(f"### {impact.policy_id}: {impact.description}\n")
            lines.append(f"**Effective Date:** {impact.effective_date}\n")
            lines.append("| Metric | Pre | Post | Change |")
            lines.append("|---|---|---|---|")
            lines.append(
                f"| Volume | {impact.pre_metrics.volume:,} | {impact.post_metrics.volume:,} | "
                f"{impact.volume_change_pct:+.1f}% |"
            )
            lines.append(
                f"| Total Allowed | ${impact.pre_metrics.total_allowed:,.2f} | "
                f"${impact.post_metrics.total_allowed:,.2f} | {impact.cost_change_pct:+.1f}% |"
            )
            lines.append(
                f"| Denial Rate | {impact.pre_metrics.denial_rate:.2%} | "
                f"{impact.post_metrics.denial_rate:.2%} | {impact.denial_rate_change:+.4f} |"
            )
            lines.append(
                f"| OON Rate | {impact.pre_metrics.oon_rate:.2%} | "
                f"{impact.post_metrics.oon_rate:.2%} | {impact.oon_rate_change:+.4f} |"
            )
            if impact.rebound_detected:
                lines.append(f"\n⚠️ **Rebound Detected:** {impact.rebound_detail}\n")
            lines.append("")
    else:
        lines.append("No policy change events configured.\n")

    # --- Appeals Analysis ---
    lines.append("## Appeals & Grievances\n")
    if funnel_path:
        lines.append(f"![Denial Funnel]({funnel_path})\n")

    lines.append(f"| Metric | Value |")
    lines.append(f"|---|---|")
    lines.append(f"| Total Denials | {appeals_report.total_denials:,} |")
    lines.append(f"| Total Appeals | {appeals_report.total_appeals:,} |")
    lines.append(f"| Total Grievances | {appeals_report.total_grievances:,} |")
    lines.append(f"| Appeal Rate (of denials) | {appeals_report.overall_appeal_rate:.2%} |")
    lines.append(f"| Est. Admin Cost | ${appeals_report.estimated_admin_cost:,.2f} |")
    lines.append("")

    if appeals_report.categories:
        lines.append("### Top Denial Categories\n")
        lines.append("| Category | Denials | Appeals | Appeal Rate | Billed |")
        lines.append("|---|---|---|---|---|")
        for cat in appeals_report.categories[:5]:
            lines.append(
                f"| {cat.category} | {cat.denial_count:,} | {cat.appeal_count:,} | "
                f"{cat.appeal_rate:.2%} | ${cat.total_billed:,.2f} |"
            )
        lines.append("")

    # --- Benchmarking ---
    lines.append("## Benchmarking vs Peer Baselines\n")
    if benchmark_report.comparisons:
        lines.append("| Metric | Internal | Baseline | Variance | Threshold | Status |")
        lines.append("|---|---|---|---|---|---|")
        for comp in benchmark_report.comparisons:
            status = "⚠️ FLAGGED" if comp.exceeds_threshold else "✅ OK"
            lines.append(
                f"| {comp.metric_name} | {comp.internal_value:.4f} | "
                f"{comp.baseline_value:.4f} | {comp.variance:+.2%} | "
                f"±{comp.threshold_pct:.0%} | {status} |"
            )
        lines.append(f"\n**Flagged metrics:** {benchmark_report.flagged_count}\n")
    else:
        lines.append("No benchmarks configured.\n")

    # --- Policy Insights ---
    if policy_kpis is not None:
        lines.extend(_render_policy_insights(policy_kpis, rank_by=rank_by))

    # --- Recommended Next Questions ---
    lines.append("## Recommended Next Questions\n")
    lines.append("1. **High-severity flags**: Review the top anomalies — are flagged suppliers known entities, or do they warrant SIU investigation?")
    lines.append("2. **Policy rebound**: If a rebound was detected, consider whether the policy removal was premature or if utilization is within expected bounds.")
    lines.append("3. **Appeal burden**: Which denial categories have the highest appeal rates? Could pre-service review reduce post-service denials?")
    lines.append("4. **OON exposure**: Are OON DME clusters concentrated geographically? Consider network adequacy analysis.")
    lines.append("5. **Benchmark variance**: For metrics flagged above, drill into the underlying claims to understand whether variance is clinically justified.")
    lines.append("")

    # Write report
    report_path = output_dir / "report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")

    return report_path
