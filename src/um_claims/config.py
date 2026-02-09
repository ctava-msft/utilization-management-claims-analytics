"""Configuration models for the UM Claims Analytics pipeline.

All pipeline behavior is controlled via Pydantic models defined here.
Configuration is the single source of truth for seeds, thresholds, and policy definitions.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pydantic import BaseModel, Field


class PolicyChangeEvent(BaseModel):
    """A simulated policy change — toggling authorization requirements on/off."""

    policy_id: str = Field(description="Unique identifier for this policy change")
    affected_procedure_prefixes: list[str] = Field(
        description="Procedure code prefixes affected (e.g., ['CPT-7'] for imaging)"
    )
    change_type: str = Field(description="'added' or 'removed'")
    effective_date: date = Field(description="Date the policy change takes effect")
    description: str = Field(default="", description="Human-readable description")


class BenchmarkBaseline(BaseModel):
    """A synthetic peer benchmark for comparison."""

    metric_name: str = Field(description="e.g., 'denial_rate', 'oon_rate', 'cost_per_member'")
    baseline_value: float = Field(description="Peer baseline value")
    threshold_pct: float = Field(
        default=0.10, description="Variance threshold (fraction) to trigger a flag"
    )


class DetectionConfig(BaseModel):
    """Thresholds for outlier and anomaly detection rules."""

    zscore_threshold: float = Field(default=2.0, description="Z-score threshold for outlier flags")
    new_entity_days: int = Field(
        default=90, description="Suppliers active < this many days are 'new entities'"
    )
    new_entity_volume_percentile: float = Field(
        default=0.90, description="Volume percentile threshold for new entity flags"
    )
    oon_rate_threshold: float = Field(
        default=0.80, description="OON rate above this triggers DME cluster flag"
    )
    billing_ratio_multiplier: float = Field(
        default=3.0, description="Billed/allowed ratio > this × peer median triggers flag"
    )
    rebound_window_weeks: int = Field(
        default=12, description="Weeks after policy change to check for rebound"
    )
    rebound_threshold_pct: float = Field(
        default=0.80,
        description="If post-change util returns to this fraction of pre-change, flag as rebound",
    )


class PipelineConfig(BaseModel):
    """Top-level configuration for the entire UM claims analytics pipeline."""

    seed: int = Field(default=42, description="Random seed for deterministic generation")
    num_claims: int = Field(default=100_000, description="Number of synthetic claims to generate")
    output_dir: Path = Field(default=Path("output"), description="Root output directory")
    date_start: date = Field(default=date(2023, 1, 1), description="Earliest service date")
    date_end: date = Field(default=date(2025, 12, 31), description="Latest service date")
    cost_per_appeal: float = Field(
        default=350.0, description="Estimated admin cost per appeal (USD)"
    )

    # Sub-configs
    detection: DetectionConfig = Field(default_factory=DetectionConfig)
    policy_events: list[PolicyChangeEvent] = Field(
        default_factory=lambda: [
            PolicyChangeEvent(
                policy_id="POL-001",
                affected_procedure_prefixes=["CPT-7"],
                change_type="removed",
                effective_date=date(2024, 7, 1),
                description="Removed prior auth requirement for imaging services",
            )
        ]
    )
    benchmarks: list[BenchmarkBaseline] = Field(
        default_factory=lambda: [
            BenchmarkBaseline(metric_name="denial_rate", baseline_value=0.08, threshold_pct=0.15),
            BenchmarkBaseline(
                metric_name="oon_rate", baseline_value=0.05, threshold_pct=0.20
            ),
            BenchmarkBaseline(
                metric_name="cost_per_claim", baseline_value=1200.0, threshold_pct=0.10
            ),
        ]
    )

    # Fraud cluster injection parameters
    fraud_cluster_supplier_count: int = Field(
        default=5, description="Number of suspicious DME suppliers to inject"
    )
    fraud_cluster_claims_per_supplier: int = Field(
        default=150, description="Claims per suspicious supplier"
    )

    # Appeal propensity by denial reason
    appeal_propensity: dict[str, float] = Field(
        default_factory=lambda: {
            "medical_necessity": 0.40,
            "not_covered": 0.15,
            "authorization_missing": 0.30,
            "coding_error": 0.10,
            "duplicate": 0.05,
            "untimely_filing": 0.02,
        }
    )


# Service category mapping from procedure code prefixes
SERVICE_CATEGORIES: dict[str, str] = {
    "HCPCS-E": "DME",
    "HCPCS-K": "DME",
    "CPT-7": "Imaging",
    "CPT-99": "E&M",
    "CPT-2": "Surgical",
    "CPT-3": "Surgical",
    "CPT-4": "Surgical",
    "CPT-5": "Surgical",
    "CPT-6": "Surgical",
}


def get_service_category(procedure_code: str) -> str:
    """Map a procedure code to its service category."""
    for prefix, category in SERVICE_CATEGORIES.items():
        if procedure_code.startswith(prefix):
            return category
    return "Other"
