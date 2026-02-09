"""Outlier and anomaly detection module.

Implements rule-based detection that produces explainable Flag objects.
Every flag carries rule_name, entity_id, feature_values, threshold, actual_value,
severity, and a human-readable description.

Rules:
1. high_volume_provider — claim volume > μ + 2σ
2. high_cost_provider — total allowed > μ + 2σ
3. new_entity_high_volume — entity age < 90 days AND volume > 90th percentile
4. oon_dme_cluster — DME supplier, OON rate > 80%, high volume, concentrated codes
5. billing_ratio_outlier — billed/allowed ratio > 3× peer median

Spec: SR-4
"""

from __future__ import annotations

from pydantic import BaseModel, Field
import polars as pl

from um_claims.config import DetectionConfig


class Flag(BaseModel):
    """An explainable detection flag.

    Every flag must carry all fields needed for a UM stakeholder to understand
    *why* it fired without looking at code.
    """

    rule_name: str = Field(description="Identifier for the detection rule")
    entity_type: str = Field(description="'provider' | 'supplier' | 'service'")
    entity_id: str = Field(description="The flagged entity's identifier")
    severity: str = Field(description="'high' | 'medium' | 'low'")
    feature_values: dict[str, float | int | str] = Field(
        description="Actual feature values used in detection"
    )
    threshold: float = Field(description="Threshold that was exceeded")
    actual_value: float = Field(description="The actual value that triggered the flag")
    description: str = Field(description="Human-readable explanation")


def detect_high_volume_providers(
    provider_features: pl.DataFrame,
    config: DetectionConfig,
) -> list[Flag]:
    """Flag providers with claim volume > μ + z*σ."""
    vol = provider_features["total_claims"]
    mean = vol.mean()
    std = vol.std()
    if mean is None or std is None or std == 0:
        return []

    threshold = mean + config.zscore_threshold * std
    flagged = provider_features.filter(pl.col("total_claims") > threshold)

    flags: list[Flag] = []
    for row in flagged.iter_rows(named=True):
        zscore = (row["total_claims"] - mean) / std
        flags.append(
            Flag(
                rule_name="high_volume_provider",
                entity_type="provider",
                entity_id=row["provider_id"],
                severity="high" if zscore > 3 else "medium",
                feature_values={
                    "total_claims": row["total_claims"],
                    "peer_mean": round(mean, 1),
                    "peer_std": round(std, 1),
                    "z_score": round(zscore, 2),
                },
                threshold=round(threshold, 1),
                actual_value=float(row["total_claims"]),
                description=(
                    f"Provider {row['provider_id']} has {row['total_claims']} claims "
                    f"(z-score={zscore:.2f}, threshold={threshold:.0f})"
                ),
            )
        )
    return flags


def detect_high_cost_providers(
    provider_features: pl.DataFrame,
    config: DetectionConfig,
) -> list[Flag]:
    """Flag providers with total allowed amount > μ + z*σ."""
    cost = provider_features["total_allowed"]
    mean = cost.mean()
    std = cost.std()
    if mean is None or std is None or std == 0:
        return []

    threshold = mean + config.zscore_threshold * std
    flagged = provider_features.filter(pl.col("total_allowed") > threshold)

    flags: list[Flag] = []
    for row in flagged.iter_rows(named=True):
        zscore = (row["total_allowed"] - mean) / std
        flags.append(
            Flag(
                rule_name="high_cost_provider",
                entity_type="provider",
                entity_id=row["provider_id"],
                severity="high" if zscore > 3 else "medium",
                feature_values={
                    "total_allowed": round(row["total_allowed"], 2),
                    "peer_mean": round(mean, 2),
                    "peer_std": round(std, 2),
                    "z_score": round(zscore, 2),
                },
                threshold=round(threshold, 2),
                actual_value=round(row["total_allowed"], 2),
                description=(
                    f"Provider {row['provider_id']} has ${row['total_allowed']:,.2f} total allowed "
                    f"(z-score={zscore:.2f}, threshold=${threshold:,.0f})"
                ),
            )
        )
    return flags


def detect_new_entity_high_volume(
    provider_features: pl.DataFrame,
    config: DetectionConfig,
) -> list[Flag]:
    """Flag new entities (< 90 days old) with volume > 90th percentile of established."""
    # Split into new vs established
    new_mask = pl.col("entity_age_days") < config.new_entity_days
    established = provider_features.filter(~new_mask)
    new_entities = provider_features.filter(new_mask)

    if established.height == 0 or new_entities.height == 0:
        return []

    volume_threshold = established["total_claims"].quantile(config.new_entity_volume_percentile)
    if volume_threshold is None:
        return []

    flagged = new_entities.filter(pl.col("total_claims") > volume_threshold)
    flags: list[Flag] = []
    for row in flagged.iter_rows(named=True):
        flags.append(
            Flag(
                rule_name="new_entity_high_volume",
                entity_type="provider",
                entity_id=row["provider_id"],
                severity="high",
                feature_values={
                    "total_claims": row["total_claims"],
                    "entity_age_days": row["entity_age_days"],
                    "specialty": row.get("specialty", "unknown"),
                },
                threshold=float(volume_threshold),
                actual_value=float(row["total_claims"]),
                description=(
                    f"New provider {row['provider_id']} (age={row['entity_age_days']}d) "
                    f"has {row['total_claims']} claims, exceeding the 90th percentile "
                    f"of established providers ({volume_threshold:.0f})"
                ),
            )
        )
    return flags


def detect_oon_dme_clusters(
    provider_features: pl.DataFrame,
    config: DetectionConfig,
) -> list[Flag]:
    """Flag DME suppliers with high OON rate AND high volume AND concentrated codes.

    A supplier is suspicious if:
    - dme_rate > 0.5 (primarily DME)
    - oon_rate > config.oon_rate_threshold (80%+)
    - unique_procedure_codes <= 3 (concentrated)
    - total_claims >= median of DME providers
    """
    dme_providers = provider_features.filter(pl.col("dme_rate") > 0.5)
    if dme_providers.height == 0:
        return []

    volume_median = dme_providers["total_claims"].median()
    if volume_median is None:
        return []

    flagged = dme_providers.filter(
        (pl.col("oon_rate") > config.oon_rate_threshold)
        & (pl.col("unique_procedure_codes") <= 3)
        & (pl.col("total_claims") >= volume_median)
    )

    flags: list[Flag] = []
    for row in flagged.iter_rows(named=True):
        flags.append(
            Flag(
                rule_name="oon_dme_cluster",
                entity_type="supplier",
                entity_id=row["provider_id"],
                severity="high",
                feature_values={
                    "oon_rate": round(row["oon_rate"], 3),
                    "dme_rate": round(row["dme_rate"], 3),
                    "unique_procedure_codes": row["unique_procedure_codes"],
                    "total_claims": row["total_claims"],
                    "total_allowed": round(row["total_allowed"], 2),
                    "entity_age_days": row["entity_age_days"],
                    "geography_state": row.get("geography_state", "unknown"),
                },
                threshold=config.oon_rate_threshold,
                actual_value=round(row["oon_rate"], 3),
                description=(
                    f"DME supplier {row['provider_id']} has {row['oon_rate']:.0%} OON rate, "
                    f"{row['total_claims']} claims, only {row['unique_procedure_codes']} unique codes. "
                    f"Possible OON DME billing scheme."
                ),
            )
        )
    return flags


def detect_billing_ratio_outliers(
    provider_features: pl.DataFrame,
    config: DetectionConfig,
) -> list[Flag]:
    """Flag providers with billed/allowed ratio > multiplier × peer median."""
    ratio_col = "avg_billed_to_allowed_ratio"
    if ratio_col not in provider_features.columns:
        return []

    valid = provider_features.filter(pl.col(ratio_col).is_not_null() & pl.col(ratio_col).is_finite())
    if valid.height == 0:
        return []

    peer_median = valid[ratio_col].median()
    if peer_median is None or peer_median == 0:
        return []

    threshold = peer_median * config.billing_ratio_multiplier
    flagged = valid.filter(pl.col(ratio_col) > threshold)

    flags: list[Flag] = []
    for row in flagged.iter_rows(named=True):
        flags.append(
            Flag(
                rule_name="billing_ratio_outlier",
                entity_type="provider",
                entity_id=row["provider_id"],
                severity="medium",
                feature_values={
                    "billed_to_allowed_ratio": round(row[ratio_col], 3),
                    "peer_median_ratio": round(peer_median, 3),
                    "total_claims": row["total_claims"],
                },
                threshold=round(threshold, 3),
                actual_value=round(row[ratio_col], 3),
                description=(
                    f"Provider {row['provider_id']} has a billed/allowed ratio of "
                    f"{row[ratio_col]:.2f} vs peer median of {peer_median:.2f} "
                    f"(threshold={threshold:.2f})"
                ),
            )
        )
    return flags


def run_all_detection_rules(
    provider_features: pl.DataFrame,
    config: DetectionConfig,
) -> list[Flag]:
    """Execute all detection rules and return combined flags.

    Args:
        provider_features: Provider-level feature DataFrame from features.py.
        config: Detection configuration with thresholds.

    Returns:
        List of all Flag objects, sorted by severity (high first).
    """
    all_flags: list[Flag] = []

    all_flags.extend(detect_high_volume_providers(provider_features, config))
    all_flags.extend(detect_high_cost_providers(provider_features, config))
    all_flags.extend(detect_new_entity_high_volume(provider_features, config))
    all_flags.extend(detect_oon_dme_clusters(provider_features, config))
    all_flags.extend(detect_billing_ratio_outliers(provider_features, config))

    # Sort by severity
    severity_order = {"high": 0, "medium": 1, "low": 2}
    all_flags.sort(key=lambda f: severity_order.get(f.severity, 3))

    return all_flags
