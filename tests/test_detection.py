"""Tests for detection module."""

import polars as pl

from um_claims.config import DetectionConfig, PipelineConfig
from um_claims.detection import (
    Flag,
    detect_billing_ratio_outliers,
    detect_high_cost_providers,
    detect_high_volume_providers,
    detect_new_entity_high_volume,
    detect_oon_dme_clusters,
    run_all_detection_rules,
)
from um_claims.features import compute_all_features
from um_claims.generate_data import generate_claims


class TestFlagModel:
    def test_flag_serialization(self) -> None:
        flag = Flag(
            rule_name="test_rule",
            entity_type="provider",
            entity_id="P001",
            severity="high",
            feature_values={"total_claims": 100},
            threshold=50.0,
            actual_value=100.0,
            description="Test flag",
        )
        data = flag.model_dump()
        assert data["rule_name"] == "test_rule"
        restored = Flag(**data)
        assert restored.entity_id == "P001"


class TestDetectionOnSyntheticData:
    def test_fraud_cluster_detected(self) -> None:
        """Planted fraud cluster should be detected by oon_dme_cluster rule."""
        config = PipelineConfig(seed=42, num_claims=2000)
        df = generate_claims(config)
        features = compute_all_features(df)

        # Verify fraud suppliers exist in the provider features and have high OON/DME rates
        fraud_providers = features["provider"].filter(
            pl.col("provider_id").str.starts_with("FRAUD")
        )
        assert fraud_providers.height > 0, "No fraud providers in features"

        # Use relaxed thresholds to ensure fraud cluster is caught
        from um_claims.config import DetectionConfig
        relaxed = DetectionConfig(oon_rate_threshold=0.70)
        flags = detect_oon_dme_clusters(features["provider"], relaxed)
        # At least some fraud suppliers should be flagged
        fraud_ids = [f.entity_id for f in flags if f.entity_id.startswith("FRAUD")]
        assert len(fraud_ids) > 0, "No fraud cluster suppliers detected"

    def test_all_flags_have_required_fields(self) -> None:
        """Every flag must have all required fields populated."""
        config = PipelineConfig(seed=42, num_claims=1000)
        df = generate_claims(config)
        features = compute_all_features(df)
        flags = run_all_detection_rules(features["provider"], config.detection)
        for flag in flags:
            assert flag.rule_name
            assert flag.entity_type in ("provider", "supplier", "service")
            assert flag.entity_id
            assert flag.severity in ("high", "medium", "low")
            assert isinstance(flag.feature_values, dict)
            assert flag.description

    def test_flags_sorted_by_severity(self) -> None:
        """Flags should be sorted: high first, then medium, then low."""
        config = PipelineConfig(seed=42, num_claims=2000)
        df = generate_claims(config)
        features = compute_all_features(df)
        flags = run_all_detection_rules(features["provider"], config.detection)
        if len(flags) > 1:
            severity_order = {"high": 0, "medium": 1, "low": 2}
            for i in range(len(flags) - 1):
                assert severity_order[flags[i].severity] <= severity_order[flags[i + 1].severity]


class TestHighVolumeDetection:
    def test_outlier_detected(self) -> None:
        """A provider with extreme volume should be detected."""
        pf = pl.DataFrame({
            "provider_id": ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10"],
            "total_claims": [10, 12, 11, 9, 500, 10, 11, 12, 9, 10],  # P5 is an outlier
            "total_allowed": [1000.0, 1200.0, 1100.0, 900.0, 50000.0, 1000.0, 1100.0, 1200.0, 900.0, 1000.0],
            "total_billed": [1500.0, 1800.0, 1650.0, 1350.0, 75000.0, 1500.0, 1650.0, 1800.0, 1350.0, 1500.0],
            "avg_allowed": [100.0] * 10,
            "avg_units": [1.0] * 10,
            "total_units": [10, 12, 11, 9, 500, 10, 11, 12, 9, 10],
            "oon_rate": [0.1] * 10,
            "denial_rate": [0.1] * 10,
            "appeal_rate": [0.05] * 10,
            "dme_rate": [0.0] * 10,
            "first_claim_date": [None] * 10,
            "last_claim_date": [None] * 10,
            "unique_members": [5, 5, 5, 5, 200, 5, 5, 5, 5, 5],
            "unique_procedure_codes": [3, 3, 3, 3, 50, 3, 3, 3, 3, 3],
            "specialty": ["IM"] * 10,
            "geography_state": ["PA"] * 10,
            "geography_region": ["Northeast"] * 10,
            "entity_age_days": [365] * 10,
            "avg_billed_to_allowed_ratio": [1.5] * 10,
            "cost_per_unit": [100.0] * 10,
        })
        flags = detect_high_volume_providers(pf, DetectionConfig())
        assert any(f.entity_id == "P5" for f in flags)

    def test_no_outlier_when_uniform(self) -> None:
        """No flags when all providers are similar."""
        pf = pl.DataFrame({
            "provider_id": ["P1", "P2", "P3"],
            "total_claims": [10, 11, 10],
            "total_allowed": [1000.0, 1100.0, 1000.0],
            "total_billed": [1500.0, 1650.0, 1500.0],
            "avg_allowed": [100.0, 100.0, 100.0],
            "avg_units": [1.0, 1.0, 1.0],
            "total_units": [10, 11, 10],
            "oon_rate": [0.1, 0.1, 0.1],
            "denial_rate": [0.1, 0.1, 0.1],
            "appeal_rate": [0.05, 0.05, 0.05],
            "dme_rate": [0.0, 0.0, 0.0],
            "first_claim_date": [None] * 3,
            "last_claim_date": [None] * 3,
            "unique_members": [5, 5, 5],
            "unique_procedure_codes": [3, 3, 3],
            "specialty": ["IM"] * 3,
            "geography_state": ["PA"] * 3,
            "geography_region": ["Northeast"] * 3,
            "entity_age_days": [365, 365, 365],
            "avg_billed_to_allowed_ratio": [1.5, 1.5, 1.5],
            "cost_per_unit": [100.0, 100.0, 100.0],
        })
        flags = detect_high_volume_providers(pf, DetectionConfig())
        assert len(flags) == 0
