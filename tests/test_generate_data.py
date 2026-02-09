"""Tests for synthetic data generation."""

import polars as pl

from um_claims.config import PipelineConfig
from um_claims.generate_data import generate_claims
from um_claims.schema import EXPECTED_COLUMNS


class TestGenerateClaims:
    def test_deterministic_output(self) -> None:
        """Same seed produces identical DataFrames."""
        config = PipelineConfig(seed=42, num_claims=200)
        df1 = generate_claims(config)
        df2 = generate_claims(config)
        assert df1.equals(df2)

    def test_output_shape(self) -> None:
        """Generated data has expected columns."""
        config = PipelineConfig(seed=42, num_claims=200)
        df = generate_claims(config)
        for col in EXPECTED_COLUMNS:
            assert col in df.columns, f"Missing column: {col}"

    def test_claim_count_includes_fraud(self) -> None:
        """Claim count > num_claims because fraud cluster is appended."""
        config = PipelineConfig(seed=42, num_claims=500)
        df = generate_claims(config)
        fraud_count = config.fraud_cluster_supplier_count * config.fraud_cluster_claims_per_supplier
        assert len(df) == config.num_claims + fraud_count

    def test_long_tail_distribution(self) -> None:
        """Top 20% of claims should account for >= 60% of billed (conservative threshold)."""
        config = PipelineConfig(seed=42, num_claims=5000)
        df = generate_claims(config)
        sorted_billed = df.sort("billed_amount", descending=True)["billed_amount"]
        total = sorted_billed.sum()
        top_20_pct = int(len(df) * 0.2)
        top_20_sum = sorted_billed.head(top_20_pct).sum()
        assert top_20_sum / total >= 0.60, f"Top 20% only accounts for {top_20_sum / total:.1%}"

    def test_fraud_cluster_presence(self) -> None:
        """Fraud cluster suppliers should be identifiable."""
        config = PipelineConfig(seed=42, num_claims=500)
        df = generate_claims(config)
        fraud_claims = df.filter(pl.col("provider_id").str.starts_with("FRAUD-PROV"))
        assert fraud_claims.height > 0
        # Should be mostly OON
        oon_rate = fraud_claims.filter(pl.col("network_status") == "OON").height / fraud_claims.height
        assert oon_rate > 0.85

    def test_denial_appeal_dynamics(self) -> None:
        """Some denied claims should have appeals."""
        config = PipelineConfig(seed=42, num_claims=2000)
        df = generate_claims(config)
        denied = df.filter(pl.col("denial_flag") == "Y")
        appealed = denied.filter(pl.col("appeal_flag") == "Y")
        assert denied.height > 0
        assert appealed.height > 0
        assert appealed.height < denied.height  # Not every denial is appealed

    def test_no_negative_amounts(self) -> None:
        """All amounts should be non-negative."""
        config = PipelineConfig(seed=42, num_claims=1000)
        df = generate_claims(config)
        for col in ["billed_amount", "allowed_amount", "paid_amount"]:
            assert df.filter(pl.col(col) < 0).height == 0

    def test_date_ordering(self) -> None:
        """claim_received_date should be >= service_date."""
        config = PipelineConfig(seed=42, num_claims=1000)
        df = generate_claims(config)
        bad = df.filter(pl.col("claim_received_date") < pl.col("service_date"))
        assert bad.height == 0
