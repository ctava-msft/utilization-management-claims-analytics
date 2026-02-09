"""Tests for appeals analytics module."""

import polars as pl

from um_claims.appeals import analyze_appeals


class TestAppealsAnalytics:
    def test_funnel_metrics(self, minimal_claims_df: pl.DataFrame) -> None:
        """Basic funnel metrics should be computed."""
        report = analyze_appeals(minimal_claims_df)
        assert report.total_claims == 5
        assert report.total_denials == 1  # C003
        assert report.total_appeals == 1  # C003
        assert report.overall_denial_rate > 0

    def test_admin_cost_calculation(self, minimal_claims_df: pl.DataFrame) -> None:
        """Admin cost should be appeals * cost_per_appeal."""
        cost = 500.0
        report = analyze_appeals(minimal_claims_df, cost_per_appeal=cost)
        assert report.estimated_admin_cost == report.total_appeals * cost

    def test_category_breakdown(self, small_claims_df: pl.DataFrame) -> None:
        """Categories should be populated for data with denials."""
        report = analyze_appeals(small_claims_df)
        if report.total_denials > 0:
            assert len(report.categories) > 0

    def test_no_denials(self) -> None:
        """No denials → zero rates, no crash."""
        df = pl.DataFrame({
            "claim_id": ["C1", "C2"],
            "provider_id": ["P1", "P2"],
            "denial_flag": ["N", "N"],
            "appeal_flag": ["N", "N"],
            "grievance_flag": ["N", "N"],
            "denial_reason_category": [None, None],
            "billed_amount": [100.0, 200.0],
            "allowed_amount": [80.0, 160.0],
        })
        report = analyze_appeals(df)
        assert report.total_denials == 0
        assert report.overall_appeal_rate == 0.0
        assert len(report.categories) == 0
