"""Tests for policy simulation module."""

from datetime import date

import polars as pl

from um_claims.config import DetectionConfig, PolicyChangeEvent
from um_claims.policy_sim import analyze_policy_impact


class TestPolicyImpact:
    def test_pre_post_comparison(self) -> None:
        """Policy change should show pre/post metrics."""
        event = PolicyChangeEvent(
            policy_id="POL-TEST",
            affected_procedure_prefixes=["CPT-7"],
            change_type="removed",
            effective_date=date(2024, 7, 1),
        )
        df = pl.DataFrame({
            "claim_id": [f"C{i}" for i in range(100)],
            "provider_id": ["P1"] * 100,
            "service_date": [
                date(2024, 5, 1) if i < 50 else date(2024, 8, 1) for i in range(100)
            ],
            "procedure_code": ["CPT-70100"] * 100,
            "allowed_amount": [500.0] * 100,
            "billed_amount": [700.0] * 100,
            "denial_flag": ["N"] * 100,
            "network_status": ["INN"] * 100,
        })
        config = DetectionConfig(rebound_window_weeks=12)
        report = analyze_policy_impact(df, [event], config)
        assert len(report.impacts) == 1
        impact = report.impacts[0]
        assert impact.pre_metrics.volume == 50
        assert impact.post_metrics.volume == 50

    def test_rebound_detection(self) -> None:
        """When post volume >= 80% of pre, rebound should be detected."""
        event = PolicyChangeEvent(
            policy_id="POL-REBOUND",
            affected_procedure_prefixes=["CPT-99"],
            change_type="removed",
            effective_date=date(2024, 7, 1),
        )
        df = pl.DataFrame({
            "claim_id": [f"C{i}" for i in range(100)],
            "provider_id": ["P1"] * 100,
            "service_date": [
                date(2024, 5, 1) if i < 50 else date(2024, 8, 1) for i in range(100)
            ],
            "procedure_code": ["CPT-99201"] * 100,
            "allowed_amount": [200.0] * 100,
            "billed_amount": [300.0] * 100,
            "denial_flag": ["N"] * 100,
            "network_status": ["INN"] * 100,
        })
        config = DetectionConfig(rebound_threshold_pct=0.80)
        report = analyze_policy_impact(df, [event], config)
        assert report.impacts[0].rebound_detected

    def test_empty_affected_services(self) -> None:
        """No claims match → empty metrics, no crash."""
        event = PolicyChangeEvent(
            policy_id="POL-EMPTY",
            affected_procedure_prefixes=["NONEXISTENT"],
            change_type="removed",
            effective_date=date(2024, 7, 1),
        )
        df = pl.DataFrame({
            "claim_id": ["C1"],
            "provider_id": ["P1"],
            "service_date": [date(2024, 5, 1)],
            "procedure_code": ["CPT-99201"],
            "allowed_amount": [200.0],
            "billed_amount": [300.0],
            "denial_flag": ["N"],
            "network_status": ["INN"],
        })
        config = DetectionConfig()
        report = analyze_policy_impact(df, [event], config)
        assert report.impacts[0].pre_metrics.volume == 0
