"""Tests for validation module."""

import polars as pl
import pytest

from um_claims.validate import validate_claims


class TestValidation:
    def test_valid_data_passes(self, small_claims_df: pl.DataFrame) -> None:
        """Generated data should pass validation."""
        result = validate_claims(small_claims_df)
        assert result.passed, f"Validation failed: {[i.message for i in result.critical_issues]}"

    def test_missing_column_fails(self, small_claims_df: pl.DataFrame) -> None:
        """Dropping a required column should fail validation."""
        bad_df = small_claims_df.drop("claim_id")
        result = validate_claims(bad_df)
        assert not result.passed
        assert any("claim_id" in i.message for i in result.critical_issues)

    def test_null_required_column_fails(self, minimal_claims_df: pl.DataFrame) -> None:
        """Null values in required columns should fail."""
        bad_df = minimal_claims_df.with_columns(pl.lit(None).alias("member_id").cast(pl.Utf8))
        result = validate_claims(bad_df)
        assert not result.passed
        assert any("member_id" in i.message for i in result.critical_issues)

    def test_negative_amount_fails(self, minimal_claims_df: pl.DataFrame) -> None:
        """Negative billed_amount should fail."""
        bad_df = minimal_claims_df.with_columns(pl.lit(-100.0).alias("billed_amount"))
        result = validate_claims(bad_df)
        assert not result.passed
        assert any("billed_amount" in i.message for i in result.critical_issues)

    def test_invalid_enum_fails(self, minimal_claims_df: pl.DataFrame) -> None:
        """Invalid enum value should fail."""
        bad_df = minimal_claims_df.with_columns(pl.lit("INVALID").alias("network_status"))
        result = validate_claims(bad_df)
        assert not result.passed
        assert any("network_status" in i.message for i in result.critical_issues)

    def test_denial_without_reason_fails(self, minimal_claims_df: pl.DataFrame) -> None:
        """denial_flag=Y with null denial_reason_category should fail."""
        bad_df = minimal_claims_df.with_columns(
            pl.lit("Y").alias("denial_flag"),
            pl.lit(None).alias("denial_reason_category").cast(pl.Utf8),
        )
        result = validate_claims(bad_df)
        assert not result.passed
        assert any("denial_reason" in i.message for i in result.critical_issues)

    def test_bad_date_ordering_fails(self, minimal_claims_df: pl.DataFrame) -> None:
        """claim_received_date before service_date should fail."""
        from datetime import date

        bad_df = minimal_claims_df.with_columns(
            pl.lit(date(2020, 1, 1)).alias("claim_received_date")
        )
        result = validate_claims(bad_df)
        assert not result.passed
        assert any("date_ordering" in i.rule for i in result.critical_issues)

    def test_advisory_zero_variance(self) -> None:
        """Zero-variance amounts should produce advisory warning."""
        df = pl.DataFrame(
            {
                "claim_id": ["C1", "C2"],
                "member_id": ["M1", "M2"],
                "provider_id": ["P1", "P2"],
                "facility_id": [None, None],
                "payer_product": ["Commercial", "Commercial"],
                "plan_type": ["PPO", "PPO"],
                "line_of_business": ["Group", "Group"],
                "service_date": [
                    __import__("datetime").date(2024, 1, 1),
                    __import__("datetime").date(2024, 1, 2),
                ],
                "claim_received_date": [
                    __import__("datetime").date(2024, 1, 2),
                    __import__("datetime").date(2024, 1, 3),
                ],
                "paid_date": [None, None],
                "claim_type": ["Professional", "Professional"],
                "place_of_service": ["11", "11"],
                "diagnosis_codes": ['["DX-1001"]', '["DX-1002"]'],
                "procedure_code": ["CPT-99201", "CPT-99201"],
                "revenue_code": [None, None],
                "billed_amount": [100.0, 100.0],  # Zero variance
                "allowed_amount": [80.0, 80.0],    # Zero variance
                "paid_amount": [70.0, 70.0],        # Zero variance
                "units": [1, 1],
                "network_status": ["INN", "INN"],
                "authorization_required": ["N", "N"],
                "authorization_id": [None, None],
                "denial_flag": ["N", "N"],
                "denial_reason_category": [None, None],
                "appeal_flag": ["N", "N"],
                "grievance_flag": ["N", "N"],
                "dme_flag": ["N", "N"],
                "supplier_type": [None, None],
                "rendering_npi": ["1234567890", "1234567891"],
                "billing_npi": ["1234567890", "1234567891"],
                "geography_state": ["PA", "PA"],
                "geography_region": ["Northeast", "Northeast"],
                "specialty": ["Internal Medicine", "Internal Medicine"],
            }
        )
        result = validate_claims(df)
        assert result.passed  # Advisory doesn't fail
        assert any("zero_variance" in i.rule for i in result.advisory_issues)
