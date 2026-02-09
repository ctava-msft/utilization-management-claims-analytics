"""Tests for schema module."""

from um_claims.schema import (
    ClaimRecord,
    EXPECTED_COLUMNS,
    REQUIRED_COLUMNS,
    PAYER_PRODUCTS,
    PROCEDURE_CODES,
)
from datetime import date


class TestClaimRecord:
    def test_valid_record(self) -> None:
        record = ClaimRecord(
            claim_id="C001",
            member_id="M001",
            provider_id="P001",
            payer_product="Commercial",
            plan_type="PPO",
            line_of_business="Group",
            service_date=date(2024, 1, 1),
            claim_received_date=date(2024, 1, 5),
            claim_type="Professional",
            place_of_service="11",
            diagnosis_codes=["DX-1001"],
            procedure_code="CPT-99201",
            billed_amount=100.0,
            allowed_amount=80.0,
            paid_amount=70.0,
            units=1,
            network_status="INN",
            authorization_required="N",
            denial_flag="N",
            appeal_flag="N",
            grievance_flag="N",
            dme_flag="N",
            rendering_npi="1234567890",
            billing_npi="1234567890",
            geography_state="PA",
            geography_region="Northeast",
            specialty="Internal Medicine",
        )
        assert record.claim_id == "C001"

    def test_units_must_be_positive(self) -> None:
        import pytest

        with pytest.raises(Exception):
            ClaimRecord(
                claim_id="C001",
                member_id="M001",
                provider_id="P001",
                payer_product="Commercial",
                plan_type="PPO",
                line_of_business="Group",
                service_date=date(2024, 1, 1),
                claim_received_date=date(2024, 1, 5),
                claim_type="Professional",
                place_of_service="11",
                diagnosis_codes=["DX-1001"],
                procedure_code="CPT-99201",
                billed_amount=100.0,
                allowed_amount=80.0,
                paid_amount=70.0,
                units=0,  # Invalid
                network_status="INN",
                authorization_required="N",
                denial_flag="N",
                appeal_flag="N",
                grievance_flag="N",
                dme_flag="N",
                rendering_npi="1234567890",
                billing_npi="1234567890",
                geography_state="PA",
                geography_region="Northeast",
                specialty="Internal Medicine",
            )


class TestSchemaConstants:
    def test_expected_columns_complete(self) -> None:
        assert len(EXPECTED_COLUMNS) >= 30

    def test_required_columns_subset(self) -> None:
        for col in REQUIRED_COLUMNS:
            assert col in EXPECTED_COLUMNS

    def test_payer_products(self) -> None:
        assert "Commercial" in PAYER_PRODUCTS
        assert "Medicare" in PAYER_PRODUCTS

    def test_procedure_code_pools(self) -> None:
        assert "E&M" in PROCEDURE_CODES
        assert "DME" in PROCEDURE_CODES
        assert len(PROCEDURE_CODES["E&M"]) > 0
