"""Shared test fixtures for UM Claims Analytics tests."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from um_claims.config import PipelineConfig


@pytest.fixture
def config() -> PipelineConfig:
    """Default test configuration with small dataset."""
    return PipelineConfig(seed=42, num_claims=500)


@pytest.fixture
def small_claims_df(config: PipelineConfig) -> pl.DataFrame:
    """Generate a small synthetic claims DataFrame for testing."""
    from um_claims.generate_data import generate_claims

    return generate_claims(config)


@pytest.fixture
def minimal_claims_df() -> pl.DataFrame:
    """A minimal hand-crafted claims DataFrame for precise testing."""
    return pl.DataFrame(
        {
            "claim_id": ["C001", "C002", "C003", "C004", "C005"],
            "member_id": ["M001", "M001", "M002", "M003", "M003"],
            "provider_id": ["P001", "P001", "P002", "P003", "P003"],
            "facility_id": ["F001", None, "F002", None, None],
            "payer_product": ["Commercial", "Commercial", "Medicare", "Medicaid", "Commercial"],
            "plan_type": ["PPO", "PPO", "HMO", "HMO", "EPO"],
            "line_of_business": ["Group", "Group", "Medicare", "Medicaid", "Individual"],
            "service_date": [
                date(2024, 1, 15),
                date(2024, 2, 20),
                date(2024, 3, 10),
                date(2024, 4, 5),
                date(2024, 5, 1),
            ],
            "claim_received_date": [
                date(2024, 1, 20),
                date(2024, 2, 25),
                date(2024, 3, 15),
                date(2024, 4, 10),
                date(2024, 5, 5),
            ],
            "paid_date": [
                date(2024, 2, 15),
                date(2024, 3, 20),
                None,
                date(2024, 5, 1),
                date(2024, 6, 1),
            ],
            "claim_type": [
                "Professional",
                "Professional",
                "Institutional",
                "Professional",
                "Professional",
            ],
            "place_of_service": ["11", "11", "21", "12", "11"],
            "diagnosis_codes": [
                '["DX-1001"]',
                '["DX-1002", "DX-1003"]',
                '["DX-2001"]',
                '["DX-3001"]',
                '["DX-3002"]',
            ],
            "procedure_code": [
                "CPT-99201",
                "CPT-70100",
                "CPT-27100",
                "HCPCS-E0100",
                "CPT-99202",
            ],
            "revenue_code": [None, None, "0301", None, None],
            "billed_amount": [150.0, 500.0, 2000.0, 300.0, 175.0],
            "allowed_amount": [120.0, 400.0, 1500.0, 250.0, 140.0],
            "paid_amount": [100.0, 350.0, 1200.0, 200.0, 120.0],
            "units": [1, 1, 1, 3, 1],
            "network_status": ["INN", "INN", "INN", "OON", "INN"],
            "authorization_required": ["N", "Y", "Y", "N", "N"],
            "authorization_id": [None, "AUTH-001", "AUTH-002", None, None],
            "denial_flag": ["N", "N", "Y", "N", "N"],
            "denial_reason_category": [None, None, "medical_necessity", None, None],
            "appeal_flag": ["N", "N", "Y", "N", "N"],
            "grievance_flag": ["N", "N", "N", "N", "N"],
            "dme_flag": ["N", "N", "N", "Y", "N"],
            "supplier_type": [None, None, None, "DME Supplier", None],
            "rendering_npi": [
                "1234567890",
                "1234567890",
                "2345678901",
                "3456789012",
                "3456789012",
            ],
            "billing_npi": [
                "1234567890",
                "1234567890",
                "2345678901",
                "3456789012",
                "3456789012",
            ],
            "geography_state": ["PA", "PA", "OH", "FL", "PA"],
            "geography_region": [
                "Northeast",
                "Northeast",
                "Midwest",
                "Southeast",
                "Northeast",
            ],
            "specialty": [
                "Internal Medicine",
                "Radiology",
                "Orthopedics",
                "DME Supplier",
                "Internal Medicine",
            ],
        }
    )
