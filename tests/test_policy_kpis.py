"""Tests for the analytics.policy_kpis module."""

from __future__ import annotations

import json

import pytest  # noqa: TC002

from um_claims.analytics.policy_kpis import compute_policy_kpis

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def matched_claims() -> list[dict]:
    return [
        {
            "claim_id": "C001",
            "policy_id": "POL-A",
            "match_confidence": 1.0,
            "allowed_amount": 100.0,
            "denial_flag": "N",
            "diagnosis_codes": json.dumps(["DX-001", "DX-002"]),
            "specialty": "Internal Medicine",
        },
        {
            "claim_id": "C002",
            "policy_id": "POL-A",
            "match_confidence": 0.8,
            "allowed_amount": 200.0,
            "denial_flag": "Y",
            "diagnosis_codes": json.dumps(["DX-001"]),
            "specialty": "Internal Medicine",
        },
        {
            "claim_id": "C003",
            "policy_id": "POL-B",
            "match_confidence": 0.6,
            "allowed_amount": 500.0,
            "denial_flag": "N",
            "diagnosis_codes": json.dumps(["DX-003"]),
            "specialty": "Radiology",
        },
        {
            "claim_id": "C004",
            "policy_id": "unmatched",
            "match_confidence": 0.0,
            "allowed_amount": 50.0,
            "denial_flag": "N",
            "diagnosis_codes": json.dumps(["DX-004"]),
            "specialty": "Pediatrics",
        },
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestComputePolicyKpis:
    def test_returns_list(self, matched_claims: list[dict]) -> None:
        kpis = compute_policy_kpis(matched_claims)
        assert isinstance(kpis, list)

    def test_one_entry_per_policy(self, matched_claims: list[dict]) -> None:
        kpis = compute_policy_kpis(matched_claims)
        policy_ids = {k["policy_id"] for k in kpis}
        assert policy_ids == {"POL-A", "POL-B", "unmatched"}

    def test_n_claims(self, matched_claims: list[dict]) -> None:
        kpis = compute_policy_kpis(matched_claims)
        kpi_map = {k["policy_id"]: k for k in kpis}
        assert kpi_map["POL-A"]["n_claims"] == 2
        assert kpi_map["POL-B"]["n_claims"] == 1
        assert kpi_map["unmatched"]["n_claims"] == 1

    def test_total_and_avg_amount(self, matched_claims: list[dict]) -> None:
        kpis = compute_policy_kpis(matched_claims)
        kpi_map = {k["policy_id"]: k for k in kpis}
        assert kpi_map["POL-A"]["total_amount"] == pytest.approx(300.0)
        assert kpi_map["POL-A"]["avg_amount"] == pytest.approx(150.0)

    def test_denial_rate(self, matched_claims: list[dict]) -> None:
        kpis = compute_policy_kpis(matched_claims)
        kpi_map = {k["policy_id"]: k for k in kpis}
        assert kpi_map["POL-A"]["denial_rate"] == pytest.approx(0.5)
        assert kpi_map["POL-A"]["approval_rate"] == pytest.approx(0.5)
        assert kpi_map["POL-B"]["denial_rate"] == pytest.approx(0.0)

    def test_sorted_by_total_amount_desc(self, matched_claims: list[dict]) -> None:
        kpis = compute_policy_kpis(matched_claims)
        amounts = [k["total_amount"] for k in kpis]
        assert amounts == sorted(amounts, reverse=True)

    def test_top_dx(self, matched_claims: list[dict]) -> None:
        kpis = compute_policy_kpis(matched_claims)
        kpi_map = {k["policy_id"]: k for k in kpis}
        assert "DX-001" in kpi_map["POL-A"]["top_dx"]

    def test_top_specialties(self, matched_claims: list[dict]) -> None:
        kpis = compute_policy_kpis(matched_claims)
        kpi_map = {k["policy_id"]: k for k in kpis}
        assert "Internal Medicine" in kpi_map["POL-A"]["top_specialties"]

    def test_empty_input(self) -> None:
        kpis = compute_policy_kpis([])
        assert kpis == []
