"""Tests for the policy.match_claims module."""

from __future__ import annotations

import json

import pytest

from um_claims.policy.match_claims import match_claim_to_policy, match_claims_to_policies

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def policy_rule() -> dict:
    return {
        "policy_id": "CPT-99201",
        "covered_cpt_codes": ["CPT-99201", "CPT-99202"],
        "site_of_service": "Professional",
        "diagnosis_constraints": ["DX-1001", "DX-1002"],
    }


@pytest.fixture
def matching_claim() -> dict:
    return {
        "claim_id": "C001",
        "procedure_code": "CPT-99201",
        "claim_type": "Professional",
        "diagnosis_codes": json.dumps(["DX-1001"]),
        "denial_flag": "N",
        "allowed_amount": 120.0,
        "specialty": "Internal Medicine",
    }


@pytest.fixture
def partial_claim() -> dict:
    return {
        "claim_id": "C002",
        "procedure_code": "CPT-99201",
        "claim_type": "Institutional",
        "diagnosis_codes": json.dumps(["DX-9999"]),
        "denial_flag": "N",
        "allowed_amount": 300.0,
        "specialty": "Radiology",
    }


@pytest.fixture
def nonmatching_claim() -> dict:
    return {
        "claim_id": "C003",
        "procedure_code": "CPT-70100",
        "claim_type": "Institutional",
        "diagnosis_codes": json.dumps(["DX-9999"]),
        "denial_flag": "Y",
        "allowed_amount": 500.0,
        "specialty": "Radiology",
    }


# ---------------------------------------------------------------------------
# match_claim_to_policy
# ---------------------------------------------------------------------------

class TestMatchClaimToPolicy:
    def test_full_match_score_is_1(self, matching_claim: dict, policy_rule: dict) -> None:
        score = match_claim_to_policy(matching_claim, policy_rule)
        assert score == pytest.approx(1.0)

    def test_cpt_only_match(self, partial_claim: dict, policy_rule: dict) -> None:
        score = match_claim_to_policy(partial_claim, policy_rule)
        assert score == pytest.approx(0.6)

    def test_no_match(self, nonmatching_claim: dict, policy_rule: dict) -> None:
        score = match_claim_to_policy(nonmatching_claim, policy_rule)
        assert score == pytest.approx(0.0)

    def test_claim_type_match_without_cpt(self, policy_rule: dict) -> None:
        claim = {
            "procedure_code": "CPT-70100",
            "claim_type": "Professional",
            "diagnosis_codes": "[]",
        }
        score = match_claim_to_policy(claim, policy_rule)
        assert score == pytest.approx(0.2)

    def test_dx_match_without_cpt(self, policy_rule: dict) -> None:
        claim = {
            "procedure_code": "CPT-70100",
            "claim_type": "Institutional",
            "diagnosis_codes": json.dumps(["DX-1002"]),
        }
        score = match_claim_to_policy(claim, policy_rule)
        assert score == pytest.approx(0.2)

    def test_empty_policy_scores_zero(self) -> None:
        score = match_claim_to_policy(
            {"procedure_code": "CPT-99201", "claim_type": "Professional", "diagnosis_codes": "[]"},
            {"covered_cpt_codes": [], "site_of_service": "", "diagnosis_constraints": []},
        )
        assert score == 0.0

    def test_deterministic(self, matching_claim: dict, policy_rule: dict) -> None:
        s1 = match_claim_to_policy(matching_claim, policy_rule)
        s2 = match_claim_to_policy(matching_claim, policy_rule)
        assert s1 == s2


# ---------------------------------------------------------------------------
# match_claims_to_policies
# ---------------------------------------------------------------------------

class TestMatchClaimsToPolicies:
    def test_every_claim_gets_assignment(
        self, matching_claim: dict, nonmatching_claim: dict, policy_rule: dict
    ) -> None:
        results = match_claims_to_policies(
            [matching_claim, nonmatching_claim], [policy_rule]
        )
        assert len(results) == 2
        assert all("policy_id" in r for r in results)
        assert all("match_confidence" in r for r in results)

    def test_best_match_selected(self, matching_claim: dict) -> None:
        policy_a = {
            "policy_id": "POL-A",
            "covered_cpt_codes": ["CPT-99201"],
            "site_of_service": "",
            "diagnosis_constraints": [],
        }
        policy_b = {
            "policy_id": "POL-B",
            "covered_cpt_codes": ["CPT-99201"],
            "site_of_service": "Professional",
            "diagnosis_constraints": ["DX-1001"],
        }
        results = match_claims_to_policies([matching_claim], [policy_a, policy_b])
        assert results[0]["policy_id"] == "POL-B"
        assert results[0]["match_confidence"] == pytest.approx(1.0)

    def test_unmatched_claims(self, nonmatching_claim: dict) -> None:
        policy = {
            "policy_id": "POL-X",
            "covered_cpt_codes": ["CPT-99999"],
            "site_of_service": "Professional",
            "diagnosis_constraints": ["DX-0001"],
        }
        results = match_claims_to_policies([nonmatching_claim], [policy])
        assert results[0]["policy_id"] == "unmatched"
        assert results[0]["match_confidence"] == 0.0

    def test_no_policies_gives_unmatched(self, matching_claim: dict) -> None:
        results = match_claims_to_policies([matching_claim], [])
        assert results[0]["policy_id"] == "unmatched"
