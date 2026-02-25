"""Tests for the policy.parse_policy_md module, including round-trip validation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from um_claims.policy.generate_policy_md import generate_policy_markdown
from um_claims.policy.parse_policy_md import (
    parse_policies_dir,
    parse_policy_markdown,
)

if TYPE_CHECKING:
    from pathlib import Path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def seed_row() -> dict:
    """A minimal, realistic policy seed row identical to the generator test fixture."""
    return {
        "procedure_code": "CPT-99201",
        "claim_type": "Professional",
        "specialty": "Internal Medicine",
        "n_claims": 120,
        "approval_rate": 0.85,
        "denial_rate": 0.15,
        "avg_claim_amount": 108.0,
        "p50_claim_amount": 110.0,
        "p90_claim_amount": 130.0,
        "top_diagnosis_codes": json.dumps(
            [{"code": "DX-001", "count": 40}, {"code": "DX-002", "count": 20}]
        ),
    }


@pytest.fixture
def generated_md(seed_row: dict) -> str:
    """Markdown string produced by generate_policy_markdown for seed_row."""
    return generate_policy_markdown(seed_row)


@pytest.fixture
def parsed(generated_md: str) -> dict:
    """Parsed dict from the generated Markdown."""
    return parse_policy_markdown(generated_md)


# ---------------------------------------------------------------------------
# parse_policy_markdown — basic structure
# ---------------------------------------------------------------------------


class TestParsePolicyMarkdown:
    def test_returns_dict(self, parsed: dict) -> None:
        assert isinstance(parsed, dict)

    def test_has_required_keys(self, parsed: dict) -> None:
        expected = {
            "policy_id",
            "covered_cpt_codes",
            "site_of_service",
            "diagnosis_constraints",
            "documentation_requirements",
        }
        assert expected == set(parsed.keys())

    def test_policy_id_is_procedure_code(self, parsed: dict) -> None:
        assert parsed["policy_id"] == "CPT-99201"

    def test_explicit_policy_id_overrides(self, generated_md: str) -> None:
        result = parse_policy_markdown(generated_md, policy_id="my-custom-id")
        assert result["policy_id"] == "my-custom-id"

    def test_covered_cpt_codes_is_list(self, parsed: dict) -> None:
        assert isinstance(parsed["covered_cpt_codes"], list)

    def test_covered_cpt_codes_contains_procedure_code(self, parsed: dict) -> None:
        assert "CPT-99201" in parsed["covered_cpt_codes"]

    def test_site_of_service_extracted(self, parsed: dict) -> None:
        assert parsed["site_of_service"] == "Professional"

    def test_diagnosis_constraints_is_list(self, parsed: dict) -> None:
        assert isinstance(parsed["diagnosis_constraints"], list)

    def test_diagnosis_constraints_extracted(self, parsed: dict) -> None:
        assert "DX-001" in parsed["diagnosis_constraints"]
        assert "DX-002" in parsed["diagnosis_constraints"]

    def test_documentation_requirements_is_list(self, parsed: dict) -> None:
        assert isinstance(parsed["documentation_requirements"], list)

    def test_documentation_requirements_not_empty(self, parsed: dict) -> None:
        assert len(parsed["documentation_requirements"]) > 0

    def test_no_na_in_diagnosis_constraints_when_empty(self) -> None:
        """When there are no Dx codes the parser should return an empty list (not 'N/A')."""
        row = {
            "procedure_code": "CPT-00000",
            "claim_type": "Institutional",
            "specialty": "Surgery",
            "n_claims": 10,
            "approval_rate": 0.8,
            "denial_rate": 0.2,
            "avg_claim_amount": 500.0,
            "p50_claim_amount": 480.0,
            "p90_claim_amount": 700.0,
            "top_diagnosis_codes": [],
        }
        md = generate_policy_markdown(row)
        result = parse_policy_markdown(md)
        assert result["diagnosis_constraints"] == []

    def test_different_claim_type(self) -> None:
        """Site-of-service field reflects the claim_type from the seed."""
        row = {
            "procedure_code": "CPT-70100",
            "claim_type": "Institutional",
            "specialty": "Radiology",
            "n_claims": 50,
            "approval_rate": 0.9,
            "denial_rate": 0.1,
            "avg_claim_amount": 300.0,
            "p50_claim_amount": 290.0,
            "p90_claim_amount": 400.0,
            "top_diagnosis_codes": [{"code": "DX-004", "count": 20}],
        }
        md = generate_policy_markdown(row)
        result = parse_policy_markdown(md)
        assert result["site_of_service"] == "Institutional"

    def test_empty_string_returns_safe_defaults(self) -> None:
        result = parse_policy_markdown("")
        assert result["policy_id"] == ""
        assert result["covered_cpt_codes"] == []
        assert result["site_of_service"] == ""
        assert result["diagnosis_constraints"] == []
        assert result["documentation_requirements"] == []


# ---------------------------------------------------------------------------
# Round-trip validation
# ---------------------------------------------------------------------------


class TestRoundTrip:
    """Generate a policy from a seed, parse it back, and assert CPT codes match."""

    def test_cpt_in_parsed_matches_seed_procedure_code(self, seed_row: dict) -> None:
        """Core round-trip: seed ProcedureCode must appear in parsed covered_cpt_codes."""
        md = generate_policy_markdown(seed_row)
        result = parse_policy_markdown(md)
        assert seed_row["procedure_code"] in result["covered_cpt_codes"]

    def test_dx_codes_in_parsed_match_seed_top_dx(self, seed_row: dict) -> None:
        """Parsed diagnosis_constraints must include all top-Dx codes from seed."""
        top_dx = json.loads(seed_row["top_diagnosis_codes"])
        expected_codes = {entry["code"] for entry in top_dx}
        md = generate_policy_markdown(seed_row)
        result = parse_policy_markdown(md)
        parsed_codes = set(result["diagnosis_constraints"])
        assert expected_codes <= parsed_codes

    def test_site_of_service_matches_seed_claim_type(self, seed_row: dict) -> None:
        md = generate_policy_markdown(seed_row)
        result = parse_policy_markdown(md)
        assert result["site_of_service"] == seed_row["claim_type"]

    def test_round_trip_multiple_seeds(self) -> None:
        """Round-trip for multiple seeds of varying CPT codes and claim types."""
        seeds = [
            {
                "procedure_code": "CPT-99201",
                "claim_type": "Professional",
                "specialty": "Internal Medicine",
                "n_claims": 120,
                "approval_rate": 0.85,
                "denial_rate": 0.15,
                "avg_claim_amount": 108.0,
                "p50_claim_amount": 110.0,
                "p90_claim_amount": 130.0,
                "top_diagnosis_codes": json.dumps(
                    [{"code": "DX-001", "count": 40}]
                ),
            },
            {
                "procedure_code": "CPT-70100",
                "claim_type": "Institutional",
                "specialty": "Radiology",
                "n_claims": 85,
                "approval_rate": 0.90,
                "denial_rate": 0.10,
                "avg_claim_amount": 390.0,
                "p50_claim_amount": 385.0,
                "p90_claim_amount": 420.0,
                "top_diagnosis_codes": json.dumps(
                    [{"code": "DX-004", "count": 30}, {"code": "DX-005", "count": 10}]
                ),
            },
        ]
        for seed in seeds:
            md = generate_policy_markdown(seed)
            result = parse_policy_markdown(md)
            assert seed["procedure_code"] in result["covered_cpt_codes"], (
                f"procedure_code {seed['procedure_code']} not found in "
                f"covered_cpt_codes: {result['covered_cpt_codes']}"
            )
            assert result["site_of_service"] == seed["claim_type"]


# ---------------------------------------------------------------------------
# parse_policies_dir
# ---------------------------------------------------------------------------


class TestParsePoliciesDir:
    def _make_policy_dir(self, tmp_path: Path, seeds: list[dict]) -> Path:
        """Write one .md policy file per seed row into tmp_path and return it."""
        import polars as pl

        from um_claims.policy.generate_policy_md import write_policies

        # Convert list of dicts to a Polars DataFrame
        df = pl.DataFrame(
            {
                "procedure_code": [s["procedure_code"] for s in seeds],
                "claim_type": [s["claim_type"] for s in seeds],
                "specialty": [s["specialty"] for s in seeds],
                "n_claims": [s["n_claims"] for s in seeds],
                "approval_rate": [s["approval_rate"] for s in seeds],
                "denial_rate": [s["denial_rate"] for s in seeds],
                "avg_claim_amount": [s["avg_claim_amount"] for s in seeds],
                "p50_claim_amount": [s["p50_claim_amount"] for s in seeds],
                "p90_claim_amount": [s["p90_claim_amount"] for s in seeds],
                "top_diagnosis_codes": [s["top_diagnosis_codes"] for s in seeds],
            }
        )
        policy_dir = tmp_path / "policies"
        write_policies(df, out_dir=policy_dir)
        return policy_dir

    @pytest.fixture
    def two_seeds(self) -> list[dict]:
        return [
            {
                "procedure_code": "CPT-99201",
                "claim_type": "Professional",
                "specialty": "Internal Medicine",
                "n_claims": 120,
                "approval_rate": 0.85,
                "denial_rate": 0.15,
                "avg_claim_amount": 108.0,
                "p50_claim_amount": 110.0,
                "p90_claim_amount": 130.0,
                "top_diagnosis_codes": json.dumps([{"code": "DX-001", "count": 40}]),
            },
            {
                "procedure_code": "CPT-70100",
                "claim_type": "Institutional",
                "specialty": "Radiology",
                "n_claims": 85,
                "approval_rate": 0.90,
                "denial_rate": 0.10,
                "avg_claim_amount": 390.0,
                "p50_claim_amount": 385.0,
                "p90_claim_amount": 420.0,
                "top_diagnosis_codes": json.dumps([{"code": "DX-004", "count": 30}]),
            },
        ]

    def test_produces_jsonl_file(self, tmp_path: Path, two_seeds: list[dict]) -> None:
        policy_dir = self._make_policy_dir(tmp_path, two_seeds)
        out = tmp_path / "policies.jsonl"
        result_path = parse_policies_dir(policy_dir, output=out)
        assert result_path.exists()

    def test_jsonl_line_count_matches_policy_files(
        self, tmp_path: Path, two_seeds: list[dict]
    ) -> None:
        policy_dir = self._make_policy_dir(tmp_path, two_seeds)
        out = tmp_path / "policies.jsonl"
        parse_policies_dir(policy_dir, output=out)
        lines = [line for line in out.read_text().splitlines() if line.strip()]
        assert len(lines) == len(two_seeds)

    def test_each_line_is_valid_json(
        self, tmp_path: Path, two_seeds: list[dict]
    ) -> None:
        policy_dir = self._make_policy_dir(tmp_path, two_seeds)
        out = tmp_path / "policies.jsonl"
        parse_policies_dir(policy_dir, output=out)
        for line in out.read_text().splitlines():
            if line.strip():
                rec = json.loads(line)
                assert "policy_id" in rec
                assert "covered_cpt_codes" in rec

    def test_source_file_key_present(
        self, tmp_path: Path, two_seeds: list[dict]
    ) -> None:
        policy_dir = self._make_policy_dir(tmp_path, two_seeds)
        out = tmp_path / "policies.jsonl"
        parse_policies_dir(policy_dir, output=out)
        for line in out.read_text().splitlines():
            if line.strip():
                rec = json.loads(line)
                assert "source_file" in rec
                assert rec["source_file"].endswith(".md")

    def test_raises_for_missing_dir(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            parse_policies_dir(tmp_path / "does_not_exist", output=tmp_path / "out.jsonl")

    def test_round_trip_cpt_in_all_records(
        self, tmp_path: Path, two_seeds: list[dict]
    ) -> None:
        """Full round-trip: every parsed record contains the seed procedure_code."""
        policy_dir = self._make_policy_dir(tmp_path, two_seeds)
        out = tmp_path / "policies.jsonl"
        parse_policies_dir(policy_dir, output=out)
        records = [json.loads(line) for line in out.read_text().splitlines() if line.strip()]
        cpt_codes_in_records = {
            cpt
            for rec in records
            for cpt in rec["covered_cpt_codes"]
        }
        for seed in two_seeds:
            assert seed["procedure_code"] in cpt_codes_in_records, (
                f"{seed['procedure_code']} not found in any parsed record's covered_cpt_codes"
            )
