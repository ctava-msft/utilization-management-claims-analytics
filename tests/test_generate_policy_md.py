"""Tests for the policy.generate_policy_md module."""

from __future__ import annotations

import json
from pathlib import Path  # noqa: TC003

import polars as pl
import pytest

from um_claims.policy.generate_policy_md import (
    _sanitize,
    generate_policy_markdown,
    write_policies,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def seed_row() -> dict:
    """A minimal, realistic policy seed row."""
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
def seeds_df() -> pl.DataFrame:
    """Small seeds DataFrame with two rows for write_policies tests."""
    return pl.DataFrame(
        {
            "procedure_code": ["CPT-99201", "CPT-70100"],
            "claim_type": ["Professional", "Professional"],
            "specialty": ["Internal Medicine", "Radiology"],
            "n_claims": [120, 85],
            "approval_rate": [0.85, 0.90],
            "denial_rate": [0.15, 0.10],
            "avg_claim_amount": [108.0, 390.0],
            "p50_claim_amount": [110.0, 385.0],
            "p90_claim_amount": [130.0, 420.0],
            "top_diagnosis_codes": [
                json.dumps([{"code": "DX-001", "count": 40}]),
                json.dumps([{"code": "DX-004", "count": 30}]),
            ],
        }
    )


# ---------------------------------------------------------------------------
# _sanitize
# ---------------------------------------------------------------------------


class TestSanitize:
    def test_replaces_hyphens(self) -> None:
        assert _sanitize("CPT-99201") == "CPT_99201"

    def test_replaces_spaces(self) -> None:
        assert _sanitize("Internal Medicine") == "Internal_Medicine"

    def test_preserves_alphanumeric(self) -> None:
        assert _sanitize("Radiology123") == "Radiology123"

    def test_multiple_special_chars(self) -> None:
        assert _sanitize("foo  --  bar") == "foo_bar"


# ---------------------------------------------------------------------------
# generate_policy_markdown
# ---------------------------------------------------------------------------


class TestGeneratePolicyMarkdown:
    def test_returns_string(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert isinstance(result, str)

    def test_contains_procedure_code(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "CPT-99201" in result

    def test_contains_claim_type(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "Professional" in result

    def test_contains_specialty(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "Internal Medicine" in result

    def test_contains_diagnosis_codes(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "DX-001" in result
        assert "DX-002" in result

    def test_contains_effective_date(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "2024-01-01" in result

    def test_is_deterministic(self, seed_row: dict) -> None:
        r1 = generate_policy_markdown(seed_row)
        r2 = generate_policy_markdown(seed_row)
        assert r1 == r2

    def test_markdown_title(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert result.startswith("# Prior Authorization Policy:")

    def test_has_covered_services_section(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "## 1. Covered Services" in result

    def test_has_authorization_criteria_section(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "## 2. Authorization Criteria" in result

    def test_has_diagnosis_context_section(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "## 3. Diagnosis Context" in result

    def test_has_documentation_requirements_section(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "## 4. Documentation Requirements" in result

    def test_has_cost_reference_section(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "## 5. Cost Reference" in result

    def test_diagnosis_criteria_line_present(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "Requires documentation of Dx" in result

    def test_n_claims_displayed(self, seed_row: dict) -> None:
        result = generate_policy_markdown(seed_row)
        assert "120" in result

    def test_top_dx_as_list(self) -> None:
        """seed_row with top_diagnosis_codes already as a list (not JSON str)."""
        row = {
            "procedure_code": "CPT-70100",
            "claim_type": "Professional",
            "specialty": "Radiology",
            "n_claims": 50,
            "approval_rate": 0.9,
            "denial_rate": 0.1,
            "avg_claim_amount": 300.0,
            "p50_claim_amount": 290.0,
            "p90_claim_amount": 400.0,
            "top_diagnosis_codes": [{"code": "DX-004", "count": 20}],
        }
        result = generate_policy_markdown(row)
        assert "DX-004" in result

    def test_empty_dx_codes(self) -> None:
        """Policy still generates when there are no diagnosis codes."""
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
        result = generate_policy_markdown(row)
        assert "CPT-00000" in result
        assert "N/A" in result


# ---------------------------------------------------------------------------
# write_policies
# ---------------------------------------------------------------------------


class TestWritePolicies:
    def test_writes_correct_number_of_files(
        self, seeds_df: pl.DataFrame, tmp_path: Path
    ) -> None:
        written = write_policies(seeds_df, out_dir=tmp_path)
        assert len(written) == 2

    def test_files_exist(self, seeds_df: pl.DataFrame, tmp_path: Path) -> None:
        written = write_policies(seeds_df, out_dir=tmp_path)
        for path in written:
            assert path.exists()

    def test_filenames_follow_convention(
        self, seeds_df: pl.DataFrame, tmp_path: Path
    ) -> None:
        written = write_policies(seeds_df, out_dir=tmp_path)
        names = {p.name for p in written}
        assert "POL_CPT_99201_Professional_Internal_Medicine.md" in names
        assert "POL_CPT_70100_Professional_Radiology.md" in names

    def test_file_content_contains_procedure_code(
        self, seeds_df: pl.DataFrame, tmp_path: Path
    ) -> None:
        written = write_policies(seeds_df, out_dir=tmp_path)
        for path in written:
            text = path.read_text(encoding="utf-8")
            assert "# Prior Authorization Policy:" in text

    def test_output_is_deterministic(
        self, seeds_df: pl.DataFrame, tmp_path: Path
    ) -> None:
        dir1 = tmp_path / "run1"
        dir2 = tmp_path / "run2"
        write_policies(seeds_df, out_dir=dir1)
        write_policies(seeds_df, out_dir=dir2)
        for p1, p2 in zip(sorted(dir1.iterdir()), sorted(dir2.iterdir()), strict=True):
            assert p1.read_text() == p2.read_text()

    def test_creates_output_dir(self, seeds_df: pl.DataFrame, tmp_path: Path) -> None:
        out = tmp_path / "new_subdir" / "policies"
        write_policies(seeds_df, out_dir=out)
        assert out.is_dir()

    def test_returns_sorted_paths(
        self, seeds_df: pl.DataFrame, tmp_path: Path
    ) -> None:
        written = write_policies(seeds_df, out_dir=tmp_path)
        assert written == sorted(written)
