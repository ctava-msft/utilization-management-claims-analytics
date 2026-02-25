"""Tests for the policy_seeds module."""

from __future__ import annotations

import json
from pathlib import Path  # noqa: TC003

import polars as pl
import pytest

from um_claims.policy_seeds import build_policy_seeds, write_policy_seeds

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def seeds_input_df() -> pl.DataFrame:
    """Hand-crafted claims DataFrame with known group structure for precise testing.

    Groups:
      A — procedure=CPT-99201, claim_type=Professional, specialty=Internal Medicine
          5 claims: 4 approved, 1 denied; allowed=[100,120,80,110,130]
      B — procedure=CPT-70100, claim_type=Professional, specialty=Radiology
          4 claims: 3 approved, 1 denied; allowed=[350,400,420,380]
    """
    n_a, n_b = 5, 4
    return pl.DataFrame(
        {
            "procedure_code": ["CPT-99201"] * n_a + ["CPT-70100"] * n_b,
            "claim_type": ["Professional"] * n_a + ["Professional"] * n_b,
            "specialty": ["Internal Medicine"] * n_a + ["Radiology"] * n_b,
            "denial_flag": ["N", "N", "Y", "N", "N", "Y", "N", "N", "N"],
            "allowed_amount": [100.0, 120.0, 80.0, 110.0, 130.0, 350.0, 400.0, 420.0, 380.0],
            "diagnosis_codes": [
                '["DX-001"]',
                '["DX-001", "DX-002"]',
                '["DX-001"]',
                '["DX-003"]',
                '["DX-002"]',
                '["DX-004"]',
                '["DX-004"]',
                '["DX-005"]',
                '["DX-004"]',
            ],
        }
    )


# ---------------------------------------------------------------------------
# build_policy_seeds
# ---------------------------------------------------------------------------

class TestBuildPolicySeeds:
    def test_columns_present(self, seeds_input_df: pl.DataFrame) -> None:
        result = build_policy_seeds(seeds_input_df, min_claims=1)
        expected = {
            "procedure_code",
            "claim_type",
            "specialty",
            "n_claims",
            "approval_rate",
            "denial_rate",
            "avg_claim_amount",
            "p50_claim_amount",
            "p90_claim_amount",
            "top_diagnosis_codes",
        }
        assert expected.issubset(set(result.columns))

    def test_n_claims_correct(self, seeds_input_df: pl.DataFrame) -> None:
        result = build_policy_seeds(seeds_input_df, min_claims=1)
        row_a = result.filter(pl.col("procedure_code") == "CPT-99201")
        assert row_a["n_claims"][0] == 5
        row_b = result.filter(pl.col("procedure_code") == "CPT-70100")
        assert row_b["n_claims"][0] == 4

    def test_approval_and_denial_rates(self, seeds_input_df: pl.DataFrame) -> None:
        result = build_policy_seeds(seeds_input_df, min_claims=1)
        row_a = result.filter(pl.col("procedure_code") == "CPT-99201")
        # Group A: 4 approved, 1 denied out of 5
        assert abs(row_a["approval_rate"][0] - 4 / 5) < 1e-9
        assert abs(row_a["denial_rate"][0] - 1 / 5) < 1e-9

    def test_avg_claim_amount(self, seeds_input_df: pl.DataFrame) -> None:
        result = build_policy_seeds(seeds_input_df, min_claims=1)
        row_a = result.filter(pl.col("procedure_code") == "CPT-99201")
        expected_avg = (100 + 120 + 80 + 110 + 130) / 5  # 108.0
        assert abs(row_a["avg_claim_amount"][0] - expected_avg) < 1e-6

    def test_p90_claim_amount(self, seeds_input_df: pl.DataFrame) -> None:
        result = build_policy_seeds(seeds_input_df, min_claims=1)
        row_b = result.filter(pl.col("procedure_code") == "CPT-70100")
        # p90 of [350, 380, 400, 420] should be <= 420
        assert row_b["p90_claim_amount"][0] <= 420.0
        assert row_b["p90_claim_amount"][0] >= 350.0

    def test_top_diagnosis_codes_is_valid_json(self, seeds_input_df: pl.DataFrame) -> None:
        result = build_policy_seeds(seeds_input_df, min_claims=1)
        for row in result["top_diagnosis_codes"].to_list():
            parsed = json.loads(row)
            assert isinstance(parsed, list)
            for entry in parsed:
                assert "code" in entry
                assert "count" in entry

    def test_top_diagnosis_codes_correct_top_code(self, seeds_input_df: pl.DataFrame) -> None:
        result = build_policy_seeds(seeds_input_df, min_claims=1)
        # Group A: DX-001 appears 3 times (rows 0,1,2), should be #1
        row_a = result.filter(pl.col("procedure_code") == "CPT-99201")
        top_codes = json.loads(row_a["top_diagnosis_codes"][0])
        assert top_codes[0]["code"] == "DX-001"
        assert top_codes[0]["count"] == 3
        # Group B: DX-004 appears 3 times (rows 5,6,8), should be #1
        row_b = result.filter(pl.col("procedure_code") == "CPT-70100")
        top_codes_b = json.loads(row_b["top_diagnosis_codes"][0])
        assert top_codes_b[0]["code"] == "DX-004"
        assert top_codes_b[0]["count"] == 3

    def test_min_claims_filter(self, seeds_input_df: pl.DataFrame) -> None:
        # With min_claims=5, only group A (5 claims) should survive
        result = build_policy_seeds(seeds_input_df, min_claims=5)
        assert len(result) == 1
        assert result["procedure_code"][0] == "CPT-99201"

    def test_min_claims_filter_excludes_all(self, seeds_input_df: pl.DataFrame) -> None:
        result = build_policy_seeds(seeds_input_df, min_claims=100)
        assert len(result) == 0

    def test_deterministic_ordering(self, seeds_input_df: pl.DataFrame) -> None:
        # Run twice; result order must be identical
        r1 = build_policy_seeds(seeds_input_df, min_claims=1)
        r2 = build_policy_seeds(seeds_input_df, min_claims=1)
        assert r1["procedure_code"].to_list() == r2["procedure_code"].to_list()
        assert r1["claim_type"].to_list() == r2["claim_type"].to_list()
        assert r1["specialty"].to_list() == r2["specialty"].to_list()

    def test_sorted_by_group_keys(self, seeds_input_df: pl.DataFrame) -> None:
        result = build_policy_seeds(seeds_input_df, min_claims=1)
        codes = result["procedure_code"].to_list()
        assert codes == sorted(codes), "Rows must be sorted by procedure_code"


# ---------------------------------------------------------------------------
# write_policy_seeds
# ---------------------------------------------------------------------------

class TestWritePolicySeeds:
    def test_writes_parquet_and_jsonl(
        self, seeds_input_df: pl.DataFrame, tmp_path: Path
    ) -> None:
        seeds = build_policy_seeds(seeds_input_df, min_claims=1)
        parquet_path, jsonl_path = write_policy_seeds(seeds, tmp_path)
        assert parquet_path.exists()
        assert jsonl_path.exists()

    def test_parquet_roundtrip(
        self, seeds_input_df: pl.DataFrame, tmp_path: Path
    ) -> None:
        seeds = build_policy_seeds(seeds_input_df, min_claims=1)
        parquet_path, _ = write_policy_seeds(seeds, tmp_path)
        reloaded = pl.read_parquet(parquet_path)
        assert reloaded.shape == seeds.shape
        assert reloaded["n_claims"].to_list() == seeds["n_claims"].to_list()

    def test_jsonl_valid_json_lines(
        self, seeds_input_df: pl.DataFrame, tmp_path: Path
    ) -> None:
        seeds = build_policy_seeds(seeds_input_df, min_claims=1)
        _, jsonl_path = write_policy_seeds(seeds, tmp_path)
        lines = jsonl_path.read_text().strip().splitlines()
        assert len(lines) == len(seeds)
        for line in lines:
            obj = json.loads(line)
            assert "procedure_code" in obj
            assert "n_claims" in obj
            # top_diagnosis_codes should be a list (parsed from JSON string)
            assert isinstance(obj["top_diagnosis_codes"], list)
