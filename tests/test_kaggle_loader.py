"""Tests for the Kaggle Enhanced Claims loader and schema adapter."""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

import polars as pl
import pytest

if TYPE_CHECKING:
    from pathlib import Path

from um_claims.io.kaggle_loader import load_kaggle_claims
from um_claims.io.kaggle_schema_adapter import adapt_kaggle_to_canonical
from um_claims.schema import EXPECTED_COLUMNS


@pytest.fixture
def kaggle_csv(tmp_path: Path) -> Path:
    """Write a small Kaggle-style CSV and return its path."""
    csv_content = textwrap.dedent("""\
        ClaimID,PatientID,ProviderID,ClaimAmount,ClaimDate,DiagnosisCode,ProcedureCode,ProviderSpecialty,ClaimType,ClaimStatus
        K001,P100,PR200,1500.00,2024-03-15,E11.9,99213,Internal Medicine,Professional,Approved
        K002,P101,PR201,250.50,2024-04-20,J06.9,99214,Family Practice,Professional,Denied
        K003,P102,PR202,8200.00,2024-05-10,S72.001A,27236,Orthopedics,Institutional,Approved
        K004,P103,PR203,45.99,2024-06-01,Z23,90471,Pediatrics,Pharmacy,Pending
        K005,P100,PR200,320.00,2024-07-12,I10,93000,Cardiology,Professional,Rejected
    """)
    csv_path = tmp_path / "kaggle_claims.csv"
    csv_path.write_text(csv_content)
    return csv_path


@pytest.fixture
def kaggle_df() -> pl.DataFrame:
    """A Kaggle-style DataFrame already parsed (no file I/O)."""
    return pl.DataFrame(
        {
            "ClaimID": ["K001", "K002"],
            "PatientID": ["P100", "P101"],
            "ProviderID": ["PR200", "PR201"],
            "ClaimAmount": [1500.0, 250.50],
            "ClaimDate": [
                pl.Series([None]).cast(pl.Date).to_list()[0],  # placeholder
            ]
            * 2,
            "DiagnosisCode": ["E11.9", "J06.9"],
            "ProcedureCode": ["99213", "99214"],
            "ProviderSpecialty": ["Internal Medicine", "Family Practice"],
            "ClaimType": ["Professional", "Professional"],
            "ClaimStatus": ["Approved", "Denied"],
        }
    ).with_columns(
        pl.Series("ClaimDate", ["2024-03-15", "2024-04-20"]).str.to_date("%Y-%m-%d")
    )


# ── load_kaggle_claims ─────────────────────────────────────────────────


class TestLoadKaggleClaims:
    def test_loads_csv_and_returns_canonical_schema(self, kaggle_csv: Path) -> None:
        df = load_kaggle_claims(kaggle_csv)
        # All canonical columns must be present
        for col in EXPECTED_COLUMNS:
            assert col in df.columns, f"Missing canonical column: {col}"

    def test_row_count_matches(self, kaggle_csv: Path) -> None:
        df = load_kaggle_claims(kaggle_csv)
        assert len(df) == 5

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_kaggle_claims(tmp_path / "nonexistent.csv")

    def test_missing_columns_raises(self, tmp_path: Path) -> None:
        bad_csv = tmp_path / "bad.csv"
        bad_csv.write_text("col_a,col_b\n1,2\n")
        with pytest.raises(ValueError, match="Missing required Kaggle columns"):
            load_kaggle_claims(bad_csv)

    def test_claim_amounts_are_float(self, kaggle_csv: Path) -> None:
        df = load_kaggle_claims(kaggle_csv)
        assert df["billed_amount"].dtype == pl.Float64
        assert df["allowed_amount"].dtype == pl.Float64
        assert df["paid_amount"].dtype == pl.Float64

    def test_service_date_is_date_type(self, kaggle_csv: Path) -> None:
        df = load_kaggle_claims(kaggle_csv)
        assert df["service_date"].dtype == pl.Date


# ── adapt_kaggle_to_canonical ──────────────────────────────────────────


class TestKaggleSchemaAdapter:
    def test_column_mapping(self, kaggle_df: pl.DataFrame) -> None:
        result = adapt_kaggle_to_canonical(kaggle_df)
        assert result["claim_id"].to_list() == ["K001", "K002"]
        assert result["member_id"].to_list() == ["P100", "P101"]
        assert result["provider_id"].to_list() == ["PR200", "PR201"]
        assert result["procedure_code"].to_list() == ["99213", "99214"]
        assert result["specialty"].to_list() == ["Internal Medicine", "Family Practice"]

    def test_denial_flag_from_status(self, kaggle_df: pl.DataFrame) -> None:
        result = adapt_kaggle_to_canonical(kaggle_df)
        assert result["denial_flag"].to_list() == ["N", "Y"]

    def test_paid_amount_zero_when_denied(self, kaggle_df: pl.DataFrame) -> None:
        result = adapt_kaggle_to_canonical(kaggle_df)
        assert result["paid_amount"][1] == 0.0  # Denied claim
        assert result["paid_amount"][0] > 0.0  # Approved claim

    def test_diagnosis_codes_json_array(self, kaggle_df: pl.DataFrame) -> None:
        result = adapt_kaggle_to_canonical(kaggle_df)
        import json

        for val in result["diagnosis_codes"].to_list():
            parsed = json.loads(val)
            assert isinstance(parsed, list)
            assert len(parsed) == 1

    def test_allowed_amount_derived(self, kaggle_df: pl.DataFrame) -> None:
        result = adapt_kaggle_to_canonical(kaggle_df)
        # allowed_amount = billed_amount * 0.8
        assert result["allowed_amount"][0] == pytest.approx(1500.0 * 0.8)

    def test_claim_type_mapping(self) -> None:
        df = pl.DataFrame(
            {
                "ClaimID": ["K1", "K2", "K3"],
                "PatientID": ["P1", "P2", "P3"],
                "ProviderID": ["PR1", "PR2", "PR3"],
                "ClaimAmount": [100.0, 200.0, 300.0],
                "ClaimDate": pl.Series(["2024-01-01"] * 3).str.to_date("%Y-%m-%d"),
                "DiagnosisCode": ["A00", "B00", "C00"],
                "ProcedureCode": ["99213", "99214", "99215"],
                "ProviderSpecialty": ["IM", "IM", "IM"],
                "ClaimType": ["Professional", "Institutional", "Pharmacy"],
                "ClaimStatus": ["Approved", "Approved", "Approved"],
            }
        )
        result = adapt_kaggle_to_canonical(df)
        assert result["claim_type"].to_list() == ["Professional", "Institutional", "Pharmacy"]

    def test_all_canonical_columns_present(self, kaggle_df: pl.DataFrame) -> None:
        result = adapt_kaggle_to_canonical(kaggle_df)
        for col in EXPECTED_COLUMNS:
            assert col in result.columns, f"Missing: {col}"
