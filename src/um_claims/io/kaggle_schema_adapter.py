"""Schema adapter: Kaggle Enhanced Claims → canonical UM Claims schema.

Maps columns from the Kaggle dataset to the repo's canonical column names
and types defined in um_claims.schema.EXPECTED_COLUMNS.  Columns not
present in the Kaggle data are filled with sensible defaults or nulls.
"""

from __future__ import annotations

import json

import polars as pl


def adapt_kaggle_to_canonical(df: pl.DataFrame) -> pl.DataFrame:
    """Map a Kaggle-normalized DataFrame to the canonical UM Claims schema.

    Mapping:
        ClaimID          → claim_id
        PatientID        → member_id
        ProviderID       → provider_id
        ClaimAmount      → billed_amount (allowed/paid derived)
        ClaimDate        → service_date  (claim_received_date derived)
        DiagnosisCode    → diagnosis_codes (JSON list)
        ProcedureCode    → procedure_code
        ProviderSpecialty → specialty
        ClaimType        → claim_type
        ClaimStatus      → denial_flag / paid_date derivation

    Args:
        df: Polars DataFrame with normalized Kaggle columns.

    Returns:
        Polars DataFrame conforming to the canonical schema.
    """
    # Derive denial_flag from ClaimStatus
    denial_flag = (
        pl.when(pl.col("ClaimStatus").str.to_lowercase().is_in(["denied", "rejected"]))
        .then(pl.lit("Y"))
        .otherwise(pl.lit("N"))
    )

    # Derive paid_date: only set when claim is not denied
    paid_date = (
        pl.when(pl.col("ClaimStatus").str.to_lowercase().is_in(["denied", "rejected"]))
        .then(pl.lit(None).cast(pl.Date))
        .otherwise(pl.col("ClaimDate") + pl.duration(days=30))
    )

    # Map ClaimType to canonical values
    claim_type_mapped = (
        pl.when(pl.col("ClaimType").str.to_lowercase().str.contains("pharm"))
        .then(pl.lit("Pharmacy"))
        .when(pl.col("ClaimType").str.to_lowercase().str.contains("inst"))
        .then(pl.lit("Institutional"))
        .otherwise(pl.lit("Professional"))
    )

    canonical = df.select(
        # Direct mappings
        pl.col("ClaimID").alias("claim_id"),
        pl.col("PatientID").alias("member_id"),
        pl.col("ProviderID").alias("provider_id"),
        pl.lit(None).cast(pl.Utf8).alias("facility_id"),
        # Defaults for fields not in Kaggle data
        pl.lit("Commercial").alias("payer_product"),
        pl.lit("PPO").alias("plan_type"),
        pl.lit("Group").alias("line_of_business"),
        # Date fields
        pl.col("ClaimDate").alias("service_date"),
        pl.col("ClaimDate").alias("claim_received_date"),
        paid_date.alias("paid_date"),
        # Claim type mapping
        claim_type_mapped.alias("claim_type"),
        pl.lit("11").alias("place_of_service"),
        # Wrap single diagnosis code in JSON array
        pl.col("DiagnosisCode")
        .map_elements(lambda v: json.dumps([v]) if v else '["UNKNOWN"]', return_dtype=pl.Utf8)
        .alias("diagnosis_codes"),
        pl.col("ProcedureCode").alias("procedure_code"),
        pl.lit(None).cast(pl.Utf8).alias("revenue_code"),
        # Amount fields — billed from Kaggle, derive allowed/paid
        pl.col("ClaimAmount").alias("billed_amount"),
        (pl.col("ClaimAmount") * 0.8).alias("allowed_amount"),
        (
            pl.when(pl.col("ClaimStatus").str.to_lowercase().is_in(["denied", "rejected"]))
            .then(pl.lit(0.0))
            .otherwise(pl.col("ClaimAmount") * 0.7)
        ).alias("paid_amount"),
        pl.lit(1).cast(pl.Int64).alias("units"),
        pl.lit("INN").alias("network_status"),
        pl.lit("N").alias("authorization_required"),
        pl.lit(None).cast(pl.Utf8).alias("authorization_id"),
        # Status-derived flags
        denial_flag.alias("denial_flag"),
        (
            pl.when(pl.col("ClaimStatus").str.to_lowercase().is_in(["denied", "rejected"]))
            .then(pl.lit("medical_necessity"))
            .otherwise(pl.lit(None).cast(pl.Utf8))
        ).alias("denial_reason_category"),
        pl.lit("N").alias("appeal_flag"),
        pl.lit("N").alias("grievance_flag"),
        pl.lit("N").alias("dme_flag"),
        pl.lit(None).cast(pl.Utf8).alias("supplier_type"),
        # Synthetic NPI from ProviderID
        pl.col("ProviderID").alias("rendering_npi"),
        pl.col("ProviderID").alias("billing_npi"),
        # Default geography
        pl.lit("PA").alias("geography_state"),
        pl.lit("Northeast").alias("geography_region"),
        # Specialty mapping
        pl.col("ProviderSpecialty").alias("specialty"),
    )

    return canonical
