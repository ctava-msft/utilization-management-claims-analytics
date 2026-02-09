"""Claims data validation module.

Two-tier validation:
- Critical: schema conformance, types, required fields, value ranges → fail pipeline.
- Advisory: statistical distribution checks, unusual patterns → warn only.

Spec: SR-2
"""

from __future__ import annotations

from pydantic import BaseModel, Field
import polars as pl

from um_claims.schema import (
    CLAIM_TYPES,
    DENIAL_REASONS,
    EXPECTED_COLUMNS,
    LINES_OF_BUSINESS,
    NETWORK_STATUSES,
    PAYER_PRODUCTS,
    PLAN_TYPES,
    REGIONS,
    REQUIRED_COLUMNS,
    YES_NO,
)


class ValidationIssue(BaseModel):
    """A single validation issue found in the data."""

    level: str = Field(description="'critical' or 'advisory'")
    rule: str = Field(description="Name of the validation rule")
    message: str = Field(description="Human-readable description")
    affected_rows: int = Field(default=0, description="Number of rows affected")
    examples: list[str] = Field(default_factory=list, description="Example values")


class ValidationResult(BaseModel):
    """Result of claims data validation."""

    passed: bool = Field(description="True if no critical issues found")
    total_rows: int = Field(description="Total rows validated")
    critical_issues: list[ValidationIssue] = Field(default_factory=list)
    advisory_issues: list[ValidationIssue] = Field(default_factory=list)

    @property
    def all_issues(self) -> list[ValidationIssue]:
        return self.critical_issues + self.advisory_issues


def validate_claims(df: pl.DataFrame) -> ValidationResult:
    """Validate a claims DataFrame against schema and business rules.

    Args:
        df: Claims DataFrame to validate.

    Returns:
        ValidationResult with pass/fail status and list of issues.
    """
    critical: list[ValidationIssue] = []
    advisory: list[ValidationIssue] = []
    total_rows = len(df)

    # --- Critical: Column presence ---
    missing_cols = set(EXPECTED_COLUMNS.keys()) - set(df.columns)
    if missing_cols:
        critical.append(
            ValidationIssue(
                level="critical",
                rule="column_presence",
                message=f"Missing required columns: {sorted(missing_cols)}",
                affected_rows=total_rows,
            )
        )
        # Can't continue validation if columns are missing
        return ValidationResult(
            passed=False,
            total_rows=total_rows,
            critical_issues=critical,
            advisory_issues=advisory,
        )

    # --- Critical: Required columns not null ---
    for col in REQUIRED_COLUMNS:
        if col in df.columns:
            null_count = df[col].null_count()
            if null_count > 0:
                critical.append(
                    ValidationIssue(
                        level="critical",
                        rule="not_null",
                        message=f"Column '{col}' has {null_count} null values",
                        affected_rows=null_count,
                    )
                )

    # --- Critical: Value ranges ---
    for col in ["billed_amount", "allowed_amount", "paid_amount"]:
        if col in df.columns:
            negative_count = df.filter(pl.col(col) < 0).height
            if negative_count > 0:
                critical.append(
                    ValidationIssue(
                        level="critical",
                        rule="non_negative_amount",
                        message=f"Column '{col}' has {negative_count} negative values",
                        affected_rows=negative_count,
                    )
                )

    if "units" in df.columns:
        invalid_units = df.filter(pl.col("units") < 1).height
        if invalid_units > 0:
            critical.append(
                ValidationIssue(
                    level="critical",
                    rule="positive_units",
                    message=f"Column 'units' has {invalid_units} values < 1",
                    affected_rows=invalid_units,
                )
            )

    # --- Critical: Enum values ---
    enum_checks: dict[str, list[str]] = {
        "payer_product": PAYER_PRODUCTS,
        "plan_type": PLAN_TYPES,
        "line_of_business": LINES_OF_BUSINESS,
        "claim_type": CLAIM_TYPES,
        "network_status": NETWORK_STATUSES,
        "denial_flag": YES_NO,
        "appeal_flag": YES_NO,
        "grievance_flag": YES_NO,
        "dme_flag": YES_NO,
        "authorization_required": YES_NO,
        "geography_region": REGIONS,
    }
    for col, valid_values in enum_checks.items():
        if col in df.columns:
            invalid = df.filter(~pl.col(col).is_in(valid_values)).height
            if invalid > 0:
                bad_examples = (
                    df.filter(~pl.col(col).is_in(valid_values))
                    .select(col)
                    .unique()
                    .head(5)
                    .to_series()
                    .to_list()
                )
                critical.append(
                    ValidationIssue(
                        level="critical",
                        rule="enum_values",
                        message=f"Column '{col}' has {invalid} rows with invalid values",
                        affected_rows=invalid,
                        examples=[str(v) for v in bad_examples],
                    )
                )

    # --- Critical: Referential integrity ---
    # denial_reason required when denial_flag = Y
    if "denial_flag" in df.columns and "denial_reason_category" in df.columns:
        denied_no_reason = df.filter(
            (pl.col("denial_flag") == "Y") & pl.col("denial_reason_category").is_null()
        ).height
        if denied_no_reason > 0:
            critical.append(
                ValidationIssue(
                    level="critical",
                    rule="denial_reason_required",
                    message=(
                        f"{denied_no_reason} claims have denial_flag=Y "
                        f"but null denial_reason_category"
                    ),
                    affected_rows=denied_no_reason,
                )
            )

    # denial_reason should be valid enum when present
    if "denial_reason_category" in df.columns:
        has_reason = df.filter(pl.col("denial_reason_category").is_not_null())
        if has_reason.height > 0:
            invalid_reasons = has_reason.filter(
                ~pl.col("denial_reason_category").is_in(DENIAL_REASONS)
            ).height
            if invalid_reasons > 0:
                critical.append(
                    ValidationIssue(
                        level="critical",
                        rule="denial_reason_enum",
                        message=f"{invalid_reasons} claims have invalid denial_reason_category",
                        affected_rows=invalid_reasons,
                    )
                )

    # --- Critical: Date ordering ---
    if "service_date" in df.columns and "claim_received_date" in df.columns:
        bad_dates = df.filter(pl.col("claim_received_date") < pl.col("service_date")).height
        if bad_dates > 0:
            critical.append(
                ValidationIssue(
                    level="critical",
                    rule="date_ordering",
                    message=f"{bad_dates} claims have claim_received_date before service_date",
                    affected_rows=bad_dates,
                )
            )

    # --- Advisory: Statistical checks ---
    for col in ["billed_amount", "allowed_amount", "paid_amount"]:
        if col in df.columns:
            variance = df[col].var()
            if variance is not None and variance == 0:
                advisory.append(
                    ValidationIssue(
                        level="advisory",
                        rule="zero_variance",
                        message=f"Column '{col}' has zero variance — suspicious for real data",
                        affected_rows=total_rows,
                    )
                )

    # --- Advisory: High null rate ---
    for col in df.columns:
        null_rate = df[col].null_count() / total_rows if total_rows > 0 else 0
        if null_rate > 0.50 and col not in ["facility_id", "paid_date", "revenue_code", "authorization_id", "supplier_type"]:
            advisory.append(
                ValidationIssue(
                    level="advisory",
                    rule="high_null_rate",
                    message=f"Column '{col}' has {null_rate:.1%} nulls",
                    affected_rows=df[col].null_count(),
                )
            )

    passed = len(critical) == 0
    return ValidationResult(
        passed=passed,
        total_rows=total_rows,
        critical_issues=critical,
        advisory_issues=advisory,
    )
