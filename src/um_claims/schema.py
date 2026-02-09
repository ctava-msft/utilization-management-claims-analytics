"""Schema definitions for UM claims data.

Provides:
- Pydantic model for individual claim records (serialization / deserialization).
- Pandera DataFrameModel for Polars DataFrame validation (schema gate).
- Constants for enum-like field values.
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Enum-like constants
# ---------------------------------------------------------------------------
PAYER_PRODUCTS = ["Commercial", "Medicare", "Medicaid", "Exchange"]
PLAN_TYPES = ["HMO", "PPO", "POS", "EPO"]
LINES_OF_BUSINESS = ["Group", "Individual", "Medicare", "Medicaid"]
CLAIM_TYPES = ["Professional", "Institutional", "Pharmacy"]
NETWORK_STATUSES = ["INN", "OON"]
YES_NO = ["Y", "N"]
DENIAL_REASONS = [
    "medical_necessity",
    "not_covered",
    "authorization_missing",
    "coding_error",
    "duplicate",
    "untimely_filing",
]
REGIONS = ["Northeast", "Southeast", "Midwest", "West"]
STATES_BY_REGION: dict[str, list[str]] = {
    "Northeast": ["PA", "NY", "NJ", "CT", "MA"],
    "Southeast": ["FL", "GA", "NC", "VA", "SC"],
    "Midwest": ["OH", "IL", "MI", "IN", "WI"],
    "West": ["CA", "WA", "OR", "CO", "AZ"],
}
SPECIALTIES = [
    "Internal Medicine",
    "Family Practice",
    "Orthopedics",
    "Cardiology",
    "Radiology",
    "General Surgery",
    "Emergency Medicine",
    "Neurology",
    "Pulmonology",
    "DME Supplier",
    "Oncology",
    "Dermatology",
    "Psychiatry",
    "Pediatrics",
    "OB/GYN",
]
PLACE_OF_SERVICE_CODES = [
    "11",  # Office
    "21",  # Inpatient Hospital
    "22",  # Outpatient Hospital
    "23",  # Emergency Room
    "31",  # Skilled Nursing Facility
    "12",  # Home
    "81",  # Independent Laboratory
    "99",  # Other
]

# Procedure code pools by service category
PROCEDURE_CODES: dict[str, list[str]] = {
    "E&M": [f"CPT-99{i:03d}" for i in range(201, 216)],
    "Imaging": [f"CPT-7{i:04d}" for i in range(100, 130)],
    "Surgical": [
        f"CPT-{prefix}{i:03d}" for prefix in [2, 3, 4, 5, 6] for i in range(100, 110)
    ],
    "DME": [f"HCPCS-E{i:04d}" for i in range(100, 120)] + [
        f"HCPCS-K{i:04d}" for i in range(100, 110)
    ],
    "Pharmacy": [f"RX-{i:05d}" for i in range(1000, 1050)],
    "Other": [f"CPT-8{i:04d}" for i in range(100, 120)],
}


# ---------------------------------------------------------------------------
# Pydantic record model (for single-claim serialization)
# ---------------------------------------------------------------------------
class ClaimRecord(BaseModel):
    """A single claim record — typed and validated."""

    claim_id: str
    member_id: str
    provider_id: str
    facility_id: str | None = None
    payer_product: str
    plan_type: str
    line_of_business: str
    service_date: date
    claim_received_date: date
    paid_date: date | None = None
    claim_type: str
    place_of_service: str
    diagnosis_codes: list[str]
    procedure_code: str
    revenue_code: str | None = None
    billed_amount: float
    allowed_amount: float
    paid_amount: float
    units: int = Field(ge=1)
    network_status: str
    authorization_required: str
    authorization_id: str | None = None
    denial_flag: str
    denial_reason_category: str | None = None
    appeal_flag: str
    grievance_flag: str
    dme_flag: str
    supplier_type: str | None = None
    rendering_npi: str
    billing_npi: str
    geography_state: str
    geography_region: str
    specialty: str


# ---------------------------------------------------------------------------
# Expected Polars schema (column name → Polars dtype string)
# Used by validate.py for schema enforcement.
# ---------------------------------------------------------------------------
EXPECTED_COLUMNS: dict[str, str] = {
    "claim_id": "String",
    "member_id": "String",
    "provider_id": "String",
    "facility_id": "String",
    "payer_product": "String",
    "plan_type": "String",
    "line_of_business": "String",
    "service_date": "Date",
    "claim_received_date": "Date",
    "paid_date": "Date",
    "claim_type": "String",
    "place_of_service": "String",
    "diagnosis_codes": "String",  # stored as JSON string in Parquet
    "procedure_code": "String",
    "revenue_code": "String",
    "billed_amount": "Float64",
    "allowed_amount": "Float64",
    "paid_amount": "Float64",
    "units": "Int64",
    "network_status": "String",
    "authorization_required": "String",
    "authorization_id": "String",
    "denial_flag": "String",
    "denial_reason_category": "String",
    "appeal_flag": "String",
    "grievance_flag": "String",
    "dme_flag": "String",
    "supplier_type": "String",
    "rendering_npi": "String",
    "billing_npi": "String",
    "geography_state": "String",
    "geography_region": "String",
    "specialty": "String",
}

# Columns that must not be null
REQUIRED_COLUMNS: list[str] = [
    "claim_id",
    "member_id",
    "provider_id",
    "payer_product",
    "plan_type",
    "line_of_business",
    "service_date",
    "claim_received_date",
    "claim_type",
    "place_of_service",
    "diagnosis_codes",
    "procedure_code",
    "billed_amount",
    "allowed_amount",
    "paid_amount",
    "units",
    "network_status",
    "authorization_required",
    "denial_flag",
    "appeal_flag",
    "grievance_flag",
    "dme_flag",
    "rendering_npi",
    "billing_npi",
    "geography_state",
    "geography_region",
    "specialty",
]
