"""Synthetic claims data generator.

Produces a Polars DataFrame of synthetic claims with realistic UM patterns:
- Long-tail cost distributions (lognormal + Pareto tail)
- Seasonal utilization patterns
- OON DME fraud-like clusters
- Denial → appeal dynamics
- Policy change simulation via authorization toggles

All generation is seeded for deterministic, reproducible output.
Spec: SR-1
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import numpy as np
import polars as pl

from um_claims.config import PipelineConfig, get_service_category
from um_claims.schema import (
    CLAIM_TYPES,
    DENIAL_REASONS,
    LINES_OF_BUSINESS,
    NETWORK_STATUSES,
    PAYER_PRODUCTS,
    PLACE_OF_SERVICE_CODES,
    PLAN_TYPES,
    PROCEDURE_CODES,
    REGIONS,
    SPECIALTIES,
    STATES_BY_REGION,
)


def _generate_service_dates(
    rng: np.random.Generator,
    n: int,
    start: date,
    end: date,
) -> list[date]:
    """Generate service dates with seasonal weighting.

    Winter months (Dec-Feb) get higher weight (respiratory/ED),
    Summer months get slightly higher elective surgery weight,
    December/January get a dip for elective procedures.
    """
    total_days = (end - start).days
    raw_days = rng.integers(0, total_days, size=n)
    dates = [start + timedelta(days=int(d)) for d in raw_days]

    # Apply seasonal acceptance-rejection to bias toward winter for ~30% of claims
    month_weights = {
        1: 1.15,
        2: 1.10,
        3: 1.00,
        4: 0.95,
        5: 0.95,
        6: 1.00,
        7: 1.00,
        8: 1.00,
        9: 1.05,
        10: 1.10,
        11: 1.05,
        12: 1.15,
    }
    accept_probs = np.array([month_weights[d.month] / 1.15 for d in dates])
    uniform = rng.random(n)
    # For rejected dates, resample toward winter
    for i in range(n):
        if uniform[i] > accept_probs[i]:
            # Resample biased toward winter
            winter_day = rng.choice(
                [rng.integers(0, 59), rng.integers(335, total_days)]  # Jan-Feb or Dec
            )
            dates[i] = start + timedelta(days=int(winter_day % total_days))

    return dates


def _generate_costs(
    rng: np.random.Generator,
    n: int,
    service_categories: list[str],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Generate billed, allowed, paid amounts with long-tail distribution.

    Uses lognormal distribution with category-specific parameters.
    Top 20% of claims should account for >= 70% of total billed.
    """
    # Base lognormal parameters by service category
    category_params: dict[str, tuple[float, float]] = {
        "E&M": (4.5, 0.8),
        "Imaging": (6.0, 1.0),
        "Surgical": (7.5, 1.2),
        "DME": (5.5, 1.5),
        "Pharmacy": (4.0, 1.8),
        "Other": (5.0, 1.0),
    }

    billed = np.zeros(n)
    for i in range(n):
        cat = service_categories[i]
        mu, sigma = category_params.get(cat, (5.0, 1.0))
        billed[i] = rng.lognormal(mu, sigma)

    # Inject Pareto tail for top 5% to ensure long-tail
    top_n = max(1, n // 20)
    top_indices = rng.choice(n, size=top_n, replace=False)
    billed[top_indices] = rng.pareto(1.5, size=top_n) * 50000 + billed[top_indices]

    # Clip to reasonable range
    billed = np.clip(billed, 10.0, 2_000_000.0)

    # Allowed is 40-95% of billed
    allowed_ratio = rng.uniform(0.40, 0.95, size=n)
    allowed = billed * allowed_ratio

    # Paid is 80-100% of allowed (some copay/coinsurance)
    paid_ratio = rng.uniform(0.80, 1.00, size=n)
    paid = allowed * paid_ratio

    return np.round(billed, 2), np.round(allowed, 2), np.round(paid, 2)


def generate_claims(config: PipelineConfig) -> pl.DataFrame:
    """Generate a synthetic claims DataFrame.

    Args:
        config: Pipeline configuration with seed, num_claims, and generation parameters.

    Returns:
        Polars DataFrame conforming to the claims schema.
    """
    rng = np.random.default_rng(config.seed)
    n = config.num_claims

    # --- Member and provider pools ---
    n_members = max(100, n // 10)
    n_providers = max(50, n // 200)
    n_facilities = max(20, n // 500)

    member_ids = [f"MEM-{i:08d}" for i in range(n_members)]
    provider_ids = [f"PROV-{i:06d}" for i in range(n_providers)]
    facility_ids = [f"FAC-{i:05d}" for i in range(n_facilities)]

    # Provider specialty assignment (fixed per provider)
    provider_specialties = {
        pid: rng.choice(SPECIALTIES) for pid in provider_ids
    }

    # Provider geography (fixed per provider)
    provider_regions = {pid: rng.choice(REGIONS) for pid in provider_ids}
    provider_states = {
        pid: rng.choice(STATES_BY_REGION[provider_regions[pid]]) for pid in provider_ids
    }

    # --- Base claim generation ---
    selected_members = rng.choice(member_ids, size=n)
    selected_providers = rng.choice(provider_ids, size=n)
    selected_facilities = [
        rng.choice(facility_ids) if rng.random() > 0.3 else None for _ in range(n)
    ]

    # Claim type distribution: Professional 60%, Institutional 25%, Pharmacy 15%
    claim_type_weights = [0.60, 0.25, 0.15]
    selected_claim_types = rng.choice(CLAIM_TYPES, size=n, p=claim_type_weights)

    # Procedure codes based on claim type
    service_categories_list: list[str] = []
    procedure_codes_list: list[str] = []
    for ct in selected_claim_types:
        if ct == "Pharmacy":
            cat = "Pharmacy"
        else:
            cat = rng.choice(["E&M", "Imaging", "Surgical", "DME", "Other"], p=[0.35, 0.20, 0.15, 0.10, 0.20])
        code = rng.choice(PROCEDURE_CODES[cat])
        service_categories_list.append(cat)
        procedure_codes_list.append(code)

    # Service dates
    service_dates = _generate_service_dates(rng, n, config.date_start, config.date_end)

    # Claim received 1-30 days after service
    claim_received_deltas = rng.integers(1, 31, size=n)
    claim_received_dates = [
        sd + timedelta(days=int(d)) for sd, d in zip(service_dates, claim_received_deltas)
    ]

    # Paid date: 14-90 days after received (null for ~5% pending)
    paid_deltas = rng.integers(14, 91, size=n)
    paid_dates = [
        rd + timedelta(days=int(d)) if rng.random() > 0.05 else None
        for rd, d in zip(claim_received_dates, paid_deltas)
    ]

    # Costs
    billed, allowed, paid_amounts = _generate_costs(rng, n, service_categories_list)

    # Units: mostly 1, sometimes 2-10 for DME/Pharmacy
    units = np.ones(n, dtype=int)
    for i in range(n):
        if service_categories_list[i] in ("DME", "Pharmacy"):
            units[i] = int(rng.integers(1, 11))
        elif rng.random() < 0.1:
            units[i] = int(rng.integers(2, 5))

    # Network status: 90% INN, 10% OON
    network_choices = rng.choice(NETWORK_STATUSES, size=n, p=[0.90, 0.10])

    # Payer product and plan type
    payer_products = rng.choice(PAYER_PRODUCTS, size=n, p=[0.45, 0.25, 0.20, 0.10])
    plan_types = rng.choice(PLAN_TYPES, size=n, p=[0.35, 0.30, 0.20, 0.15])

    # Line of business derived from payer product
    lob_map = {
        "Commercial": ["Group", "Individual"],
        "Medicare": ["Medicare"],
        "Medicaid": ["Medicaid"],
        "Exchange": ["Individual"],
    }
    lobs = [rng.choice(lob_map[pp]) for pp in payer_products]

    # Authorization required: ~25% of claims
    auth_required_arr = rng.choice(["Y", "N"], size=n, p=[0.25, 0.75])

    # Apply policy change — remove auth for imaging after effective_date
    for event in config.policy_events:
        if event.change_type == "removed":
            for i in range(n):
                if (
                    auth_required_arr[i] == "Y"
                    and service_dates[i] >= event.effective_date
                    and any(
                        procedure_codes_list[i].startswith(prefix)
                        for prefix in event.affected_procedure_prefixes
                    )
                ):
                    auth_required_arr[i] = "N"

    # Authorization IDs
    auth_ids = [
        f"AUTH-{rng.integers(100000, 999999)}" if auth_required_arr[i] == "Y" and rng.random() > 0.1 else None
        for i in range(n)
    ]

    # Denial: ~12% base denial rate
    denial_flags = rng.choice(["Y", "N"], size=n, p=[0.12, 0.88])
    denial_reasons = [
        rng.choice(DENIAL_REASONS) if denial_flags[i] == "Y" else None for i in range(n)
    ]
    # Zero out paid for denied claims
    for i in range(n):
        if denial_flags[i] == "Y":
            paid_amounts[i] = 0.0

    # Appeals based on denial reason propensity
    appeal_flags = ["N"] * n
    for i in range(n):
        if denial_flags[i] == "Y" and denial_reasons[i] is not None:
            propensity = config.appeal_propensity.get(denial_reasons[i], 0.10)
            if rng.random() < propensity:
                appeal_flags[i] = "Y"

    # Grievances: ~2% of all claims
    grievance_flags = rng.choice(["Y", "N"], size=n, p=[0.02, 0.98])

    # DME flag derived from service category
    dme_flags = ["Y" if service_categories_list[i] == "DME" else "N" for i in range(n)]

    # Supplier type for DME
    supplier_types = [
        rng.choice(["DME Supplier", "Medical Equipment", "Prosthetics"]) if dme_flags[i] == "Y" else None
        for i in range(n)
    ]

    # NPIs
    rendering_npis = [f"{rng.integers(1000000000, 9999999999)}" for _ in range(n)]
    billing_npis = [f"{rng.integers(1000000000, 9999999999)}" for _ in range(n)]

    # Geography from provider
    states = [provider_states[selected_providers[i]] for i in range(n)]
    regions = [provider_regions[selected_providers[i]] for i in range(n)]

    # Specialties from provider
    specialties = [provider_specialties[selected_providers[i]] for i in range(n)]

    # POS codes
    pos_codes = rng.choice(PLACE_OF_SERVICE_CODES, size=n).tolist()

    # Diagnosis codes: 1-5 synthetic ICD codes per claim
    diagnosis_codes_list = [
        json.dumps([f"DX-{rng.integers(1000, 9999)}" for _ in range(int(rng.integers(1, 6)))])
        for _ in range(n)
    ]

    # Revenue codes for institutional claims
    revenue_codes = [
        f"{rng.integers(100, 999):04d}" if selected_claim_types[i] == "Institutional" else None
        for i in range(n)
    ]

    # Claim IDs
    claim_ids = [f"CLM-{i:010d}" for i in range(n)]

    # --- Build DataFrame ---
    df = pl.DataFrame(
        {
            "claim_id": claim_ids,
            "member_id": selected_members.tolist(),
            "provider_id": selected_providers.tolist(),
            "facility_id": selected_facilities,
            "payer_product": payer_products.tolist(),
            "plan_type": plan_types.tolist(),
            "line_of_business": lobs,
            "service_date": service_dates,
            "claim_received_date": claim_received_dates,
            "paid_date": paid_dates,
            "claim_type": selected_claim_types.tolist(),
            "place_of_service": pos_codes,
            "diagnosis_codes": diagnosis_codes_list,
            "procedure_code": procedure_codes_list,
            "revenue_code": revenue_codes,
            "billed_amount": billed.tolist(),
            "allowed_amount": allowed.tolist(),
            "paid_amount": paid_amounts.tolist(),
            "units": units.tolist(),
            "network_status": network_choices.tolist(),
            "authorization_required": auth_required_arr.tolist(),
            "authorization_id": auth_ids,
            "denial_flag": denial_flags.tolist(),
            "denial_reason_category": denial_reasons,
            "appeal_flag": appeal_flags,
            "grievance_flag": grievance_flags.tolist(),
            "dme_flag": dme_flags,
            "supplier_type": supplier_types,
            "rendering_npi": rendering_npis,
            "billing_npi": billing_npis,
            "geography_state": states,
            "geography_region": regions,
            "specialty": specialties,
        }
    )

    # --- Inject fraud cluster ---
    fraud_df = _inject_fraud_cluster(rng, config)
    df = pl.concat([df, fraud_df])

    return df


def _inject_fraud_cluster(
    rng: np.random.Generator,
    config: PipelineConfig,
) -> pl.DataFrame:
    """Create a cluster of suspicious OON DME suppliers.

    These suppliers have:
    - Very recent entity age (all claims in last 90 days of date range)
    - High OON rate (95%+)
    - Concentrated procedure codes (1-2 codes only)
    - Concentrated geography (all same state)
    - High volume per supplier
    """
    n_suppliers = config.fraud_cluster_supplier_count
    claims_per = config.fraud_cluster_claims_per_supplier
    total = n_suppliers * claims_per

    # Recent dates only — last 90 days
    fraud_start = config.date_end - timedelta(days=90)
    days_range = (config.date_end - fraud_start).days

    records: dict[str, list] = {col: [] for col in [
        "claim_id", "member_id", "provider_id", "facility_id",
        "payer_product", "plan_type", "line_of_business",
        "service_date", "claim_received_date", "paid_date",
        "claim_type", "place_of_service", "diagnosis_codes",
        "procedure_code", "revenue_code",
        "billed_amount", "allowed_amount", "paid_amount", "units",
        "network_status", "authorization_required", "authorization_id",
        "denial_flag", "denial_reason_category",
        "appeal_flag", "grievance_flag", "dme_flag", "supplier_type",
        "rendering_npi", "billing_npi",
        "geography_state", "geography_region", "specialty",
    ]}

    # Suspicious suppliers use 1-2 concentrated DME codes
    fraud_codes = ["HCPCS-E0100", "HCPCS-E0101"]
    fraud_state = "FL"
    fraud_region = "Southeast"

    for s in range(n_suppliers):
        supplier_id = f"FRAUD-PROV-{s:04d}"
        npi = f"{rng.integers(1000000000, 9999999999)}"

        for c in range(claims_per):
            sd = fraud_start + timedelta(days=int(rng.integers(0, days_range)))
            rd = sd + timedelta(days=int(rng.integers(1, 15)))
            pd_val = rd + timedelta(days=int(rng.integers(14, 60)))

            # High billed amounts
            billed_val = round(float(rng.lognormal(7.0, 0.8)), 2)
            allowed_val = round(billed_val * float(rng.uniform(0.60, 0.90)), 2)
            paid_val = round(allowed_val * float(rng.uniform(0.85, 1.0)), 2)

            records["claim_id"].append(f"CLM-FRAUD-{s:04d}-{c:05d}")
            records["member_id"].append(f"MEM-{rng.integers(0, 10000):08d}")
            records["provider_id"].append(supplier_id)
            records["facility_id"].append(None)
            records["payer_product"].append(rng.choice(["Commercial", "Medicare"]))
            records["plan_type"].append("PPO")
            records["line_of_business"].append("Group")
            records["service_date"].append(sd)
            records["claim_received_date"].append(rd)
            records["paid_date"].append(pd_val)
            records["claim_type"].append("Professional")
            records["place_of_service"].append("12")  # Home
            records["diagnosis_codes"].append(json.dumps([f"DX-{rng.integers(8000, 8999)}"]))
            records["procedure_code"].append(rng.choice(fraud_codes))
            records["revenue_code"].append(None)
            records["billed_amount"].append(billed_val)
            records["allowed_amount"].append(allowed_val)
            records["paid_amount"].append(paid_val)
            records["units"].append(int(rng.integers(3, 15)))
            # 95% OON
            records["network_status"].append("OON" if rng.random() < 0.95 else "INN")
            records["authorization_required"].append("N")
            records["authorization_id"].append(None)
            records["denial_flag"].append("N")
            records["denial_reason_category"].append(None)
            records["appeal_flag"].append("N")
            records["grievance_flag"].append("N")
            records["dme_flag"].append("Y")
            records["supplier_type"].append("DME Supplier")
            records["rendering_npi"].append(npi)
            records["billing_npi"].append(npi)
            records["geography_state"].append(fraud_state)
            records["geography_region"].append(fraud_region)
            records["specialty"].append("DME Supplier")

    return pl.DataFrame(records)
