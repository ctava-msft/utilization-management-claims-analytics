"""Policy KPI roll-ups.

Groups matched claims by ``policy_id`` and computes per-policy metrics:
  - n_claims, total_amount, avg_amount
  - approval_rate, denial_rate
  - top_dx, top_specialties

Spec: Analytics — Policy KPI roll-ups
"""

from __future__ import annotations

import json
from collections import Counter


def compute_policy_kpis(
    matched_claims: list[dict],
) -> list[dict]:
    """Compute KPI roll-ups grouped by ``policy_id``.

    Args:
        matched_claims: Claim dicts that include ``policy_id`` (as produced by
            :func:`um_claims.policy.match_claims.match_claims_to_policies`).

    Returns:
        List of KPI dicts sorted by ``total_amount`` descending.  Each dict
        contains:

        - ``policy_id``
        - ``n_claims``
        - ``total_amount``
        - ``avg_amount``
        - ``approval_rate``
        - ``denial_rate``
        - ``top_dx`` (list of up to 5 most-common diagnosis codes)
        - ``top_specialties`` (list of up to 5 most-common specialties)
    """
    # Group claims by policy_id
    groups: dict[str, list[dict]] = {}
    for claim in matched_claims:
        pid = claim.get("policy_id", "unmatched")
        groups.setdefault(pid, []).append(claim)

    kpis: list[dict] = []
    for policy_id, claims in groups.items():
        n = len(claims)

        # Amounts — use allowed_amount when present, fall back to billed_amount
        amounts = [
            c.get("allowed_amount") or c.get("billed_amount") or 0.0
            for c in claims
        ]
        total_amount = sum(amounts)
        avg_amount = total_amount / n if n else 0.0

        # Approval / denial
        denial_count = sum(1 for c in claims if c.get("denial_flag") == "Y")
        approval_count = n - denial_count
        approval_rate = approval_count / n if n else 0.0
        denial_rate = denial_count / n if n else 0.0

        # Top Dx
        dx_counter: Counter[str] = Counter()
        for c in claims:
            raw = c.get("diagnosis_codes", "[]")
            if isinstance(raw, str):
                try:
                    codes = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    codes = []
            else:
                codes = list(raw)
            for code in codes:
                dx_counter[code] += 1
        top_dx = [code for code, _ in dx_counter.most_common(5)]

        # Top specialties
        spec_counter: Counter[str] = Counter()
        for c in claims:
            spec = c.get("specialty")
            if spec:
                spec_counter[spec] += 1
        top_specialties = [s for s, _ in spec_counter.most_common(5)]

        kpis.append(
            {
                "policy_id": policy_id,
                "n_claims": n,
                "total_amount": round(total_amount, 2),
                "avg_amount": round(avg_amount, 2),
                "approval_rate": round(approval_rate, 4),
                "denial_rate": round(denial_rate, 4),
                "top_dx": top_dx,
                "top_specialties": top_specialties,
            }
        )

    # Sort by total_amount descending by default
    kpis.sort(key=lambda k: k["total_amount"], reverse=True)
    return kpis
