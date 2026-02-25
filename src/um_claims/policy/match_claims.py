"""Deterministic claim-to-policy matching.

Scores each claim against every policy rule and assigns the best match.

Scoring:
  +0.6 if ProcedureCode matches any of the policy's covered_cpt_codes
  +0.2 if ClaimType matches site_of_service (when present)
  +0.2 if any DiagnosisCode is in diagnosis_constraints (when present)

Spec: Analytics — Match claims to policies
"""

from __future__ import annotations

import json


def match_claim_to_policy(claim_row: dict, policy_rule: dict) -> float:
    """Score how well a single claim matches a single policy rule.

    Args:
        claim_row: Dictionary with at least ``procedure_code``,
            ``claim_type``, and ``diagnosis_codes`` keys.
        policy_rule: Parsed policy dictionary with ``covered_cpt_codes``,
            ``site_of_service``, and ``diagnosis_constraints``.

    Returns:
        A confidence score in [0.0, 1.0].
    """
    score = 0.0

    # --- CPT match (+0.6) ---
    covered = set(policy_rule.get("covered_cpt_codes") or [])
    if covered and claim_row.get("procedure_code") in covered:
        score += 0.6

    # --- Site-of-service / ClaimType match (+0.2) ---
    site = (policy_rule.get("site_of_service") or "").strip()
    if site and claim_row.get("claim_type") == site:
        score += 0.2

    # --- Diagnosis match (+0.2) ---
    dx_constraints = set(policy_rule.get("diagnosis_constraints") or [])
    if dx_constraints:
        raw_dx = claim_row.get("diagnosis_codes", "[]")
        if isinstance(raw_dx, str):
            try:
                dx_list = json.loads(raw_dx)
            except (json.JSONDecodeError, TypeError):
                dx_list = []
        else:
            dx_list = list(raw_dx)
        if dx_constraints & set(dx_list):
            score += 0.2

    return score


def match_claims_to_policies(
    claims: list[dict],
    policies: list[dict],
) -> list[dict]:
    """Match every claim to its best policy.

    Each returned dict has the original claim keys plus:
      - ``policy_id``: best-matching policy id, or ``"unmatched"``
      - ``match_confidence``: float score of the best match

    Args:
        claims: List of claim row dicts.
        policies: List of parsed policy rule dicts.

    Returns:
        List of claim dicts augmented with match info.
    """
    results: list[dict] = []
    for claim in claims:
        best_id = "unmatched"
        best_score = 0.0
        for policy in policies:
            s = match_claim_to_policy(claim, policy)
            if s > best_score:
                best_score = s
                best_id = policy.get("policy_id", "unknown")
        row = dict(claim)
        row["policy_id"] = best_id
        row["match_confidence"] = best_score
        results.append(row)
    return results
