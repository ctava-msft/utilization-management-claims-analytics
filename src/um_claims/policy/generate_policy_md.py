"""Synthetic PA policy Markdown generator.

Generates structured Markdown policy documents from policy_seeds rows.
Generation is purely template-based and deterministic — no external dependencies.

Spec: Feature — Generate synthetic PA policy Markdown from claims-derived seeds
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

# Fixed synthetic effective date so output is deterministic.
_EFFECTIVE_DATE = "2024-01-01"


def _sanitize(value: str) -> str:
    """Replace non-alphanumeric characters with underscores for file naming."""
    return re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_")


def _parse_top_dx(top_diagnosis_codes: str | list) -> list[dict]:
    """Return a list of ``{"code": str, "count": int}`` dicts from various input types."""
    if isinstance(top_diagnosis_codes, list):
        return top_diagnosis_codes
    if isinstance(top_diagnosis_codes, str):
        try:
            parsed = json.loads(top_diagnosis_codes)
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
    return []


def generate_policy_markdown(seed_row: dict) -> str:
    """Generate a structured Markdown policy document from a policy seed row.

    The output is deterministic for a given ``seed_row`` — no timestamps or
    random values are introduced.

    Args:
        seed_row: A dictionary representing one row from the policy seeds
            DataFrame (or JSONL).  Expected keys:

            - ``procedure_code`` (str)
            - ``claim_type`` (str)
            - ``specialty`` (str)
            - ``n_claims`` (int)
            - ``approval_rate`` (float, 0–1)
            - ``denial_rate`` (float, 0–1)
            - ``avg_claim_amount`` (float)
            - ``p50_claim_amount`` (float)
            - ``p90_claim_amount`` (float)
            - ``top_diagnosis_codes`` (JSON str or list of ``{"code", "count"}``)

    Returns:
        A Markdown string containing the full policy document.
    """
    procedure_code: str = seed_row.get("procedure_code", "UNKNOWN")
    claim_type: str = seed_row.get("claim_type", "UNKNOWN")
    specialty: str = seed_row.get("specialty", "UNKNOWN")
    n_claims: int = int(seed_row.get("n_claims", 0))
    approval_rate: float = float(seed_row.get("approval_rate", 0.0))
    denial_rate: float = float(seed_row.get("denial_rate", 0.0))
    avg_amount: float = float(seed_row.get("avg_claim_amount", 0.0))
    p50_amount: float = float(seed_row.get("p50_claim_amount", 0.0))
    p90_amount: float = float(seed_row.get("p90_claim_amount", 0.0))
    top_dx: list[dict] = _parse_top_dx(seed_row.get("top_diagnosis_codes", []))

    top_dx_codes = [entry["code"] for entry in top_dx if "code" in entry]
    top_dx_list = "\n".join(f"  - {code}" for code in top_dx_codes) if top_dx_codes else "  - N/A"

    criteria_lines: list[str] = []
    if top_dx_codes:
        codes_inline = ", ".join(top_dx_codes)
        criteria_lines.append(
            f"- Requires documentation of Dx in [{codes_inline}]."
        )
    if denial_rate > 0:
        denial_pct = round(denial_rate * 100, 1)
        criteria_lines.append(
            f"- Prior authorization required; historical denial rate is {denial_pct}%."
        )
    if approval_rate >= 0.9:
        criteria_lines.append(
            "- Service is routinely approved when clinical criteria are met."
        )
    criteria_lines.append(
        "- Medical necessity must be established by the treating provider."
    )
    criteria_block = "\n".join(f"{line}" for line in criteria_lines)

    doc = f"""\
# Prior Authorization Policy: {procedure_code}

**Specialty:** {specialty}
**Site of Service (ClaimType):** {claim_type}
**Effective Date:** {_EFFECTIVE_DATE}

---

## 1. Covered Services

This policy applies to procedure code **{procedure_code}** when rendered by a
**{specialty}** provider in a **{claim_type}** setting.

---

## 2. Authorization Criteria

{criteria_block}

---

## 3. Diagnosis Context

The following diagnosis codes were most frequently associated with
**{procedure_code}** claims in the reference population (n={n_claims:,}):

{top_dx_list}

---

## 4. Documentation Requirements

Submitting providers must include:

- Clinical notes supporting medical necessity.
- Diagnosis code(s) from the list above, where applicable.
- Any relevant imaging, lab, or specialist reports.

---

## 5. Cost Reference

| Metric              | Value        |
|---------------------|-------------|
| Average Claim Amount | ${avg_amount:,.2f} |
| Median Claim Amount  | ${p50_amount:,.2f} |
| 90th Pct Claim Amount| ${p90_amount:,.2f} |
| Approval Rate        | {round(approval_rate * 100, 1)}% |
| Denial Rate          | {round(denial_rate * 100, 1)}% |

---

*This policy is synthetically generated from claims-derived seeds for
analytical purposes only.*
"""
    return doc


def write_policies(
    seeds_df: pl.DataFrame,
    out_dir: str | Path = "output/policies",
) -> list[Path]:
    """Write one Markdown policy file per row in ``seeds_df``.

    File naming convention::

        POL_{ProcedureCode}_{ClaimType}_{ProviderSpecialty}.md

    where each component is sanitized (non-alphanumeric characters replaced
    with underscores).

    Args:
        seeds_df: Policy seeds DataFrame from
            :func:`um_claims.policy_seeds.build_policy_seeds`.
        out_dir: Directory to write ``.md`` files into.

    Returns:
        Sorted list of :class:`~pathlib.Path` objects for written files.
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for row in seeds_df.to_dicts():
        procedure_code: str = str(row.get("procedure_code", "UNKNOWN"))
        claim_type: str = str(row.get("claim_type", "UNKNOWN"))
        specialty: str = str(row.get("specialty", "UNKNOWN"))

        filename = (
            f"POL_{_sanitize(procedure_code)}"
            f"_{_sanitize(claim_type)}"
            f"_{_sanitize(specialty)}.md"
        )
        file_path = out_path / filename
        file_path.write_text(generate_policy_markdown(row), encoding="utf-8")
        written.append(file_path)

    return sorted(written)
