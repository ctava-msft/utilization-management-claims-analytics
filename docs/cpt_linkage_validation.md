# CPT-to-Policy Linkage Validation — Results

**Date:** 2026-02-25
**Pipeline:** `um-claims run-all --seed 42 --num-claims 1000`
**Dataset:** Synthetic (1,750 claims generated, 186 unique CPT codes)

---

## Overview

This document records the results of validating that CPT/procedure-code values
propagate correctly through every stage of the UM Claims Analytics pipeline:

```
raw claims → validation → feature engineering → policy simulation → report
```

**Outcome:** All checks passed. No CPT codes are dropped silently, and
denial/approval rates are correctly aggregated at CPT and CPT-category levels.

---

## Stage-by-Stage Results

### Stage 1 — Ingestion (`raw_claims.parquet`)

| Metric | Value |
|---|---|
| Total rows | 1,750 |
| Unique CPT codes | 186 |
| Null `procedure_code` values | 0 |

Top CPT codes by frequency:

| CPT Code | Count |
|---|---|
| HCPCS-E0100 | 395 |
| HCPCS-E0101 | 358 |
| CPT-99207 | 34 |
| CPT-99214 | 28 |
| CPT-99211 | 28 |

**Result:** ✅ PASS — `procedure_code` column present, no nulls.

---

### Stage 2 — Feature Engineering (`enriched_claims.parquet`)

| Metric | Value |
|---|---|
| Total rows | 1,750 |
| Unique CPT codes | 186 |
| Null `procedure_code` values | 0 |
| CPTs in raw but NOT in enriched (dropped) | 0 |
| CPTs in enriched but NOT in raw (added) | 0 |
| Row count change | 0 (1,750 → 1,750) |

Every CPT maps to exactly one `service_category` (no ambiguous mappings).

**Result:** ✅ PASS — zero CPTs dropped, zero nulls introduced.

---

### Stage 2b — Service Category Tagging

| Service Category | Claims | Unique CPTs | Denial Rate | OON Rate |
|---|---|---|---|---|
| DME | 832 | 29 | 0.0132 | 0.8666 |
| E&M | 328 | 15 | 0.1280 | 0.0884 |
| Other | 319 | 65 | 0.1003 | 0.0815 |
| Imaging | 159 | 30 | 0.0943 | 0.0881 |
| Surgical | 112 | 47 | 0.1607 | 0.1429 |

- Total claims across categories: 1,750 (matches enriched claim count exactly).
- All 186 unique CPTs correctly mapped via `config.get_service_category()`.
- Recomputed service-category features match saved `service_category_features.parquet`.

**Result:** ✅ PASS — denial/approval rates aggregated at CPT-category level, no claims lost.

---

### Stage 2c — Provider Features (`provider_features.parquet`)

| Metric | Value |
|---|---|
| Total providers | 55 |
| Providers with 0 unique CPTs | 0 |
| `unique_procedure_codes` column present | Yes |

**Result:** ✅ PASS — all providers have ≥ 1 unique procedure code.

---

### Stage 3 — Policy Simulation (`policy_impact.json`)

| Field | Value |
|---|---|
| Policy | POL-001 |
| Description | Removed prior auth requirement for imaging services |
| Affected CPT prefixes | `["CPT-7"]` |
| Raw CPTs matching prefix | 30 |
| Pre-period volume | 12 |
| Post-period volume | 15 |
| Volume change | +25.0% |
| Pre-denial rate | 0.1667 |
| Post-denial rate | 0.1333 |
| Rebound detected | Yes |

- Policy simulation filters claims by `procedure_code.starts_with("CPT-7")`.
- 159 / 1,750 claims matched the imaging prefix across all time periods.

**Result:** ✅ PASS — policy simulation correctly references CPT/procedure fields.

---

### Stage 4 — Detection Flags (`flags.json`)

| Rule | Count |
|---|---|
| `high_volume_provider` | 5 |
| `high_cost_provider` | 2 |
| `new_entity_high_volume` | 5 |
| `oon_dme_cluster` | 5 |
| **Total** | **17** |

DME cluster flags correctly reference `unique_procedure_codes` in their
`feature_values` (e.g., `unique_codes=2, oon_rate=0.947`).

**Result:** ✅ PASS

---

### Stage 5 — Report (`report.md`)

| Check | Present |
|---|---|
| "Denial Rate" | Yes |
| "Policy Impact" section | Yes |
| CPT/HCPCS code references | Indirect (via policy prefix) |
| Report size | 5,731 chars |

**Result:** ✅ PASS

---

### Stage 6 — Policy Seeds (cross-stage grouping)

| Metric | Value |
|---|---|
| Grouping keys | `(procedure_code, claim_type, specialty)` |
| Unique groups in enriched claims | 801 |
| Clusters in policy seeds (min_claims=1) | 801 |
| `denial_rate + approval_rate ≈ 1.0` | Yes (all sampled) |

- Every unique `(CPT, claim_type, specialty)` triple in enriched claims appears
  in the seed output — zero groups lost.
- Per-seed denial and approval rates sum to 1.0 (±0.01).

**Result:** ✅ PASS — all CPT groups preserved, rates consistent.

---

## Cross-Stage Propagation Summary

```
raw_claims (186 CPTs, 1750 rows)
  │
  ├─ validation ────────── 186 CPTs, 0 nulls              ✅
  │
  ├─ enriched_claims ───── 186 CPTs, 1750 rows, 0 dropped ✅
  │   ├─ service_category_features ── 5 categories, 1750 total ✅
  │   ├─ provider_features ────────── 55 providers, all >0 CPTs ✅
  │   └─ temporal_features ────────── weekly/monthly aggregates  ✅
  │
  ├─ policy_sim ────────── CPT-7 prefix → 159 claims, 30 CPTs  ✅
  │
  ├─ policy_seeds ──────── 801 clusters, 186 CPTs preserved     ✅
  │
  └─ report.md ─────────── denial rates, policy impact present  ✅
```

---

## Debug Logging Added

The following `logger.debug()` calls were added to confirm CPT propagation at
runtime. They are debug-level only and do not affect normal pipeline operation.

| Module | Log Message |
|---|---|
| `features.py` | CPT propagation: input/enriched unique counts, null count, row counts |
| `policy_sim.py` | Per-policy prefix match: prefixes, matched claim count, unique CPTs |
| `policy_seeds.py` | Seed clustering: input CPTs → seed CPTs, cluster count |

---

## Bugs Found

**None.** The CPT-to-policy linkage is correct across all pipeline stages.
