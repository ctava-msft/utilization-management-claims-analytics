# Specifications — UM Claims Analytics

> System-level requirements, user stories, acceptance criteria, and success metrics.
> Technology-agnostic where possible; implementation choices are deferred to PLAN.

---

## 1. Stakeholder Roles

| Role | Description |
|---|---|
| **UM Analyst** | Primary user; reviews reports, investigates anomalies, evaluates policy effectiveness |
| **UM Director** | Consumes summary dashboards; makes policy change decisions |
| **SIU Investigator** | Reviews fraud/waste/abuse flags for further investigation |
| **Data Engineer** | Maintains pipeline, extends schema, adds new data sources |

---

## 2. System Requirements

### SR-1: Synthetic Data Generation
The system shall generate synthetic claims data that mimics real-world UM patterns without containing PHI.

**Sub-requirements:**
- SR-1.1: Generate claims with all required schema fields (see Claims Schema below).
- SR-1.2: Produce long-tail cost distributions (Pareto-like: ~20% of claims account for ~80% of cost).
- SR-1.3: Embed seasonal utilization patterns (higher ED/respiratory in winter, elective surgery dips in Dec/Jan).
- SR-1.4: Generate OON DME fraud-like clusters (sudden volume spikes, geographic anomalies, repeated procedure codes from new suppliers).
- SR-1.5: Simulate denial→appeal dynamics with configurable appeal propensity by denial reason.
- SR-1.6: Simulate policy change events (authorization requirement toggled off for selected services at a specific date).
- SR-1.7: All generation must be seeded and deterministic.
- SR-1.8: Default dataset size: 100,000 claims; configurable via CLI up to 10M+.

### SR-2: Data Validation
The system shall validate all ingested data against a strict schema before processing.

**Sub-requirements:**
- SR-2.1: Validate column presence, data types, and nullable constraints.
- SR-2.2: Validate value ranges (e.g., amounts ≥ 0, dates within expected bounds).
- SR-2.3: Validate referential consistency (e.g., denial_reason required when denial_flag=Y).
- SR-2.4: Validate statistical distributions (e.g., warn if billed_amount has zero variance).
- SR-2.5: Produce a validation report listing all failures with row counts and examples.
- SR-2.6: Fail the pipeline if critical validations fail; warn on advisory validations.

### SR-3: Feature Engineering
The system shall compute UM-relevant features from validated claims.

**Sub-requirements:**
- SR-3.1: Aggregate claims by provider, facility, service category, and time period.
- SR-3.2: Compute per-provider metrics: total allowed, average units, claim frequency, OON rate, denial rate.
- SR-3.3: Compute temporal features: weekly/monthly volumes, rolling 4-week averages, YoY comparisons.
- SR-3.4: Compute "entity age" features: days since first claim for each provider/supplier.
- SR-3.5: Compute cost-per-unit and allowed-to-billed ratio features.
- SR-3.6: Tag service categories from procedure codes (e.g., DME, imaging, E&M, surgical).

### SR-4: Outlier and Anomaly Detection
The system shall identify providers, suppliers, and utilization patterns that deviate from expected norms.

**Sub-requirements:**
- SR-4.1: Flag providers whose allowed amount or claim volume exceeds ±2σ from peer group mean.
- SR-4.2: Flag "new entity" suppliers (< 90 days of history) with volumes exceeding the 90th percentile of established suppliers.
- SR-4.3: Flag OON DME clusters: suppliers with >80% OON rate AND high volume AND concentrated procedure codes.
- SR-4.4: Flag billing anomalies: billed-to-allowed ratio > 3x peer median.
- SR-4.5: Every flag must include: rule_name, entity_id, feature_values (dict), threshold, actual_value, severity (high/medium/low).

### SR-5: Policy Effectiveness Analysis
The system shall compare utilization before and after simulated policy changes.

**Sub-requirements:**
- SR-5.1: Define policy change events with: policy_id, affected_services, change_type (added/removed), effective_date.
- SR-5.2: Compute pre/post metrics: volume, cost, denial rate, OON rate for affected services.
- SR-5.3: Detect "rebound" patterns: services where utilization returns to pre-change levels within N weeks after a policy removal.
- SR-5.4: Produce a policy impact summary with effect sizes and confidence indicators.

### SR-6: Appeals and Grievances Analytics
The system shall analyze denial-to-appeal funnels and identify administrative burden hotspots.

**Sub-requirements:**
- SR-6.1: Compute denial→appeal conversion rates by denial_reason_category.
- SR-6.2: Identify top denial categories by volume and by appeal rate.
- SR-6.3: Estimate admin cost proxy: appeals count × configurable cost-per-appeal.
- SR-6.4: Identify providers or third-party patterns with disproportionate appeal volumes.

### SR-7: Benchmarking
The system shall provide structure for comparing internal metrics to peer baselines.

**Sub-requirements:**
- SR-7.1: Accept synthetic peer baseline inputs (configurable per metric).
- SR-7.2: Compute variance-to-benchmark for key metrics (denial rate, cost-per-member, OON rate).
- SR-7.3: Flag metrics that exceed benchmark thresholds.

### SR-8: Reporting
The system shall produce structured, human-readable reports.

**Sub-requirements:**
- SR-8.1: Generate a summary report (Markdown and/or HTML) per pipeline run.
- SR-8.2: Report includes: run metadata, key metrics, top anomalies (ranked by severity), policy impact summaries, and recommended next questions.
- SR-8.3: Generate visualizations: cost distributions, utilization trend lines with control bands, denial funnel charts.
- SR-8.4: All outputs written to a timestamped output directory.

### SR-9: CLI Interface
The system shall expose a CLI with discrete commands for each pipeline stage.

**Sub-requirements:**
- SR-9.1: Commands: `generate-data`, `validate`, `process`, `detect`, `report`, `run-all`.
- SR-9.2: Common options: `--seed`, `--num-claims`, `--output-dir`, `--config`.
- SR-9.3: Each command is independently runnable; `run-all` chains them.
- SR-9.4: Exit codes: 0 = success, 1 = validation failure, 2 = runtime error.

---

## 3. Claims Schema (Logical)

| Field | Type | Nullable | Constraints |
|---|---|---|---|
| claim_id | string | No | Unique |
| member_id | string | No | Synthetic UUID prefix |
| provider_id | string | No | |
| facility_id | string | Yes | |
| payer_product | string | No | Enum: Commercial, Medicare, Medicaid, Exchange |
| plan_type | string | No | Enum: HMO, PPO, POS, EPO |
| line_of_business | string | No | Enum: Group, Individual, Medicare, Medicaid |
| service_date | date | No | 2023-01-01 to 2025-12-31 |
| claim_received_date | date | No | ≥ service_date |
| paid_date | date | Yes | ≥ claim_received_date |
| claim_type | string | No | Enum: Professional, Institutional, Pharmacy |
| place_of_service | string | No | 2-digit POS code |
| diagnosis_codes | list[string] | No | 1-5 synthetic ICD-like codes |
| procedure_code | string | No | Synthetic CPT/HCPCS-like |
| revenue_code | string | Yes | 4-digit |
| billed_amount | float | No | > 0 |
| allowed_amount | float | No | ≥ 0, ≤ billed_amount |
| paid_amount | float | No | ≥ 0, ≤ allowed_amount |
| units | int | No | ≥ 1 |
| network_status | string | No | Enum: INN, OON |
| authorization_required | string | No | Enum: Y, N |
| authorization_id | string | Yes | Present when authorization_required=Y (may still be null) |
| denial_flag | string | No | Enum: Y, N |
| denial_reason_category | string | Yes | Required when denial_flag=Y |
| appeal_flag | string | No | Enum: Y, N |
| grievance_flag | string | No | Enum: Y, N |
| dme_flag | string | No | Enum: Y, N |
| supplier_type | string | Yes | |
| rendering_npi | string | No | 10-digit synthetic |
| billing_npi | string | No | 10-digit synthetic |
| geography_state | string | No | 2-letter state code |
| geography_region | string | No | Enum: Northeast, Southeast, Midwest, West |
| specialty | string | No | Synthetic specialty name |

---

## 4. User Stories

### US-1: Generate Synthetic Data
**As a** Data Engineer, **I want to** generate a configurable volume of synthetic claims data with realistic UM patterns, **so that** analysts can develop and test analytics without PHI risk.

**Acceptance Criteria:**
- AC-1.1: Running `um-claims generate-data --num-claims 100000 --seed 42` produces a Parquet file with exactly 100,000 rows.
- AC-1.2: Re-running with the same seed produces byte-identical output.
- AC-1.3: Cost distribution is long-tailed (top 20% of claims ≥ 70% of total billed).
- AC-1.4: At least one identifiable OON DME cluster exists in the output.
- AC-1.5: At least one policy change event is embedded with a clear effective date.

### US-2: Validate Data Quality
**As a** UM Analyst, **I want to** validate that claims data meets schema and quality requirements before analysis, **so that** downstream results are trustworthy.

**Acceptance Criteria:**
- AC-2.1: Valid data passes validation with exit code 0.
- AC-2.2: Data with a missing required column fails with a clear error message naming the column.
- AC-2.3: Data with denial_flag=Y and null denial_reason fails referential validation.
- AC-2.4: Validation report is written to the output directory.

### US-3: Identify Provider Outliers
**As an** SIU Investigator, **I want to** see providers flagged as statistical outliers by cost, volume, or billing patterns, **so that** I can prioritize reviews.

**Acceptance Criteria:**
- AC-3.1: Flags include rule_name, entity_id, feature_values, threshold, actual_value, severity.
- AC-3.2: Known synthetic outlier providers appear in the flagged list.
- AC-3.3: Flags are ranked by severity in the report.

### US-4: Detect OON DME Fraud Patterns
**As an** SIU Investigator, **I want to** detect clusters of OON DME suppliers with suspicious billing patterns, **so that** I can investigate potential fraud schemes.

**Acceptance Criteria:**
- AC-4.1: The synthetic fraud cluster is detected and flagged.
- AC-4.2: Flags explain: supplier_id, OON rate, volume, procedure concentration, entity age.
- AC-4.3: Non-suspicious DME suppliers are not flagged (low false-positive rate on synthetic data).

### US-5: Evaluate Policy Changes
**As a** UM Director, **I want to** see the impact of policy changes on utilization and cost, **so that** I can decide whether to keep, modify, or revert policies.

**Acceptance Criteria:**
- AC-5.1: Pre/post comparison table shows volume and cost changes for affected services.
- AC-5.2: Rebound detection identifies services returning to pre-change levels.
- AC-5.3: Effect size and direction are clearly labeled.

### US-6: Analyze Appeal Burden
**As a** UM Analyst, **I want to** understand which denial categories drive the most appeals, **so that** I can recommend process improvements.

**Acceptance Criteria:**
- AC-6.1: Denial→appeal funnel shows conversion rate by category.
- AC-6.2: Top 5 categories by appeal volume are highlighted.
- AC-6.3: Estimated admin cost is computed.

### US-7: Benchmark Against Peers
**As a** UM Director, **I want to** compare our utilization metrics against peer baselines, **so that** I can identify areas of overuse or underuse.

**Acceptance Criteria:**
- AC-7.1: Benchmark comparison table shows internal rate, baseline rate, and variance.
- AC-7.2: Metrics exceeding threshold are flagged.

### US-8: Run Full Pipeline
**As a** Data Engineer, **I want to** run the entire pipeline end-to-end with a single command, **so that** I can produce a complete report.

**Acceptance Criteria:**
- AC-8.1: `um-claims run-all` executes generate → validate → process → detect → report.
- AC-8.2: All outputs are in a single timestamped directory.
- AC-8.3: Exit code reflects the worst stage outcome.

---

## 5. Success Metrics

| Metric | Target | Measurement |
|---|---|---|
| Synthetic data realism | Long-tail distribution validated; seasonal patterns visible in trend plots | Automated distribution checks in tests |
| Detection recall (synthetic) | 100% of planted fraud clusters detected | Test with known-planted anomalies |
| Detection precision (synthetic) | < 10% false-positive rate on synthetic data | Count non-planted entities flagged |
| Validation coverage | 100% of schema fields checked; referential rules enforced | Count of validation rules vs schema fields |
| Test coverage | ≥ 80% line coverage on core modules | pytest-cov report |
| Pipeline reproducibility | Identical outputs across 2 runs with same seed | Byte-level diff of outputs |
| Report actionability | Every flagged item has rule + evidence + severity | Manual review of sample report |
| Performance | 1M claims processed in < 60s on commodity hardware | Timed run |

---

## 6. Assumptions

- Synthetic procedure codes follow a simplified coding scheme (e.g., `CPT-XXXXX`) rather than real CPT codes, to avoid copyright concerns.
- Diagnosis codes are synthetic ICD-like codes (e.g., `DX-XXXX`).
- Place-of-service codes use real 2-digit CMS POS codes (public domain).
- Peer benchmarks are synthetic; in production, these would come from CMS or proprietary databases.
- The "admin cost per appeal" is a configurable constant, not derived from real operational data.
- Policy simulation is a simplified toggle model; real UM policies have complex eligibility rules.

## 7. Out of Scope

- Real-time or streaming data processing.
- Member-level risk stratification or predictive modeling.
- Integration with claims adjudication or authorization systems.
- HIPAA compliance infrastructure (because no PHI exists).
- Multi-tenant or role-based access control.
- Cloud deployment or infrastructure provisioning.
