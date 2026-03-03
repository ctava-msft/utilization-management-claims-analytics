# Specification — POC (Approach A)

> Personas, use cases, and acceptance criteria for the **POC deployment** of the UM Claims Analytics platform.
> The POC runs on a single Azure VM where a **solution architect / solution engineer** manually drives the Python CLI pipeline.

---

## 1. Personas

| Persona | Role | How They Interact |
|---|---|---|
| **Solution Architect** | Primary operator. Runs the Python CLI on the VM, reviews LLM-generated summaries, validates flags, and iterates on pipeline configuration. | SSH/RDP into the VM via Azure Bastion → runs `um-claims run-all` → reviews Markdown reports and JSON flag files on the VM filesystem. |
| **Solution Engineer** | Extends the pipeline — adds new detection rules, adapts schema mappings for UPMC data shapes, tunes thresholds. Tests changes locally before pushing to the VM. | Develops locally or on the VM → runs `pytest` → pushes updates → re-runs the pipeline to validate. |
| **Data Engineer (UPMC)** | Prepares the de-identified claims extract in Snowflake and configures the OneLake shortcut (Option A) or ADF pipeline (Option B). | Works in Snowflake / ADF → validates data lands in the Fabric Lakehouse → hands off to the solution architect. |
| **UM Analyst (UPMC)** | Consumes the output reports and flags produced by the solution architect. Provides domain feedback on flag relevance and threshold tuning. | Reviews Markdown reports / JSON files shared by the solution architect. May also view Lakehouse tables in Power BI (if available). |
| **UM Director (UPMC)** | Reviews high-level POC findings to decide whether to proceed to Production. | Reads executive summary produced by the solution architect; reviews sample dashboards if Power BI is connected. |

---

## 2. Use Cases

### UC-POC-1: Run Full Analytics Pipeline

**Actor:** Solution Architect

**Preconditions:**
- De-identified claims data has been landed in the Fabric Lakehouse (via Option A or B).
- Structured policy JSON has been ingested into the Lakehouse.
- Azure OpenAI deployment (GPT-5.2-chat) is provisioned with `temperature=0` and a fixed `seed`.

**Flow:**
1. Solution architect connects to the Azure VM via Bastion.
2. Runs `um-claims run-all --seed 42 --output-dir ./output`.
3. The CLI reads de-identified claims and policy data from the Fabric Lakehouse.
4. Feature engineering computes per-provider, per-facility, and temporal metrics.
5. Detection rules flag outliers, OON DME clusters, and billing anomalies.
6. CPT-code join results are sent to GPT-5.2-chat (`temperature=0`, fixed `seed`) for summarisation.
7. GPT returns plain-language anomaly explanations and flag recommendations.
8. Results (UM insights, flags, alerts) are written back to the Fabric Lakehouse as Delta tables.
9. Markdown report and JSON flag files are written to the VM filesystem.
10. Solution architect reviews output, validates flags with the UM analyst.

**Postconditions:**
- Lakehouse contains UM insight, flag, and alert tables.
- VM filesystem contains `report.md`, `flags.json`, `appeals_report.json`, etc.
- Re-running with the same `--seed` and input data produces identical output (determinism).

**Acceptance Criteria:**
- AC-POC-1.1: Pipeline completes end-to-end with exit code 0 on valid data.
- AC-POC-1.2: All planted synthetic anomalies appear in the flagged output.
- AC-POC-1.3: LLM summaries are identical across two consecutive runs with the same seed and input.
- AC-POC-1.4: No PHI is present in any output artefact.

---

### UC-POC-2: Validate Incoming Claims Data

**Actor:** Solution Architect / Solution Engineer

**Preconditions:**
- New de-identified claims data has been loaded into the Lakehouse.

**Flow:**
1. Run `um-claims validate --output-dir ./output`.
2. Schema validation checks column presence, types, and nullable constraints.
3. Referential validation checks (e.g., `denial_reason_category` required when `denial_flag=Y`).
4. Statistical distribution checks (e.g., billed_amount variance, cost long-tail shape).
5. Validation report written to output directory.

**Postconditions:**
- Validation report lists all failures with row counts and examples.
- Pipeline halts on critical failures (exit code 1); warns on advisory issues.

**Acceptance Criteria:**
- AC-POC-2.1: Valid data passes with exit code 0.
- AC-POC-2.2: Data with missing required columns fails with a clear error naming the column.
- AC-POC-2.3: Data with `denial_flag=Y` and null `denial_reason_category` fails referential validation.

---

### UC-POC-3: Review and Iterate on Detection Thresholds

**Actor:** Solution Architect + UM Analyst

**Preconditions:**
- Initial pipeline run has completed (UC-POC-1).

**Flow:**
1. Solution architect shares flags and report with the UM analyst.
2. UM analyst provides feedback: "too many false positives on DME volume", "threshold for billing ratio should be 4x not 3x", etc.
3. Solution engineer adjusts thresholds in configuration.
4. Solution architect re-runs `um-claims detect --output-dir ./output`.
5. Reviews updated flags and compares to prior run.

**Postconditions:**
- Updated flags reflect new thresholds.
- Prior run output is preserved for comparison.

**Acceptance Criteria:**
- AC-POC-3.1: Threshold changes are configuration-driven, not code changes.
- AC-POC-3.2: Re-run produces different flag counts consistent with the new thresholds.

---

### UC-POC-4: Summarise Policy Impact

**Actor:** Solution Architect

**Preconditions:**
- Claims data includes a policy change event (e.g., authorization requirement removed for a service).

**Flow:**
1. Run `um-claims run-all` (or `um-claims report` on pre-computed features).
2. Policy simulation compares pre/post utilisation for affected services.
3. GPT-5.2-chat summarises the impact in plain language.
4. Result is included in the Markdown report and written to the Lakehouse.

**Postconditions:**
- Policy impact table shows volume, cost, and denial-rate changes.
- LLM summary explains the direction and magnitude of the effect.

**Acceptance Criteria:**
- AC-POC-4.1: Pre/post comparison table is present in the report.
- AC-POC-4.2: Rebound patterns are detected for services returning to pre-change levels.
- AC-POC-4.3: Effect size and direction are clearly labelled.

---

### UC-POC-5: Demonstrate Reproducibility to Customer

**Actor:** Solution Architect + UM Director

**Preconditions:**
- At least one completed pipeline run exists.

**Flow:**
1. Solution architect re-runs the pipeline with the same seed and input data.
2. Compares output files byte-for-byte with the previous run.
3. Demonstrates to the UM director that results are deterministic.

**Postconditions:**
- Outputs are byte-identical across runs.

**Acceptance Criteria:**
- AC-POC-5.1: `diff` of two run outputs shows zero differences.
- AC-POC-5.2: LLM summaries are character-identical (enforced by `temperature=0` + fixed `seed`).

---

## 3. Data Privacy Constraints (POC)

| Constraint | Enforcement |
|---|---|
| **No PHI in claims data** | De-identification is applied in Snowflake before extraction. The Snowflake team owns this boundary. |
| **No PHI in LLM prompts** | The CLI sends only de-identified CPT-code join results and policy text to Azure OpenAI. |
| **No PHI in outputs** | All Markdown reports, JSON files, and Lakehouse tables are derived from de-identified data. |
| **No PHI in logs** | Pipeline logs do not include claim-level data; only aggregate counts and error messages. |

---

## 4. LLM Governance (POC)

| Parameter | Value | Rationale |
|---|---|---|
| **Model** | GPT-5.2-chat (Azure OpenAI) | Customer-approved model deployment. |
| **`temperature`** | `0` | Eliminates sampling randomness; produces the most likely completion. |
| **`seed`** | Fixed integer (configurable, default `42`) | Combined with `temperature=0`, maximises output determinism across runs. |
| **`max_tokens`** | Capped per prompt type | Prevents runaway token usage. |
| **Prompt templates** | Version-controlled in the repository | Ensures prompt changes are tracked and auditable. |
| **Output grounding** | LLM summaries augment rule-based flags; they never replace the structured evidence (rule name, feature values, threshold). | Aligns with the constitution's explainability principle. |

---

## 5. Success Criteria (POC)

| Metric | Target |
|---|---|
| Pipeline runs end-to-end on de-identified UPMC claims | Yes, within 1 day of data landing |
| All planted anomalies detected | 100% recall on synthetic + seeded patterns |
| False-positive rate acceptable to UM analyst | < 15% on initial thresholds; tuneable |
| LLM summaries are reproducible | Identical across runs with same seed/input |
| UM director receives actionable POC report | Markdown report reviewed and accepted |
| No PHI in any artefact | Verified by manual audit of output files |
| Decision on Production go/no-go | Within 2 weeks of POC completion |
