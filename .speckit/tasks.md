# Tasks — UM Claims Analytics

> Dependency-ordered implementation tasks with Definition of Done (DoD).

---

## Task 1: Project Scaffolding
**Depends on:** None
**Specs:** SR-9, Plan §3

**Work:**
- Create `pyproject.toml` with uv configuration, dependencies, and `[project.scripts]` entrypoint.
- Create `src/um_claims/__init__.py`.
- Create `.gitignore` (data/, output/, __pycache__, .venv).
- Configure ruff and pyright in `pyproject.toml`.

**DoD:**
- `uv sync` installs all dependencies.
- `ruff check src/` passes.
- Project is importable: `python -c "import um_claims"`.

---

## Task 2: Config and Schema Models
**Depends on:** Task 1
**Specs:** SR-2, Claims Schema, Plan §6

**Work:**
- Implement `config.py`: `PipelineConfig`, `PolicyChangeEvent`, `BenchmarkBaseline` Pydantic models.
- Implement `schema.py`: `ClaimRecord` Pydantic model + Pandera DataFrameSchema for Polars.

**DoD:**
- Config models serialize/deserialize to JSON.
- Schema validates a correct DataFrame and rejects an incorrect one.
- Unit tests pass for both config and schema.

---

## Task 3: Synthetic Data Generation
**Depends on:** Task 2
**Specs:** SR-1

**Work:**
- Implement `generate_data.py`: `generate_claims(config: PipelineConfig) -> pl.DataFrame`.
- Implement long-tail cost distributions (lognormal + Pareto tail).
- Implement seasonal patterns via service_date month weighting.
- Inject DME fraud cluster: N suspicious suppliers with high OON rate, concentrated codes, recent entity age.
- Inject policy change event: toggle auth requirement off at a date for selected services.
- Implement denial→appeal dynamics with configurable propensity.
- Write output as Parquet.

**DoD:**
- Running `generate_claims(PipelineConfig(seed=42, num_claims=100000))` twice produces identical DataFrames.
- Top 20% of claims account for ≥ 70% of billed amount.
- At least one DME fraud cluster is identifiable in the data.
- Tests validate determinism, distribution shape, and fraud cluster presence.

---

## Task 4: Ingestion
**Depends on:** Task 2
**Specs:** SR-1, SR-9

**Work:**
- Implement `ingest.py`: `load_claims(path: Path) -> pl.DataFrame`.
- Support Parquet input.

**DoD:**
- Loads a generated Parquet file and returns a valid Polars DataFrame.
- Test confirms round-trip: generate → save → load → same shape.

---

## Task 5: Validation
**Depends on:** Task 2, Task 4
**Specs:** SR-2

**Work:**
- Implement `validate.py`: `validate_claims(df: pl.DataFrame) -> ValidationResult`.
- Critical checks: schema conformance, types, required fields, value ranges.
- Referential checks: denial_reason required when denial_flag=Y.
- Statistical checks: zero-variance warning, unexpected null rates.
- Return structured `ValidationResult` with pass/fail + list of issues.

**DoD:**
- Valid synthetic data passes validation.
- Data with dropped column fails with clear error.
- Data with denial_flag=Y and null denial_reason fails.
- Tests cover all validation rule categories.

---

## Task 6: Feature Engineering
**Depends on:** Task 5
**Specs:** SR-3

**Work:**
- Implement `features.py`: provider-level, temporal, and service-category features.
- Provider metrics: total allowed, avg units, claim frequency, OON rate, denial rate, entity age.
- Temporal metrics: weekly/monthly volume, rolling 4-week averages.
- Service category tagging from procedure codes.
- Cost-per-unit and allowed-to-billed ratio.

**DoD:**
- Feature DataFrames have expected columns and correct aggregation logic.
- Tests verify aggregation on a small fixture (e.g., 3 providers, 10 claims).
- No Python loops; all vectorized Polars operations.

---

## Task 7: Outlier and Anomaly Detection
**Depends on:** Task 6
**Specs:** SR-4

**Work:**
- Implement `detection.py`: detection rules that produce `Flag` objects.
- Rules: high_volume_provider, high_cost_provider, new_entity_high_volume, oon_dme_cluster, billing_ratio_outlier.
- Each rule is a function taking feature DataFrame and returning list of Flags.

**DoD:**
- Planted synthetic fraud cluster is detected by oon_dme_cluster rule.
- Non-suspicious entities are not flagged.
- Every Flag has all required fields (rule_name, entity_id, feature_values, threshold, actual_value, severity, description).
- Tests validate rule logic on crafted fixtures.

---

## Task 8: Policy Simulation
**Depends on:** Task 6
**Specs:** SR-5

**Work:**
- Implement `policy_sim.py`: compare pre/post utilization around policy change events.
- Compute volume, cost, denial rate, OON rate before/after.
- Detect rebound patterns.
- Return structured `PolicyImpactReport`.

**DoD:**
- Synthetic policy change is detected with correct pre/post metrics.
- Rebound detection works on a crafted fixture.
- Tests verify arithmetic and edge cases (no claims in period).

---

## Task 9: Appeals Analytics
**Depends on:** Task 6
**Specs:** SR-6

**Work:**
- Implement `appeals.py`: denial→appeal funnel, top categories, admin cost estimate.
- Compute conversion rates by denial reason.
- Identify high-appeal-rate providers.

**DoD:**
- Funnel metrics match manual calculation on fixture data.
- Admin cost = appeals_count × cost_per_appeal.
- Tests cover the funnel and cost calculations.

---

## Task 10: Benchmarking
**Depends on:** Task 6
**Specs:** SR-7

**Work:**
- Implement `benchmarking.py`: compare internal metrics to synthetic peer baselines.
- Variance-to-benchmark computation.
- Flag metrics exceeding threshold.

**DoD:**
- Benchmark comparison table is correct for known inputs.
- Flags fire when internal metric exceeds baseline by configured threshold.
- Tests verify comparisons.

---

## Task 11: Reporting
**Depends on:** Tasks 7-10
**Specs:** SR-8

**Work:**
- Implement `reporting.py`: aggregate all results into Markdown report.
- Include: run metadata, key metrics, top anomalies, policy impact, appeal burden, benchmark comparison.
- Generate matplotlib visualizations: cost distribution histogram, utilization trend with control bands, denial funnel bar chart.
- Save report + figures to timestamped output directory.

**DoD:**
- Report is generated as a valid Markdown file.
- At least 3 charts (distribution, trend, funnel) are saved as PNG.
- Report references charts correctly.
- Tests verify report generation produces expected files.

---

## Task 12: CLI
**Depends on:** Tasks 3-11
**Specs:** SR-9

**Work:**
- Implement `cli.py`: Typer app with commands: generate-data, validate, process, detect, report, run-all.
- Wire each command to the corresponding module functions.
- Common options: --seed, --num-claims, --output-dir, --config.
- Exit codes: 0 success, 1 validation failure, 2 runtime error.

**DoD:**
- `um-claims generate-data --seed 42 --num-claims 1000` works.
- `um-claims run-all` chains all stages and produces output directory.
- Exit codes are correct on success and failure.
- Help text is auto-generated for all commands.

---

## Task 13: Integration Tests + Coverage
**Depends on:** Task 12
**Specs:** All

**Work:**
- Write integration test that runs full pipeline on 1,000 claims.
- Verify all output files exist.
- Configure pytest-cov; target ≥ 80% line coverage.

**DoD:**
- Integration test passes.
- Coverage report ≥ 80%.

---

## Task 14: README
**Depends on:** Task 12
**Specs:** Deliverables

**Work:**
- Write README.md with setup instructions, example commands, sample output interpretation.

**DoD:**
- A new developer can clone the repo, run `uv sync`, and execute `um-claims run-all` successfully following README instructions.
