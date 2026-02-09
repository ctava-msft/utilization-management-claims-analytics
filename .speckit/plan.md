# Plan — UM Claims Analytics

> Architecture, technology choices, data flow, and component mapping.
> Traces to specifications in `.speckit/specifications.md`.

---

## 1. Architecture Overview

The system follows a **staged pipeline** architecture with explicit data contracts between stages.

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Generate /  │───▶│   Validate   │───▶│   Feature    │───▶│   Detect /   │───▶│   Report     │
│   Ingest     │    │   (Gate)     │    │  Engineer    │    │  Policy Sim  │    │              │
└─────────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
     │                    │                   │                   │                   │
     ▼                    ▼                   ▼                   ▼                   ▼
  raw claims         validation           features            flags/alerts       report + viz
  (Parquet)          report (JSON)        (Parquet)           (Parquet+JSON)     (Markdown+HTML)
```

Each stage reads from the previous stage's output directory. All intermediate artifacts are persisted as Parquet for columnar efficiency and JSON for metadata.

---

## 2. Technology Choices

### DataFrame Library: **Polars**
**Chosen over pandas.** Rationale:
- **Performance**: Polars is written in Rust; 5-10x faster than pandas on typical operations at scale. Spec requires processing tens of millions of rows (SR-1.8).
- **Memory efficiency**: Columnar Apache Arrow memory model; no copies on most operations.
- **Lazy evaluation**: Query plan optimization before execution — important for complex feature pipelines.
- **Deterministic nulls**: Explicit null handling avoids pandas' NaN/None ambiguity.
- **Thread safety**: Multi-threaded by default; pandas is single-threaded.
- **Trade-off acknowledged**: Smaller ecosystem than pandas; some libraries expect pandas. Mitigation: `.to_pandas()` bridge for visualization libraries if needed.

### Data Validation: **Pandera (with Polars backend)**
- Pandera supports Polars DataFrames natively via `pandera.polars`.
- Declarative schema definitions align with spec's schema-first principle (SR-2).
- Custom checks for referential and statistical validations.

### Typed Config / Records: **Pydantic v2**
- Configuration objects, policy definitions, detection rule specs, and flag records are Pydantic models.
- Strict mode for parsing; JSON-serializable for report metadata.

### CLI: **Typer**
- Built on Click; provides type-hinted CLI with auto-generated help.
- Maps directly to SR-9 commands.

### Testing: **pytest**
- With `pytest-cov` for coverage measurement.
- Fixtures provide small deterministic DataFrames for unit tests.

### Linting/Formatting: **Ruff**
- Single tool for both linting and formatting (replaces flake8 + black + isort).
- Fast; written in Rust.

### Type Checking: **Pyright**
- Better Polars type stubs than mypy.
- Stricter default mode catches more issues.

### Visualization: **Matplotlib** (primary) + **Plotly** (optional HTML)
- Matplotlib for static PNG charts in reports.
- Plotly for interactive HTML report variant (optional enhancement).

### Package/Project Management: **uv**
- Lock file for reproducible installs.
- `pyproject.toml` with `[project.scripts]` for CLI entrypoint.

---

## 3. Project Layout

```
utilization-management-claims-analytics/
├── .speckit/
│   ├── constitution.md
│   ├── specifications.md
│   ├── plan.md
│   └── tasks.md
├── src/
│   └── um_claims/
│       ├── __init__.py
│       ├── cli.py              # Typer CLI entrypoint (SR-9)
│       ├── config.py           # Pydantic config models
│       ├── schema.py           # Pandera schemas + Pydantic claim record
│       ├── generate_data.py    # Synthetic data generation (SR-1)
│       ├── ingest.py           # Load from Parquet/CSV
│       ├── validate.py         # Schema + referential + stats validation (SR-2)
│       ├── features.py         # Feature engineering (SR-3)
│       ├── detection.py        # Outlier + fraud detection (SR-4)
│       ├── policy_sim.py       # Policy effectiveness analysis (SR-5)
│       ├── appeals.py          # Appeals/grievances analytics (SR-6)
│       ├── benchmarking.py     # Benchmarking (SR-7)
│       └── reporting.py        # Report generation (SR-8)
├── tests/
│   ├── conftest.py             # Shared fixtures
│   ├── test_schema.py
│   ├── test_generate_data.py
│   ├── test_validate.py
│   ├── test_features.py
│   ├── test_detection.py
│   ├── test_policy_sim.py
│   ├── test_appeals.py
│   ├── test_benchmarking.py
│   └── test_reporting.py
├── data/                       # Generated sample data (gitignored except small samples)
├── output/                     # Pipeline run outputs (gitignored)
├── pyproject.toml
├── README.md
└── .gitignore
```

---

## 4. Component → Specification Mapping

| Module | Specifications Covered | Key Interfaces |
|---|---|---|
| `config.py` | All (cross-cutting) | `PipelineConfig`, `PolicyChangeEvent`, `BenchmarkBaseline` |
| `schema.py` | SR-2, Claims Schema | `ClaimsSchema` (Pandera), `ClaimRecord` (Pydantic) |
| `generate_data.py` | SR-1 | `generate_claims(config) → DataFrame` |
| `ingest.py` | SR-1, SR-9 | `load_claims(path) → DataFrame` |
| `validate.py` | SR-2 | `validate_claims(df, schema) → ValidationResult` |
| `features.py` | SR-3 | `compute_features(df) → FeaturesBundle` |
| `detection.py` | SR-4 | `detect_anomalies(features) → list[Flag]` |
| `policy_sim.py` | SR-5 | `analyze_policy_impact(df, events) → PolicyReport` |
| `appeals.py` | SR-6 | `analyze_appeals(df) → AppealsReport` |
| `benchmarking.py` | SR-7 | `compare_to_benchmarks(features, baselines) → BenchmarkReport` |
| `reporting.py` | SR-8 | `generate_report(all_results, output_dir)` |
| `cli.py` | SR-9 | Typer app with commands |

---

## 5. Data Flow Detail

### Stage 1: Generate / Ingest
- **Input**: CLI args (num_claims, seed) OR existing Parquet file path.
- **Output**: `raw_claims.parquet` — full claims DataFrame.
- **Key design**: Data generation uses NumPy seeded RNG. Fraud clusters and policy events are injected as post-processing overlays on the base distribution.

### Stage 2: Validate
- **Input**: `raw_claims.parquet`
- **Output**: `validation_report.json` + pass/fail status.
- **Key design**: Two-tier validation:
  - **Critical** (fail-fast): schema conformance, type checks, required fields.
  - **Advisory** (warn): statistical distribution checks, outlier in source data.

### Stage 3: Feature Engineering
- **Input**: Validated claims DataFrame.
- **Output**: `provider_features.parquet`, `temporal_features.parquet`, `service_features.parquet`.
- **Key design**: Features computed via Polars lazy groupby + window functions. No Python loops.

### Stage 4: Detection + Policy Sim
- **Input**: Feature DataFrames + policy config.
- **Output**: `flags.parquet`, `flags.json`, `policy_impact.json`.
- **Key design**: Each detection rule is a function `(features_df) → list[Flag]`. Rules are registered in a list and iterated. Flags are Pydantic models serialized to JSON.

### Stage 5: Reporting
- **Input**: All previous outputs.
- **Output**: `report.md`, `report.html` (optional), PNG charts in `figures/`.
- **Key design**: Jinja2-style template rendering (or string formatting for simplicity in starter). Charts via matplotlib saved as PNG and embedded in HTML.

---

## 6. Key Design Patterns

### Explainable Flags
```python
class Flag(BaseModel):
    rule_name: str          # e.g., "high_volume_new_supplier"
    entity_type: str        # "provider" | "supplier" | "service"
    entity_id: str
    severity: str           # "high" | "medium" | "low"
    feature_values: dict    # actual features used
    threshold: float
    actual_value: float
    description: str        # human-readable explanation
```

### Pipeline Config
```python
class PipelineConfig(BaseModel):
    seed: int = 42
    num_claims: int = 100_000
    output_dir: Path = Path("output")
    policy_events: list[PolicyChangeEvent] = []
    benchmarks: list[BenchmarkBaseline] = []
    cost_per_appeal: float = 350.0
```

### Service Categories
Procedure codes are mapped to service categories via a deterministic lookup:
- DME: codes `HCPCS-E*`, `HCPCS-K*`
- Imaging: codes `CPT-7*`
- E&M: codes `CPT-99*`
- Surgical: codes `CPT-2*`, `CPT-3*`, `CPT-4*`, `CPT-5*`, `CPT-6*`
- Pharmacy: claim_type = Pharmacy
- Other: everything else

---

## 7. Performance Considerations

- Polars lazy mode for all aggregation pipelines.
- Parquet I/O for all intermediate data (columnar, compressed).
- No row-by-row iteration; all transforms are vectorized.
- For demo (100K rows): entire pipeline completes in seconds.
- For scale test (10M rows): pipeline should complete in < 5 minutes on commodity hardware.

---

## 8. Trade-offs and Risks

| Decision | Trade-off | Mitigation |
|---|---|---|
| Polars over pandas | Smaller ecosystem; some visualization libs need pandas | `.to_pandas()` adapter for chart data |
| Rule-based detection over ML | Less adaptive; may miss novel patterns | Explicit by design (explainability requirement); ML can layer on top |
| Single-machine only | Cannot scale beyond ~50M rows efficiently | Architecture is compatible with distributed Polars / Spark migration |
| Synthetic codes (not real CPT) | Less realistic | Avoids AMA copyright; patterns are preserved |
| Markdown report (not BI tool) | Less interactive | Starter solution; production would integrate with PowerBI/Tableau |
