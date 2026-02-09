# UM Claims Analytics

Deterministic, explainable analytics pipeline for **Utilization Management (UM) claims data**. Generates realistic synthetic claims, validates schemas, engineers features, detects outliers/fraud patterns, simulates policy changes, analyzes appeals funnels, benchmarks against baselines, and produces Markdown + chart reports — all without touching PHI.

## Quick Start

```bash
# Install (requires Python ≥ 3.11 and uv)
uv sync --all-extras

# Run the full pipeline (generate → validate → features → detect → report)
um-claims run-all --seed 42 --num-claims 5000 --output-dir output

# View the report
cat output/report.md
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `um-claims generate-data` | Generate synthetic UM claims with realistic patterns |
| `um-claims validate` | Validate a claims CSV against the schema |
| `um-claims process` | Ingest + validate + compute features |
| `um-claims detect` | Run all detection rules on feature data |
| `um-claims report` | Generate Markdown report with charts |
| `um-claims run-all` | Execute the full pipeline end-to-end |

### Common Options

```bash
um-claims run-all --seed 42 --num-claims 10000 --output-dir results
um-claims generate-data --seed 123 --num-claims 5000 --output-dir data
um-claims validate --input data/claims.parquet
```

## Pipeline Stages

```
generate_data ──► ingest ──► validate ──► features ──► detect ──► report
                                                     ├── policy_sim
                                                     ├── appeals
                                                     └── benchmarking
```

1. **Generate** — Synthetic claims with long-tail costs (lognormal + Pareto), seasonal patterns, denial→appeal dynamics, OON DME fraud clusters, and policy change effects.
2. **Validate** — Two-tier validation (critical + advisory) checking column presence, nulls, value ranges, enum membership, referential integrity, and data quality.
3. **Features** — Provider-level aggregates (17+ metrics), temporal trends (weekly/monthly rolling), and service-category breakdowns.
4. **Detect** — Five rule-based detectors producing explainable `Flag` objects:
   - `high_volume_provider` — claim count > μ + 2σ
   - `high_cost_provider` — total allowed > μ + 2σ
   - `new_entity_high_volume` — entity < 90 days old, volume > 90th percentile
   - `oon_dme_cluster` — DME supplier with ≥80% OON, ≤3 procedure codes, high volume
   - `billing_ratio_outlier` — billed/allowed ratio > 3× peer median
5. **Policy Simulation** — Pre/post comparison of configurable policy events with rebound detection.
6. **Appeals** — Denial→appeal funnel analysis with overturn rates, category breakdown, and admin cost estimates.
7. **Benchmarking** — Variance-to-baseline comparison with configurable thresholds.
8. **Report** — Markdown summary with three PNG charts (cost distribution, utilization trend, denial funnel).

## Sample Output

Running `um-claims run-all --seed 42 --num-claims 1000` produces:

```
output/
├── claims.parquet          # Raw synthetic claims
├── validation.json         # Validation results
├── provider_features.parquet
├── temporal_features.parquet
├── service_category_features.parquet
├── flags.json              # Detection flags with explanations
├── report.md               # Markdown narrative report
├── cost_distribution.png
├── utilization_trend.png
└── denial_funnel.png
```

### Reading Flags

Each flag is a self-contained JSON object:

```json
{
  "rule_name": "oon_dme_cluster",
  "entity_type": "supplier",
  "entity_id": "FRAUD_DME_001",
  "severity": "high",
  "feature_values": {
    "dme_rate": 1.0,
    "oon_rate": 0.95,
    "unique_procedure_codes": 2,
    "total_claims": 150
  },
  "threshold": 0.8,
  "actual_value": 0.95,
  "description": "DME supplier FRAUD_DME_001 has 95.0% OON rate..."
}
```

## Project Structure

```
src/um_claims/
├── __init__.py        # Version
├── cli.py             # Typer CLI (6 commands)
├── config.py          # PipelineConfig, DetectionConfig, constants
├── schema.py          # ClaimRecord (Pydantic), column definitions
├── generate_data.py   # Synthetic data with realistic patterns
├── ingest.py          # Load/save Parquet files
├── validate.py        # Two-tier schema validation
├── features.py        # Provider, temporal, category features
├── detection.py       # 5 detection rules → Flag objects
├── policy_sim.py      # Policy impact analysis
├── appeals.py         # Denial→appeal funnel
├── benchmarking.py    # Variance-to-baseline
└── reporting.py       # Markdown + PNG chart generation
tests/
├── conftest.py        # Shared fixtures
└── test_*.py          # 58 tests across 10 files
```

## Tech Stack

| Component | Choice | Why |
|-----------|--------|-----|
| DataFrame | Polars | Columnar, multi-threaded, lazy evaluation |
| Config/Models | Pydantic v2 | Typed, serializable, validated |
| CLI | Typer | Autocompletion, help generation |
| Charts | Matplotlib | Static PNGs, no browser needed |
| Random | NumPy | Seeded, deterministic |
| Tests | pytest + pytest-cov | Fast, fixtures, coverage |
| Lint | Ruff | Fast all-in-one |
| Types | Pyright | Strict type checking |

## Development

```bash
# Install dev dependencies
uv sync --all-extras

# Run tests
uv run pytest tests/ -v

# Run with coverage
uv run pytest tests/ --cov=um_claims --cov-report=term-missing

# Lint & format
uv run ruff check src/ tests/
uv run ruff format src/ tests/

# Type check
uv run pyright
```

## Design Principles

- **No PHI** — All data is synthetic; no real patient information
- **Deterministic** — Same seed → same output, every time
- **Schema-first** — Data contracts enforced before processing
- **Explainable** — Every flag carries full context for human review
- **Modular** — Each stage independently testable and replaceable

## License

MIT
