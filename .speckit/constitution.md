# Constitution — UM Claims Analytics

> Governing principles and non-negotiables for the Utilization Management Claims Analytics project.

## Purpose

This project provides a **production-grade, specification-driven Python solution** for Utilization Management (UM) analytics in a payer environment. It operates exclusively on synthetic data and demonstrates patterns applicable to UPMC Health Plan's scale (~43 M claims/year).

## Core Principles

### 1. Specifications Are the Source of Truth
- Every design decision, module boundary, and acceptance test traces back to a specification.
- Code implements specs; specs are not reverse-engineered from code.
- When specs and code diverge, specs win — code must be corrected.

### 2. No PHI — Ever
- The repository must **never** contain Protected Health Information (PHI) or real member/provider data.
- All identifiers (member, provider, NPI, claim) are synthetically generated.
- Data generation is deterministic and seeded to ensure reproducibility without real-world linkage.

### 3. Determinism and Reproducibility
- All random processes use explicit seeds.
- Dependencies are pinned (lock file).
- Given the same inputs, every run produces identical outputs.

### 4. Schema-First, Fail-Fast
- Data schemas are defined before any transformation logic.
- Strong typing via Pydantic models and Pandera dataframe schemas.
- Validation gates run before processing; invalid data halts the pipeline with clear diagnostics rather than silently propagating errors.

### 5. Explainability Over Black Boxes
- Every flag, alert, or anomaly score must carry the **rule name**, **feature values**, and **threshold** that triggered it.
- No unexplained scores. A UM stakeholder must be able to read any flag and understand *why* it fired.

### 6. Modularity and Separation of Concerns
- The pipeline is decomposed into discrete stages: ingestion → validation → feature engineering → detection → reporting.
- Each stage has its own module, its own tests, and its own interface contract.
- Stages are composable; you can run validation alone, or skip to reporting from pre-computed features.

### 7. Performance Awareness
- Design assumes tens of millions of rows even when demo data is smaller.
- Use columnar data formats and chunked processing where appropriate.
- Avoid row-by-row Python loops on core paths.

### 8. Test Coverage Is Non-Negotiable
- Every core transformation, validation rule, and detection rule has at least one automated test.
- Tests run via `pytest` and must pass before any task is considered done.
- Edge cases (empty data, nulls, boundary values) are explicitly tested.

### 9. Code Quality Gates
- Linting and formatting enforced by `ruff`.
- Static type checking configured (pyright or mypy).
- CI-ready configuration (even if CI is not set up in this starter).

### 10. Security Hygiene
- No secrets, credentials, or API keys in the repository.
- No external network calls required to run the solution.
- All dependencies are installable from public PyPI.

## Non-Negotiable Boundaries

| Boundary | Rule |
|---|---|
| Data privacy | Synthetic only; no PHI under any circumstance |
| Reproducibility | Seeded RNG + pinned deps = identical outputs |
| Validation | Schema validation must gate every pipeline run |
| Explainability | Every flag shows rule + evidence |
| Testing | No untested transformations ship |
| Modularity | No monolith scripts; clear module boundaries |

## Assumptions Recorded Here

- The solution is a **starter/accelerator**, not a production deployment. It demonstrates patterns at scale with synthetic data.
- Peer benchmarking baselines are synthetic placeholders; real baselines would come from external datasets in production.
- The project targets a single-machine execution model (not distributed compute), but data patterns are compatible with future migration to Spark/Dask/distributed Polars.
- UM policy definitions are simplified simulations; real policies would integrate with a rules engine and benefit configuration system.

## Out of Scope

- Real-time streaming ingestion.
- Integration with EHR, claims adjudication, or authorization systems.
- User authentication / RBAC for reports.
- Deployment infrastructure (containers, cloud services).
- Machine learning model training (the project uses rule-based detection).
