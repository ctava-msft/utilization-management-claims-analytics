"""CLI entrypoint for UM Claims Analytics.

Commands:
  generate-data  — Generate synthetic claims data
  validate       — Validate claims data against schema
  process        — Run feature engineering
  detect         — Run outlier/anomaly detection
  report         — Generate summary report
  run-all        — Execute the full pipeline end-to-end

Spec: SR-9
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import typer
from rich.console import Console

from um_claims.config import PipelineConfig

app = typer.Typer(
    name="um-claims",
    help="UM Claims Analytics — synthetic data, validation, detection, and reporting.",
    add_completion=False,
)
console = Console()


def _get_config(
    seed: int,
    num_claims: int,
    output_dir: Path,
    config_file: Path | None = None,
) -> PipelineConfig:
    """Build pipeline config from CLI args and optional config file."""
    if config_file and config_file.exists():
        raw = json.loads(config_file.read_text())
        return PipelineConfig(**raw)
    return PipelineConfig(seed=seed, num_claims=num_claims, output_dir=output_dir)


@app.command()
def generate_data(
    seed: int = typer.Option(42, help="Random seed for reproducible generation"),
    num_claims: int = typer.Option(100_000, help="Number of claims to generate"),
    output_dir: Path = typer.Option(Path("output"), help="Output directory"),
    config_file: Path | None = typer.Option(None, "--config", help="JSON config file"),
) -> None:
    """Generate synthetic claims data."""
    from um_claims.generate_data import generate_claims
    from um_claims.ingest import save_claims

    config = _get_config(seed, num_claims, output_dir, config_file)
    console.print(f"[bold blue]Generating {config.num_claims:,} synthetic claims (seed={config.seed})...[/]")

    df = generate_claims(config)
    out_path = config.output_dir / "raw_claims.parquet"
    save_claims(df, out_path)

    console.print(f"[green]✓ Generated {len(df):,} claims → {out_path}[/]")


@app.command()
def validate(
    output_dir: Path = typer.Option(Path("output"), help="Directory with raw_claims.parquet"),
) -> None:
    """Validate claims data against schema and business rules."""
    from um_claims.ingest import load_claims
    from um_claims.validate import validate_claims

    claims_path = output_dir / "raw_claims.parquet"
    console.print(f"[bold blue]Validating {claims_path}...[/]")

    df = load_claims(claims_path)
    result = validate_claims(df)

    # Save validation report
    report_path = output_dir / "validation_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(result.model_dump_json(indent=2))

    if result.passed:
        console.print(f"[green]✓ Validation passed ({result.total_rows:,} rows, "
                      f"{len(result.advisory_issues)} advisory warnings)[/]")
    else:
        console.print(f"[red]✗ Validation FAILED — {len(result.critical_issues)} critical issues:[/]")
        for issue in result.critical_issues:
            console.print(f"  [red]• {issue.rule}: {issue.message}[/]")
        raise typer.Exit(code=1)


@app.command()
def process(
    output_dir: Path = typer.Option(Path("output"), help="Directory with raw_claims.parquet"),
) -> None:
    """Run feature engineering on validated claims."""
    from um_claims.features import compute_all_features
    from um_claims.ingest import load_claims

    claims_path = output_dir / "raw_claims.parquet"
    console.print(f"[bold blue]Processing features from {claims_path}...[/]")

    df = load_claims(claims_path)
    features = compute_all_features(df)

    # Save feature DataFrames
    for name, feat_df in features.items():
        if name == "claims":
            feat_df.write_parquet(output_dir / "enriched_claims.parquet")
        else:
            feat_df.write_parquet(output_dir / f"{name}_features.parquet")

    console.print(f"[green]✓ Features computed: {', '.join(features.keys())}[/]")


@app.command()
def detect(
    seed: int = typer.Option(42, help="Random seed (for config loading)"),
    num_claims: int = typer.Option(100_000, help="Num claims (for config loading)"),
    output_dir: Path = typer.Option(Path("output"), help="Directory with feature parquets"),
    config_file: Path | None = typer.Option(None, "--config", help="JSON config file"),
) -> None:
    """Run outlier/anomaly detection and policy analysis."""
    from um_claims.appeals import analyze_appeals
    from um_claims.benchmarking import compare_to_benchmarks
    from um_claims.detection import run_all_detection_rules
    from um_claims.ingest import load_claims
    from um_claims.policy_sim import analyze_policy_impact

    config = _get_config(seed, num_claims, output_dir, config_file)
    console.print("[bold blue]Running detection rules...[/]")

    # Load features
    provider_features = load_claims(output_dir / "provider_features.parquet")
    claims_df = load_claims(output_dir / "enriched_claims.parquet")

    # Detection
    flags = run_all_detection_rules(provider_features, config.detection)
    console.print(f"  Flags: {len(flags)} ({sum(1 for f in flags if f.severity == 'high')} high)")

    # Policy impact
    policy_report = analyze_policy_impact(claims_df, config.policy_events, config.detection)

    # Appeals
    appeals_report = analyze_appeals(claims_df, config.cost_per_appeal)

    # Benchmarking
    benchmark_report = compare_to_benchmarks(claims_df, config.benchmarks)

    # Save results
    flags_json = [f.model_dump() for f in flags]
    (output_dir / "flags.json").write_text(json.dumps(flags_json, indent=2, default=str))
    (output_dir / "policy_impact.json").write_text(policy_report.model_dump_json(indent=2))
    (output_dir / "appeals_report.json").write_text(appeals_report.model_dump_json(indent=2))
    (output_dir / "benchmark_report.json").write_text(benchmark_report.model_dump_json(indent=2))

    console.print(f"[green]✓ Detection complete. {len(flags)} flags written.[/]")


@app.command()
def report(
    seed: int = typer.Option(42, help="Random seed (for config loading)"),
    num_claims: int = typer.Option(100_000, help="Num claims (for config loading)"),
    output_dir: Path = typer.Option(Path("output"), help="Directory with all pipeline outputs"),
    config_file: Path | None = typer.Option(None, "--config", help="JSON config file"),
) -> None:
    """Generate summary report with visualizations."""
    from um_claims.appeals import AppealsReport
    from um_claims.benchmarking import BenchmarkReport
    from um_claims.detection import Flag
    from um_claims.ingest import load_claims
    from um_claims.policy_sim import PolicyImpactReport
    from um_claims.reporting import generate_report

    config = _get_config(seed, num_claims, output_dir, config_file)
    console.print("[bold blue]Generating report...[/]")

    claims_df = load_claims(output_dir / "enriched_claims.parquet")
    temporal = load_claims(output_dir / "temporal_features.parquet")

    # Load detection results
    flags_raw = json.loads((output_dir / "flags.json").read_text())
    flags = [Flag(**f) for f in flags_raw]
    policy_report = PolicyImpactReport(**json.loads((output_dir / "policy_impact.json").read_text()))
    appeals_report = AppealsReport(**json.loads((output_dir / "appeals_report.json").read_text()))
    benchmark_report = BenchmarkReport(**json.loads((output_dir / "benchmark_report.json").read_text()))

    report_path = generate_report(
        config=config,
        df=claims_df,
        flags=flags,
        policy_report=policy_report,
        appeals_report=appeals_report,
        benchmark_report=benchmark_report,
        temporal_features=temporal,
        output_dir=output_dir,
    )

    console.print(f"[green]✓ Report generated → {report_path}[/]")


@app.command()
def ingest_kaggle(
    input_file: Path = typer.Option(..., "--input", help="Path to Kaggle CSV file"),
    output_dir: Path = typer.Option(Path("output"), help="Output directory"),
) -> None:
    """Ingest a Kaggle Enhanced Health Insurance Claims CSV into canonical format."""
    from um_claims.ingest import save_claims
    from um_claims.io.kaggle_loader import load_kaggle_claims

    console.print(f"[bold blue]Ingesting Kaggle CSV: {input_file}...[/]")

    df = load_kaggle_claims(input_file)
    out_path = output_dir / "raw_claims.parquet"
    save_claims(df, out_path)

    console.print(f"[green]✓ Ingested {len(df):,} claims → {out_path}[/]")


@app.command()
def run_all(
    seed: int = typer.Option(42, help="Random seed for reproducible generation"),
    num_claims: int = typer.Option(100_000, help="Number of claims to generate"),
    output_dir: Path = typer.Option(Path("output"), help="Output directory"),
    config_file: Path | None = typer.Option(None, "--config", help="JSON config file"),
) -> None:
    """Execute the full pipeline: generate → validate → process → detect → report."""
    config = _get_config(seed, num_claims, output_dir, config_file)
    console.print("[bold blue]═══ UM Claims Analytics — Full Pipeline ═══[/]\n")

    try:
        # Stage 1: Generate
        from um_claims.generate_data import generate_claims
        from um_claims.ingest import save_claims

        console.print("[bold]Stage 1: Generate Data[/]")
        df = generate_claims(config)
        save_claims(df, config.output_dir / "raw_claims.parquet")
        console.print(f"  [green]✓ {len(df):,} claims generated[/]\n")

        # Stage 2: Validate
        from um_claims.validate import validate_claims

        console.print("[bold]Stage 2: Validate[/]")
        result = validate_claims(df)
        report_path = config.output_dir / "validation_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(result.model_dump_json(indent=2))

        if not result.passed:
            console.print(f"  [red]✗ Validation FAILED[/]")
            for issue in result.critical_issues:
                console.print(f"    [red]• {issue.rule}: {issue.message}[/]")
            raise typer.Exit(code=1)
        console.print(f"  [green]✓ Passed ({len(result.advisory_issues)} advisories)[/]\n")

        # Stage 3: Feature Engineering
        from um_claims.features import compute_all_features

        console.print("[bold]Stage 3: Feature Engineering[/]")
        features = compute_all_features(df)
        for name, feat_df in features.items():
            if name == "claims":
                feat_df.write_parquet(config.output_dir / "enriched_claims.parquet")
            else:
                feat_df.write_parquet(config.output_dir / f"{name}_features.parquet")
        console.print(f"  [green]✓ Features: {', '.join(features.keys())}[/]\n")

        # Stage 4: Detection
        from um_claims.appeals import analyze_appeals
        from um_claims.benchmarking import compare_to_benchmarks
        from um_claims.detection import run_all_detection_rules
        from um_claims.policy_sim import analyze_policy_impact

        console.print("[bold]Stage 4: Detection & Analysis[/]")
        flags = run_all_detection_rules(features["provider"], config.detection)
        policy_report = analyze_policy_impact(
            features["claims"], config.policy_events, config.detection
        )
        appeals_report = analyze_appeals(features["claims"], config.cost_per_appeal)
        benchmark_report = compare_to_benchmarks(features["claims"], config.benchmarks)

        flags_json = [f.model_dump() for f in flags]
        (config.output_dir / "flags.json").write_text(
            json.dumps(flags_json, indent=2, default=str)
        )
        (config.output_dir / "policy_impact.json").write_text(
            policy_report.model_dump_json(indent=2)
        )
        (config.output_dir / "appeals_report.json").write_text(
            appeals_report.model_dump_json(indent=2)
        )
        (config.output_dir / "benchmark_report.json").write_text(
            benchmark_report.model_dump_json(indent=2)
        )

        high_count = sum(1 for f in flags if f.severity == "high")
        console.print(f"  [green]✓ {len(flags)} flags ({high_count} high)[/]\n")

        # Stage 5: Report
        from um_claims.reporting import generate_report

        console.print("[bold]Stage 5: Report Generation[/]")
        report_file = generate_report(
            config=config,
            df=features["claims"],
            flags=flags,
            policy_report=policy_report,
            appeals_report=appeals_report,
            benchmark_report=benchmark_report,
            temporal_features=features["temporal"],
            output_dir=config.output_dir,
        )
        console.print(f"  [green]✓ Report → {report_file}[/]\n")

        console.print("[bold green]═══ Pipeline complete ═══[/]")

    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Pipeline error: {e}[/]")
        raise typer.Exit(code=2) from e


if __name__ == "__main__":
    app()
