"""Tests for reporting module."""

from pathlib import Path
import json

import polars as pl

from um_claims.appeals import AppealsReport, analyze_appeals
from um_claims.benchmarking import BenchmarkReport, compare_to_benchmarks
from um_claims.config import PipelineConfig
from um_claims.detection import run_all_detection_rules
from um_claims.features import compute_all_features
from um_claims.generate_data import generate_claims
from um_claims.policy_sim import PolicyImpactReport, analyze_policy_impact
from um_claims.reporting import generate_report


class TestReporting:
    def test_report_generated(self, tmp_path: Path) -> None:
        """Full report generation should produce expected files."""
        config = PipelineConfig(seed=42, num_claims=500, output_dir=tmp_path)
        df = generate_claims(config)
        features = compute_all_features(df)

        flags = run_all_detection_rules(features["provider"], config.detection)
        policy_report = analyze_policy_impact(
            features["claims"], config.policy_events, config.detection
        )
        appeals_report = analyze_appeals(features["claims"], config.cost_per_appeal)
        benchmark_report = compare_to_benchmarks(features["claims"], config.benchmarks)

        report_path = generate_report(
            config=config,
            df=features["claims"],
            flags=flags,
            policy_report=policy_report,
            appeals_report=appeals_report,
            benchmark_report=benchmark_report,
            temporal_features=features["temporal"],
            output_dir=tmp_path,
        )

        assert report_path.exists()
        assert (tmp_path / "figures" / "cost_distribution.png").exists()
        assert (tmp_path / "figures" / "utilization_trend.png").exists()
        assert (tmp_path / "figures" / "denial_funnel.png").exists()

        # Report content checks
        content = report_path.read_text(encoding="utf-8")
        assert "Key Metrics" in content
        assert "Top Anomalies" in content
        assert "Policy Impact" in content
        assert "Appeals" in content
        assert "Benchmarking" in content
        assert "Recommended Next Questions" in content
