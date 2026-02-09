"""Tests for benchmarking module."""

import polars as pl

from um_claims.benchmarking import BenchmarkReport, compare_to_benchmarks, compute_internal_metrics
from um_claims.config import BenchmarkBaseline


class TestInternalMetrics:
    def test_compute_metrics(self, minimal_claims_df: pl.DataFrame) -> None:
        metrics = compute_internal_metrics(minimal_claims_df)
        assert "denial_rate" in metrics
        assert "oon_rate" in metrics
        assert "cost_per_claim" in metrics
        assert 0 <= metrics["denial_rate"] <= 1.0
        assert 0 <= metrics["oon_rate"] <= 1.0
        assert metrics["cost_per_claim"] > 0

    def test_empty_dataframe(self) -> None:
        df = pl.DataFrame({
            "denial_flag": pl.Series([], dtype=pl.Utf8),
            "network_status": pl.Series([], dtype=pl.Utf8),
            "allowed_amount": pl.Series([], dtype=pl.Float64),
        })
        metrics = compute_internal_metrics(df)
        assert metrics["denial_rate"] == 0.0


class TestBenchmarkComparison:
    def test_flags_when_exceeds_threshold(self, minimal_claims_df: pl.DataFrame) -> None:
        """Metrics exceeding threshold should be flagged."""
        baselines = [
            BenchmarkBaseline(metric_name="denial_rate", baseline_value=0.01, threshold_pct=0.10),
        ]
        report = compare_to_benchmarks(minimal_claims_df, baselines)
        # Our denial rate is 20% which is way above 0.01
        assert report.comparisons[0].exceeds_threshold
        assert report.comparisons[0].direction == "above"

    def test_within_threshold(self) -> None:
        """Metrics within threshold should not be flagged."""
        df = pl.DataFrame({
            "denial_flag": ["N"] * 100,
            "network_status": ["INN"] * 100,
            "allowed_amount": [100.0] * 100,
        })
        baselines = [
            BenchmarkBaseline(metric_name="denial_rate", baseline_value=0.0, threshold_pct=0.10),
        ]
        report = compare_to_benchmarks(df, baselines)
        assert not report.comparisons[0].exceeds_threshold

    def test_flagged_count(self, minimal_claims_df: pl.DataFrame) -> None:
        baselines = [
            BenchmarkBaseline(metric_name="denial_rate", baseline_value=0.01, threshold_pct=0.01),
            BenchmarkBaseline(metric_name="oon_rate", baseline_value=0.01, threshold_pct=0.01),
        ]
        report = compare_to_benchmarks(minimal_claims_df, baselines)
        assert report.flagged_count >= 1
