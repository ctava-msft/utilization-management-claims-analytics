"""Tests for feature engineering module."""

import polars as pl

from um_claims.features import (
    compute_all_features,
    compute_provider_features,
    compute_temporal_features,
    tag_service_categories,
)


class TestServiceCategoryTagging:
    def test_tags_added(self, minimal_claims_df: pl.DataFrame) -> None:
        tagged = tag_service_categories(minimal_claims_df)
        assert "service_category" in tagged.columns
        categories = tagged["service_category"].unique().to_list()
        assert "E&M" in categories  # CPT-99201
        assert "Imaging" in categories  # CPT-70100
        assert "DME" in categories  # HCPCS-E0100

    def test_surgical_category(self, minimal_claims_df: pl.DataFrame) -> None:
        tagged = tag_service_categories(minimal_claims_df)
        surgical = tagged.filter(pl.col("procedure_code") == "CPT-27100")
        assert surgical["service_category"][0] == "Surgical"


class TestProviderFeatures:
    def test_aggregation(self, minimal_claims_df: pl.DataFrame) -> None:
        tagged = tag_service_categories(minimal_claims_df)
        pf = compute_provider_features(tagged)
        assert "total_claims" in pf.columns
        assert "oon_rate" in pf.columns
        assert "denial_rate" in pf.columns
        assert "entity_age_days" in pf.columns
        # P001 has 2 claims
        p1 = pf.filter(pl.col("provider_id") == "P001")
        assert p1["total_claims"][0] == 2

    def test_oon_rate_correct(self, minimal_claims_df: pl.DataFrame) -> None:
        tagged = tag_service_categories(minimal_claims_df)
        pf = compute_provider_features(tagged)
        # P003 has 2 claims: 1 OON, 1 INN
        p3 = pf.filter(pl.col("provider_id") == "P003")
        assert p3["oon_rate"][0] == 0.5

    def test_denial_rate_correct(self, minimal_claims_df: pl.DataFrame) -> None:
        tagged = tag_service_categories(minimal_claims_df)
        pf = compute_provider_features(tagged)
        # P002 has 1 claim, 1 denial
        p2 = pf.filter(pl.col("provider_id") == "P002")
        assert p2["denial_rate"][0] == 1.0


class TestTemporalFeatures:
    def test_temporal_output(self, minimal_claims_df: pl.DataFrame) -> None:
        tf = compute_temporal_features(minimal_claims_df)
        assert "period_start" in tf.columns
        assert "period_type" in tf.columns
        assert "rolling_4w_claims" in tf.columns
        weekly = tf.filter(pl.col("period_type") == "weekly")
        monthly = tf.filter(pl.col("period_type") == "monthly")
        assert weekly.height > 0
        assert monthly.height > 0


class TestComputeAllFeatures:
    def test_returns_all_keys(self, minimal_claims_df: pl.DataFrame) -> None:
        features = compute_all_features(minimal_claims_df)
        assert "claims" in features
        assert "provider" in features
        assert "temporal" in features
        assert "service_category" in features
        assert "service_category" in features["claims"].columns
