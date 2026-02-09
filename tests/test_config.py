"""Tests for config module."""

from um_claims.config import (
    PipelineConfig,
    PolicyChangeEvent,
    BenchmarkBaseline,
    DetectionConfig,
    get_service_category,
)
from datetime import date
import json


class TestPipelineConfig:
    def test_defaults(self) -> None:
        config = PipelineConfig()
        assert config.seed == 42
        assert config.num_claims == 100_000
        assert config.cost_per_appeal == 350.0
        assert len(config.policy_events) == 1
        assert len(config.benchmarks) == 3

    def test_json_round_trip(self) -> None:
        config = PipelineConfig(seed=99, num_claims=500)
        json_str = config.model_dump_json()
        restored = PipelineConfig.model_validate_json(json_str)
        assert restored.seed == 99
        assert restored.num_claims == 500

    def test_policy_event(self) -> None:
        event = PolicyChangeEvent(
            policy_id="P1",
            affected_procedure_prefixes=["CPT-7"],
            change_type="removed",
            effective_date=date(2024, 7, 1),
        )
        assert event.policy_id == "P1"

    def test_benchmark_baseline(self) -> None:
        b = BenchmarkBaseline(metric_name="denial_rate", baseline_value=0.08)
        assert b.threshold_pct == 0.10

    def test_detection_config_defaults(self) -> None:
        dc = DetectionConfig()
        assert dc.zscore_threshold == 2.0
        assert dc.new_entity_days == 90


class TestServiceCategory:
    def test_dme_mapping(self) -> None:
        assert get_service_category("HCPCS-E0100") == "DME"
        assert get_service_category("HCPCS-K0100") == "DME"

    def test_imaging_mapping(self) -> None:
        assert get_service_category("CPT-70100") == "Imaging"

    def test_em_mapping(self) -> None:
        assert get_service_category("CPT-99201") == "E&M"

    def test_surgical_mapping(self) -> None:
        assert get_service_category("CPT-27100") == "Surgical"

    def test_other_mapping(self) -> None:
        assert get_service_category("CPT-81000") == "Other"
        assert get_service_category("RX-01000") == "Other"
