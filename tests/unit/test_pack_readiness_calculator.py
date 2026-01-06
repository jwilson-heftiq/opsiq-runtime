"""Unit tests for pack readiness calculator."""

from datetime import datetime, timezone, timedelta

import pytest

from opsiq_runtime.app.api.services.pack_readiness.calculator import PackReadinessCalculator
from opsiq_runtime.app.api.services.pack_readiness.models import (
    CanonicalFreshnessResult,
    DecisionHealthResult,
    RollupIntegrityResult,
)


class TestPackReadinessCalculator:
    """Test pack readiness calculator logic."""

    def test_canonical_freshness_pass(self):
        """Test canonical freshness PASS status."""
        calculator = PackReadinessCalculator(freshness_threshold_hours=36.0)
        now = datetime.now(timezone.utc)
        last_ts = now - timedelta(hours=12)

        result = calculator.calculate_canonical_freshness("test_table", last_ts)

        assert result.status == "PASS"
        assert result.last_as_of_ts == last_ts
        assert result.hours_since_last_update == pytest.approx(12.0, abs=0.1)

    def test_canonical_freshness_warn(self):
        """Test canonical freshness WARN status."""
        calculator = PackReadinessCalculator(freshness_threshold_hours=36.0)
        now = datetime.now(timezone.utc)
        last_ts = now - timedelta(hours=48)

        result = calculator.calculate_canonical_freshness("test_table", last_ts)

        assert result.status == "WARN"
        assert result.hours_since_last_update == pytest.approx(48.0, abs=0.1)

    def test_canonical_freshness_fail_no_data(self):
        """Test canonical freshness FAIL status when no data."""
        calculator = PackReadinessCalculator()

        result = calculator.calculate_canonical_freshness("test_table", None)

        assert result.status == "FAIL"
        assert result.last_as_of_ts is None
        assert result.hours_since_last_update is None

    def test_decision_health_pass(self):
        """Test decision health PASS status."""
        calculator = PackReadinessCalculator()
        state_counts = {"AT_RISK": 10, "NOT_AT_RISK": 90, "UNKNOWN": 5}

        result = calculator.calculate_decision_health(
            "test_primitive", 105, state_counts, datetime.now(timezone.utc)
        )

        assert result.status == "PASS"
        assert result.unknown_rate == pytest.approx(5 / 105, abs=0.001)

    def test_decision_health_warn(self):
        """Test decision health WARN status (30% unknown)."""
        calculator = PackReadinessCalculator(unknown_rate_warn_threshold=0.30)
        state_counts = {"AT_RISK": 10, "NOT_AT_RISK": 60, "UNKNOWN": 30}

        result = calculator.calculate_decision_health(
            "test_primitive", 100, state_counts, datetime.now(timezone.utc)
        )

        assert result.status == "WARN"
        assert result.unknown_rate == 0.30

    def test_decision_health_fail(self):
        """Test decision health FAIL status (60% unknown)."""
        calculator = PackReadinessCalculator(unknown_rate_fail_threshold=0.60)
        state_counts = {"AT_RISK": 10, "NOT_AT_RISK": 30, "UNKNOWN": 60}

        result = calculator.calculate_decision_health(
            "test_primitive", 100, state_counts, datetime.now(timezone.utc)
        )

        assert result.status == "FAIL"
        assert result.unknown_rate == 0.60

    def test_decision_health_fail_no_decisions(self):
        """Test decision health FAIL status when no decisions."""
        calculator = PackReadinessCalculator()

        result = calculator.calculate_decision_health(
            "test_primitive", 0, {}, None
        )

        assert result.status == "FAIL"
        assert result.total_decisions == 0

    def test_rollup_integrity_pass(self):
        """Test rollup integrity PASS status."""
        calculator = PackReadinessCalculator()

        result = calculator.calculate_rollup_integrity("test_check", 100, 98)

        assert result.status == "PASS"
        assert result.pass_rate == 0.98

    def test_rollup_integrity_warn(self):
        """Test rollup integrity WARN status (90% pass rate)."""
        calculator = PackReadinessCalculator(integrity_warn_threshold=0.95)

        result = calculator.calculate_rollup_integrity("test_check", 100, 90)

        assert result.status == "WARN"
        assert result.pass_rate == 0.90

    def test_rollup_integrity_fail(self):
        """Test rollup integrity FAIL status (70% pass rate)."""
        calculator = PackReadinessCalculator(integrity_fail_threshold=0.80)

        result = calculator.calculate_rollup_integrity("test_check", 100, 70)

        assert result.status == "FAIL"
        assert result.pass_rate == 0.70

    def test_rollup_integrity_fail_no_data(self):
        """Test rollup integrity FAIL status when no data (default behavior)."""
        calculator = PackReadinessCalculator()

        result = calculator.calculate_rollup_integrity("test_check", 0, 0)

        assert result.status == "FAIL"
        assert result.pass_rate == 0.0

    def test_rollup_integrity_warn_no_data(self):
        """Test rollup integrity WARN status when no data and zero_total_status='WARN'."""
        calculator = PackReadinessCalculator()

        result = calculator.calculate_rollup_integrity("test_check", 0, 0, zero_total_status="WARN")

        assert result.status == "WARN"
        assert result.pass_rate == 0.0

    def test_aggregate_status_fail_priority(self):
        """Test that FAIL status takes priority over WARN and PASS."""
        calculator = PackReadinessCalculator()
        freshness = [CanonicalFreshnessResult(table="t1", last_as_of_ts=None, hours_since_last_update=None, status="FAIL")]
        health = [DecisionHealthResult(primitive_name="p1", total_decisions=100, state_counts={}, unknown_rate=0.1, last_computed_at=None, status="PASS")]
        integrity = [RollupIntegrityResult(check="c1", pass_rate=0.99, status="PASS")]

        status = calculator.aggregate_status(freshness, health, integrity)

        assert status == "FAIL"

    def test_aggregate_status_warn_priority(self):
        """Test that WARN status takes priority over PASS."""
        calculator = PackReadinessCalculator()
        freshness = [CanonicalFreshnessResult(table="t1", last_as_of_ts=datetime.now(timezone.utc), hours_since_last_update=40.0, status="WARN")]
        health = [DecisionHealthResult(primitive_name="p1", total_decisions=100, state_counts={}, unknown_rate=0.1, last_computed_at=None, status="PASS")]
        integrity = [RollupIntegrityResult(check="c1", pass_rate=0.99, status="PASS")]

        status = calculator.aggregate_status(freshness, health, integrity)

        assert status == "WARN"

    def test_aggregate_status_all_pass(self):
        """Test that all PASS results aggregate to PASS."""
        calculator = PackReadinessCalculator()
        now = datetime.now(timezone.utc)
        freshness = [CanonicalFreshnessResult(table="t1", last_as_of_ts=now, hours_since_last_update=12.0, status="PASS")]
        health = [DecisionHealthResult(primitive_name="p1", total_decisions=100, state_counts={}, unknown_rate=0.05, last_computed_at=now, status="PASS")]
        integrity = [RollupIntegrityResult(check="c1", pass_rate=0.99, status="PASS")]

        status = calculator.aggregate_status(freshness, health, integrity)

        assert status == "PASS"

    def test_aggregate_status_empty_results(self):
        """Test that empty results aggregate to FAIL."""
        calculator = PackReadinessCalculator()

        status = calculator.aggregate_status([], [], [])

        assert status == "FAIL"

    def test_build_readiness_response(self):
        """Test building complete readiness response."""
        calculator = PackReadinessCalculator()
        now = datetime.now(timezone.utc)
        freshness = [CanonicalFreshnessResult(table="t1", last_as_of_ts=now, hours_since_last_update=12.0, status="PASS")]
        health = [DecisionHealthResult(primitive_name="p1", total_decisions=100, state_counts={}, unknown_rate=0.05, last_computed_at=now, status="PASS")]
        integrity = [RollupIntegrityResult(check="c1", pass_rate=0.99, status="PASS")]

        response = calculator.build_readiness_response(
            tenant_id="test_tenant",
            pack_id="test_pack",
            pack_version="1.0.0",
            canonical_freshness=freshness,
            decision_health=health,
            rollup_integrity=integrity,
        )

        assert response.tenant_id == "test_tenant"
        assert response.pack_id == "test_pack"
        assert response.pack_version == "1.0.0"
        assert response.overall_status == "PASS"
        assert len(response.canonical_freshness) == 1
        assert len(response.decision_health) == 1
        assert len(response.rollup_integrity) == 1
        assert response.computed_at is not None

