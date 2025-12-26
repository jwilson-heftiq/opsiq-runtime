"""Unit tests for DecisionsRepository."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from opsiq_runtime.app.api.repositories.decisions_repo import DecisionsRepository
from opsiq_runtime.settings import Settings


@pytest.fixture
def mock_client() -> Mock:
    """Create a mock DatabricksSqlClient."""
    return Mock()


@pytest.fixture
def settings() -> Settings:
    """Create test settings."""
    return Settings(
        databricks_table_prefix="",
        databricks_catalog=None,
        databricks_schema=None,
    )


@pytest.fixture
def repository(mock_client: Mock, settings: Settings) -> DecisionsRepository:
    """Create a DecisionsRepository with mocked client."""
    return DecisionsRepository(mock_client, settings)


def test_decode_cursor_valid(repository: DecisionsRepository) -> None:
    """Test decoding a valid cursor."""
    computed_at = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    subject_id = "test_subject_123"
    data = {"computed_at": computed_at.isoformat(), "subject_id": subject_id}
    encoded = base64.b64encode(json.dumps(data).encode()).decode()

    decoded_ts, decoded_subject_id = repository._decode_cursor(encoded)

    assert decoded_ts == computed_at
    assert decoded_subject_id == subject_id


def test_decode_cursor_none(repository: DecisionsRepository) -> None:
    """Test decoding None cursor."""
    decoded_ts, decoded_subject_id = repository._decode_cursor(None)
    assert decoded_ts is None
    assert decoded_subject_id is None


def test_decode_cursor_invalid(repository: DecisionsRepository) -> None:
    """Test decoding an invalid cursor."""
    decoded_ts, decoded_subject_id = repository._decode_cursor("invalid_cursor")
    assert decoded_ts is None
    assert decoded_subject_id is None


def test_encode_cursor(repository: DecisionsRepository) -> None:
    """Test encoding a cursor."""
    computed_at = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    subject_id = "test_subject_123"

    encoded = repository._encode_cursor(computed_at, subject_id)

    # Decode and verify
    decoded = base64.b64decode(encoded.encode()).decode()
    data = json.loads(decoded)
    assert data["subject_id"] == subject_id
    assert datetime.fromisoformat(data["computed_at"]) == computed_at


def test_parse_json_field_valid_json(repository: DecisionsRepository) -> None:
    """Test parsing a valid JSON field."""
    json_str = '["driver1", "driver2"]'
    result = repository._parse_json_field(json_str, [])
    assert result == ["driver1", "driver2"]


def test_parse_json_field_invalid_json(repository: DecisionsRepository) -> None:
    """Test parsing an invalid JSON field returns default."""
    result = repository._parse_json_field("invalid json", [])
    assert result == []


def test_parse_json_field_none(repository: DecisionsRepository) -> None:
    """Test parsing None JSON field returns default."""
    result = repository._parse_json_field(None, [])
    assert result == []


def test_parse_json_field_dict_default(repository: DecisionsRepository) -> None:
    """Test parsing JSON field with dict default."""
    json_str = '{"key": "value"}'
    result = repository._parse_json_field(json_str, {})
    assert result == {"key": "value"}


def test_build_table_name_no_catalog_schema(repository: DecisionsRepository) -> None:
    """Test building table name without catalog or schema."""
    result = repository._build_table_name("test_table")
    assert result == "test_table"


def test_build_table_name_with_catalog_schema(repository: DecisionsRepository, settings: Settings, mock_client: Mock) -> None:
    """Test building table name with catalog and schema."""
    settings.databricks_catalog = "catalog"
    settings.databricks_schema = "schema"
    repo = DecisionsRepository(mock_client, settings)
    result = repo._build_table_name("test_table")
    assert result == "catalog.schema.test_table"


def test_row_to_decision_detail(repository: DecisionsRepository) -> None:
    """Test converting a database row to DecisionDetail."""
    row = {
        "tenant_id": "tenant1",
        "subject_type": "shopper",
        "subject_id": "subject1",
        "primitive_name": "shopper_health_classification",
        "primitive_version": "1.0.0",
        "canonical_version": "v1",
        "config_version": "cfg_v1",
        "as_of_ts": datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        "decision_state": "URGENT",
        "confidence": "HIGH",
        "computed_at": datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
        "valid_until": None,
        "drivers_json": '["LAPSE_RISK"]',
        "metrics_json": '{"key": 1.5}',
        "evidence_refs_json": '["evid1", "evid2"]',
        "correlation_id": "corr123",
    }

    detail = repository._row_to_decision_detail(row)

    assert detail.tenant_id == "tenant1"
    assert detail.subject_id == "subject1"
    assert detail.decision_state == "URGENT"
    assert detail.confidence == "HIGH"
    assert detail.drivers == ["LAPSE_RISK"]
    assert detail.metrics == {"key": 1.5}
    assert detail.evidence_refs == ["evid1", "evid2"]
    assert detail.correlation_id == "corr123"

