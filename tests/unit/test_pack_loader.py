"""Unit tests for pack loader service."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from opsiq_runtime.app.api.services.pack_loader import PackLoaderService
from opsiq_runtime.settings import Settings


@pytest.fixture
def temp_packs_dir():
    """Create a temporary directory structure for packs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        
        # Create schema directory
        schema_dir = base / "decision_packs" / "_schemas"
        schema_dir.mkdir(parents=True)
        
        # Create decision pack schema
        decision_pack_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["pack_id", "pack_version", "name", "status", "subjects", "primitives"],
            "properties": {
                "pack_id": {"type": "string"},
                "pack_version": {"type": "string"},
                "name": {"type": "string"},
                "status": {"type": "string", "enum": ["ACTIVE", "DEPRECATED", "INTERNAL"]},
                "subjects": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["subject_type", "default_worklist"],
                        "properties": {
                            "subject_type": {"type": "string"},
                            "default_worklist": {
                                "type": "object",
                                "required": ["title", "primitive_name", "ui_route"],
                                "properties": {
                                    "title": {"type": "string"},
                                    "primitive_name": {"type": "string"},
                                    "ui_route": {"type": "string"},
                                },
                            },
                        },
                    },
                },
                "primitives": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["primitive_name", "primitive_version", "canonical_version", "kind", "depends_on"],
                        "properties": {
                            "primitive_name": {"type": "string"},
                            "primitive_version": {"type": "string"},
                            "canonical_version": {"type": "string"},
                            "kind": {"type": "string"},
                            "depends_on": {
                                "type": "object",
                                "required": ["canonical_inputs", "primitives"],
                                "properties": {
                                    "canonical_inputs": {"type": "array", "items": {"type": "string"}},
                                    "primitives": {"type": "array", "items": {"type": "string"}},
                                },
                            },
                        },
                    },
                },
            },
        }
        with open(schema_dir / "decision_pack.schema.json", "w") as f:
            json.dump(decision_pack_schema, f)
        
        # Create tenant enablement schema
        tenant_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["tenant_id", "enabled_packs"],
            "properties": {
                "tenant_id": {"type": "string"},
                "enabled_packs": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["pack_id", "pack_version", "enabled", "config"],
                        "properties": {
                            "pack_id": {"type": "string"},
                            "pack_version": {"type": "string"},
                            "enabled": {"type": "boolean"},
                            "config": {"type": "object"},
                        },
                    },
                },
            },
        }
        with open(schema_dir / "tenant_enablement.schema.json", "w") as f:
            json.dump(tenant_schema, f)
        
        # Create a test pack
        pack_dir = base / "decision_packs" / "test_pack" / "1.0.0"
        pack_dir.mkdir(parents=True)
        
        pack_json = {
            "pack_id": "test_pack",
            "pack_version": "1.0.0",
            "name": "Test Pack",
            "description": "A test pack",
            "status": "ACTIVE",
            "tags": ["test"],
            "subjects": [
                {
                    "subject_type": "test_subject",
                    "default_worklist": {
                        "title": "Test Worklist",
                        "primitive_name": "test_primitive",
                        "ui_route": "/{tenantId}/test",
                    },
                }
            ],
            "primitives": [
                {
                    "primitive_name": "test_primitive",
                    "primitive_version": "1.0.0",
                    "canonical_version": "v1",
                    "kind": "primitive",
                    "depends_on": {
                        "canonical_inputs": [],
                        "primitives": [],
                    },
                }
            ],
        }
        with open(pack_dir / "pack.json", "w") as f:
            json.dump(pack_json, f)
        
        # Create a test tenant enablement
        tenant_dir = base / "tenants" / "test_tenant"
        tenant_dir.mkdir(parents=True)
        
        tenant_json = {
            "tenant_id": "test_tenant",
            "enabled_packs": [
                {
                    "pack_id": "test_pack",
                    "pack_version": "1.0.0",
                    "enabled": True,
                    "config": {
                        "default_config_version": "cfg_v1",
                    },
                }
            ],
        }
        with open(tenant_dir / "packs.json", "w") as f:
            json.dump(tenant_json, f)
        
        yield base


def test_load_valid_pack(temp_packs_dir):
    """Test loading a valid pack definition."""
    settings = Settings(packs_base_dir=str(temp_packs_dir))
    loader = PackLoaderService(settings)
    
    pack = loader.get_pack_definition("test_pack", "1.0.0")
    
    assert pack["pack_id"] == "test_pack"
    assert pack["pack_version"] == "1.0.0"
    assert pack["name"] == "Test Pack"
    assert len(pack["subjects"]) == 1
    assert len(pack["primitives"]) == 1


def test_load_invalid_pack_schema(temp_packs_dir):
    """Test that invalid pack JSON raises an error."""
    settings = Settings(packs_base_dir=str(temp_packs_dir))
    loader = PackLoaderService(settings)
    
    # Create an invalid pack (missing required field)
    invalid_pack_dir = temp_packs_dir / "decision_packs" / "invalid_pack" / "1.0.0"
    invalid_pack_dir.mkdir(parents=True)
    
    invalid_pack = {
        "pack_id": "invalid_pack",
        # Missing required fields
    }
    with open(invalid_pack_dir / "pack.json", "w") as f:
        json.dump(invalid_pack, f)
    
    with pytest.raises((ValueError, FileNotFoundError)):
        loader.get_pack_definition("invalid_pack", "1.0.0")


def test_load_tenant_enablement(temp_packs_dir):
    """Test loading tenant enablement."""
    settings = Settings(packs_base_dir=str(temp_packs_dir))
    loader = PackLoaderService(settings)
    
    enablement = loader.get_tenant_enablement("test_tenant")
    
    assert enablement["tenant_id"] == "test_tenant"
    assert len(enablement["enabled_packs"]) == 1
    assert enablement["enabled_packs"][0]["pack_id"] == "test_pack"
    assert enablement["enabled_packs"][0]["enabled"] is True


def test_pack_not_found(temp_packs_dir):
    """Test that missing pack raises FileNotFoundError."""
    settings = Settings(packs_base_dir=str(temp_packs_dir))
    loader = PackLoaderService(settings)
    
    with pytest.raises(FileNotFoundError):
        loader.get_pack_definition("nonexistent_pack", "1.0.0")


def test_tenant_not_found(temp_packs_dir):
    """Test that missing tenant raises FileNotFoundError."""
    settings = Settings(packs_base_dir=str(temp_packs_dir))
    loader = PackLoaderService(settings)
    
    with pytest.raises(FileNotFoundError):
        loader.get_tenant_enablement("nonexistent_tenant")


def test_cache_behavior(temp_packs_dir):
    """Test that pack loader caches results."""
    settings = Settings(packs_base_dir=str(temp_packs_dir))
    loader = PackLoaderService(settings)
    
    # First load
    pack1 = loader.get_pack_definition("test_pack", "1.0.0")
    
    # Second load should use cache (we can't easily test this without mocking time,
    # but we can verify it doesn't raise an error)
    pack2 = loader.get_pack_definition("test_pack", "1.0.0")
    
    assert pack1 == pack2


def test_scan_pack_directory(temp_packs_dir):
    """Test scanning pack directory."""
    settings = Settings(packs_base_dir=str(temp_packs_dir))
    loader = PackLoaderService(settings)
    
    packs = loader.list_all_packs()
    
    assert ("test_pack", "1.0.0") in packs

