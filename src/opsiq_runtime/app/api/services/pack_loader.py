"""Service for loading and validating decision pack definitions."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import jsonschema

from opsiq_runtime.settings import Settings

logger = logging.getLogger(__name__)


class PackLoaderService:
    """Service for loading and validating decision pack definitions."""

    CACHE_TTL_SECONDS = 60

    def __init__(self, settings: Settings) -> None:
        """Initialize the pack loader service."""
        self.settings = settings
        self.base_dir = Path(settings.packs_base_dir)
        self._pack_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._tenant_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._error_cache: dict[str, tuple[float, Exception]] = {}

    def _validate_json(self, data: dict[str, Any], schema_path: Path) -> None:
        """Validate JSON data against a JSON schema."""
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)

        try:
            jsonschema.validate(instance=data, schema=schema)
        except jsonschema.ValidationError as e:
            raise ValueError(f"JSON validation failed: {e.message}") from e
        except jsonschema.SchemaError as e:
            raise ValueError(f"Schema error: {e.message}") from e

    def _load_pack_definition(self, pack_id: str, pack_version: str) -> dict[str, Any]:
        """Load and validate a pack definition JSON file."""
        cache_key = f"{pack_id}:{pack_version}"
        now = time.time()

        # Check cache
        if cache_key in self._pack_cache:
            cached_time, cached_data = self._pack_cache[cache_key]
            if now - cached_time < self.CACHE_TTL_SECONDS:
                return cached_data

        # Check error cache
        if cache_key in self._error_cache:
            cached_time, cached_error = self._error_cache[cache_key]
            if now - cached_time < self.CACHE_TTL_SECONDS:
                raise cached_error

        # Load pack file
        pack_path = self.base_dir / "decision_packs" / pack_id / pack_version / "pack.json"
        if not pack_path.exists():
            error = FileNotFoundError(f"Pack definition not found: {pack_path}")
            self._error_cache[cache_key] = (now, error)
            raise error

        # Load and parse JSON
        try:
            with open(pack_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            error = ValueError(f"Invalid JSON in pack file {pack_path}: {e}")
            self._error_cache[cache_key] = (now, error)
            raise error

        # Validate against schema
        schema_path = self.base_dir / "decision_packs" / "_schemas" / "decision_pack.schema.json"
        try:
            self._validate_json(data, schema_path)
        except Exception as e:
            self._error_cache[cache_key] = (now, e)
            raise

        # Cache and return
        self._pack_cache[cache_key] = (now, data)
        return data

    def _load_tenant_enablement(self, tenant_id: str) -> dict[str, Any]:
        """Load and validate a tenant enablement JSON file."""
        cache_key = tenant_id
        now = time.time()

        # Check cache
        if cache_key in self._tenant_cache:
            cached_time, cached_data = self._tenant_cache[cache_key]
            if now - cached_time < self.CACHE_TTL_SECONDS:
                return cached_data

        # Check error cache
        if cache_key in self._error_cache:
            cached_time, cached_error = self._error_cache[cache_key]
            if now - cached_time < self.CACHE_TTL_SECONDS:
                raise cached_error

        # Load tenant file
        tenant_path = self.base_dir / "tenants" / tenant_id / "packs.json"
        if not tenant_path.exists():
            error = FileNotFoundError(f"Tenant enablement not found: {tenant_path}")
            self._error_cache[cache_key] = (now, error)
            raise error

        # Load and parse JSON
        try:
            with open(tenant_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            error = ValueError(f"Invalid JSON in tenant file {tenant_path}: {e}")
            self._error_cache[cache_key] = (now, error)
            raise error

        # Validate against schema
        schema_path = self.base_dir / "decision_packs" / "_schemas" / "tenant_enablement.schema.json"
        try:
            self._validate_json(data, schema_path)
        except Exception as e:
            self._error_cache[cache_key] = (now, e)
            raise

        # Cache and return
        self._tenant_cache[cache_key] = (now, data)
        return data

    def _scan_pack_directory(self) -> list[tuple[str, str]]:
        """Scan the decision_packs directory for all pack.json files."""
        packs_dir = self.base_dir / "decision_packs"
        if not packs_dir.exists():
            logger.warning(f"Decision packs directory not found: {packs_dir}")
            return []

        packs: list[tuple[str, str]] = []
        for pack_id_dir in packs_dir.iterdir():
            if not pack_id_dir.is_dir() or pack_id_dir.name.startswith("_"):
                continue

            pack_id = pack_id_dir.name
            for version_dir in pack_id_dir.iterdir():
                if not version_dir.is_dir():
                    continue

                pack_version = version_dir.name
                pack_file = version_dir / "pack.json"
                if pack_file.exists():
                    packs.append((pack_id, pack_version))

        return packs

    def get_pack_definition(self, pack_id: str, pack_version: str) -> dict[str, Any]:
        """Get a pack definition by ID and version."""
        return self._load_pack_definition(pack_id, pack_version)

    def get_tenant_enablement(self, tenant_id: str) -> dict[str, Any]:
        """Get tenant enablement configuration."""
        return self._load_tenant_enablement(tenant_id)

    def list_all_packs(self) -> list[tuple[str, str]]:
        """List all available packs (pack_id, pack_version)."""
        return self._scan_pack_directory()

