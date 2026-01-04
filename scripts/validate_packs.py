#!/usr/bin/env python3
"""Validation script for decision pack JSON files.

Scans all pack.json and tenant packs.json files and validates them against schemas.
Exits with error code if any invalid files are found.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import jsonschema


def find_repo_root() -> Path:
    """Find the repository root by looking for pyproject.toml."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    # Fallback to current working directory
    return Path.cwd()


def validate_json_file(file_path: Path, schema_path: Path) -> tuple[bool, str | None]:
    """Validate a JSON file against a schema."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {e}"

    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
    except Exception as e:
        return False, f"Failed to load schema: {e}"

    try:
        jsonschema.validate(instance=data, schema=schema)
        return True, None
    except jsonschema.ValidationError as e:
        return False, f"Validation error: {e.message}"
    except jsonschema.SchemaError as e:
        return False, f"Schema error: {e.message}"


def main() -> int:
    """Main validation function."""
    repo_root = find_repo_root()
    packs_dir = repo_root / "decision_packs"
    tenants_dir = repo_root / "tenants"

    errors: list[str] = []

    # Validate pack definitions
    if packs_dir.exists():
        schema_path = packs_dir / "_schemas" / "decision_pack.schema.json"
        if not schema_path.exists():
            print(f"ERROR: Schema not found: {schema_path}", file=sys.stderr)
            return 1

        for pack_id_dir in packs_dir.iterdir():
            if not pack_id_dir.is_dir() or pack_id_dir.name.startswith("_"):
                continue

            for version_dir in pack_id_dir.iterdir():
                if not version_dir.is_dir():
                    continue

                pack_file = version_dir / "pack.json"
                if pack_file.exists():
                    valid, error = validate_json_file(pack_file, schema_path)
                    if not valid:
                        errors.append(f"{pack_file}: {error}")
                    else:
                        print(f"✓ {pack_file}")

    # Validate tenant enablements
    if tenants_dir.exists():
        schema_path = packs_dir / "_schemas" / "tenant_enablement.schema.json"
        if not schema_path.exists():
            print(f"ERROR: Schema not found: {schema_path}", file=sys.stderr)
            return 1

        for tenant_dir in tenants_dir.iterdir():
            if not tenant_dir.is_dir():
                continue

            packs_file = tenant_dir / "packs.json"
            if packs_file.exists():
                valid, error = validate_json_file(packs_file, schema_path)
                if not valid:
                    errors.append(f"{packs_file}: {error}")
                else:
                    print(f"✓ {packs_file}")

    # Report errors
    if errors:
        print("\nValidation errors:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1

    print("\nAll files validated successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())

