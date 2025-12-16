from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class VersionInfo:
    primitive_version: str
    canonical_version: str
    config_version: str

