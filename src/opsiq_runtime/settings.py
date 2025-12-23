from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Settings:
    """Runtime settings sourced from environment variables."""

    log_level: str = "INFO"
    default_at_risk_days: int = 30
    output_dir: str = "/tmp/opsiq-runtime-output"
    # Shopper frequency trend defaults
    default_baseline_window_days: int = 90
    default_min_baseline_trips: int = 4
    default_decline_ratio_threshold: float = 1.5
    default_improve_ratio_threshold: float = 0.75
    default_max_reasonable_gap_days: int = 365
    # Databricks settings
    databricks_server_hostname: Optional[str] = None
    databricks_http_path: Optional[str] = None
    databricks_access_token: Optional[str] = None
    databricks_catalog: Optional[str] = None
    databricks_schema: Optional[str] = None
    databricks_warehouse_timeout_seconds: int = 30
    databricks_table_prefix: str = ""
    databricks_use_merge: bool = True

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            log_level=os.getenv("LOG_LEVEL", cls.log_level),
            default_at_risk_days=int(os.getenv("DEFAULT_AT_RISK_DAYS", cls.default_at_risk_days)),
            output_dir=os.getenv("OUTPUT_DIR", cls.output_dir),
            default_baseline_window_days=int(os.getenv("DEFAULT_BASELINE_WINDOW_DAYS", cls.default_baseline_window_days)),
            default_min_baseline_trips=int(os.getenv("DEFAULT_MIN_BASELINE_TRIPS", cls.default_min_baseline_trips)),
            default_decline_ratio_threshold=float(os.getenv("DEFAULT_DECLINE_RATIO_THRESHOLD", cls.default_decline_ratio_threshold)),
            default_improve_ratio_threshold=float(os.getenv("DEFAULT_IMPROVE_RATIO_THRESHOLD", cls.default_improve_ratio_threshold)),
            default_max_reasonable_gap_days=int(os.getenv("DEFAULT_MAX_REASONABLE_GAP_DAYS", cls.default_max_reasonable_gap_days)),
            databricks_server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            databricks_http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            databricks_access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
            databricks_catalog=os.getenv("DATABRICKS_CATALOG"),
            databricks_schema=os.getenv("DATABRICKS_SCHEMA"),
            databricks_warehouse_timeout_seconds=int(
                os.getenv("DATABRICKS_WAREHOUSE_TIMEOUT_SECONDS", cls.databricks_warehouse_timeout_seconds)
            ),
            databricks_table_prefix=os.getenv("DATABRICKS_TABLE_PREFIX", cls.databricks_table_prefix),
            databricks_use_merge=os.getenv("DATABRICKS_USE_MERGE", "true").lower() in ("true", "1", "yes"),
        )


def get_settings(_cache: dict[str, Settings] = {}) -> Settings:
    """Provide a simple cached settings object."""

    if "settings" not in _cache:
        _cache["settings"] = Settings.from_env()
    return _cache["settings"]

