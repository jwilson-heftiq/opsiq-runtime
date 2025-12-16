from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opsiq_runtime.adapters.config.inline_config_provider import InlineConfigProvider
    from opsiq_runtime.adapters.events.noop_publisher import NoopEventPublisher
    from opsiq_runtime.adapters.inputs.in_memory_inputs_repository import InMemoryInputsRepository
    from opsiq_runtime.adapters.locks.noop_lock_manager import NoopLockManager
    from opsiq_runtime.ports.config_provider import ConfigProvider
    from opsiq_runtime.ports.event_publisher import EventPublisher
    from opsiq_runtime.ports.inputs_repository import InputsRepository
    from opsiq_runtime.ports.lock_manager import LockManager
    from opsiq_runtime.ports.outputs_repository import OutputsRepository

from opsiq_runtime.adapters.config.inline_config_provider import InlineConfigProvider
from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.adapters.databricks.inputs_repo import DatabricksInputsRepository
from opsiq_runtime.adapters.databricks.outputs_repo import DatabricksOutputsRepository
from opsiq_runtime.adapters.events.noop_publisher import NoopEventPublisher
from opsiq_runtime.adapters.inputs.in_memory_inputs_repository import InMemoryInputsRepository
from opsiq_runtime.adapters.locks.noop_lock_manager import NoopLockManager
from opsiq_runtime.adapters.outputs.file_outputs_repository import FileOutputsRepository
from opsiq_runtime.domain.common.ids import CorrelationId
from opsiq_runtime.settings import get_settings


def create_adapters(
    correlation_id: str | None = None,
) -> tuple[
    "ConfigProvider",
    "InputsRepository",
    "OutputsRepository",
    "EventPublisher",
    "LockManager",
]:
    """
    Factory function to create adapters based on RUNTIME_ADAPTERS environment variable.

    If RUNTIME_ADAPTERS=databricks, creates Databricks adapters.
    Otherwise, creates local adapters (default).
    """
    settings = get_settings()
    runtime_adapters = os.getenv("RUNTIME_ADAPTERS", "").lower()

    config_provider: ConfigProvider = InlineConfigProvider()
    event_publisher: EventPublisher = NoopEventPublisher()
    lock_manager: LockManager = NoopLockManager()

    if runtime_adapters == "databricks":
        # Validate required Databricks settings
        required_settings = [
            ("DATABRICKS_SERVER_HOSTNAME", settings.databricks_server_hostname),
            ("DATABRICKS_HTTP_PATH", settings.databricks_http_path),
            ("DATABRICKS_ACCESS_TOKEN", settings.databricks_access_token),
        ]
        missing = [name for name, value in required_settings if not value]
        if missing:
            raise ValueError(f"Missing required Databricks settings: {', '.join(missing)}")

        # Create Databricks client
        correlation_id_obj = CorrelationId(correlation_id) if correlation_id else None
        client = DatabricksSqlClient(settings, correlation_id_obj)

        # Create Databricks repositories
        inputs_repo: InputsRepository = DatabricksInputsRepository(client, settings)
        outputs_repo: OutputsRepository = DatabricksOutputsRepository(client, settings)

    else:
        # Local adapters (default behavior)
        inputs_repo = InMemoryInputsRepository()
        # Use FileOutputsRepository for consistency (CLI can redirect if needed)
        outputs_repo = FileOutputsRepository()

    return (config_provider, inputs_repo, outputs_repo, event_publisher, lock_manager)

