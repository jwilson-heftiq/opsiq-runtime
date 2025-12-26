"""Repository for querying run registry data from Databricks."""

from __future__ import annotations

import base64
import json
import logging
from datetime import datetime
from typing import Any

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.app.api.models.runs import RunRegistryItem, RunRegistryResponse
from opsiq_runtime.settings import Settings

logger = logging.getLogger(__name__)


class RunsRepository:
    """Repository for querying run registry from Databricks."""

    def __init__(self, client: DatabricksSqlClient, settings: Settings) -> None:
        self.client = client
        self.settings = settings
        self.run_registry_table_name = f"{settings.databricks_table_prefix}gold_runtime_run_registry_v1"

    def _build_table_name(self, table_name: str) -> str:
        """Build fully qualified table name with catalog and schema if specified."""
        parts = []
        if self.settings.databricks_catalog:
            parts.append(self.settings.databricks_catalog)
        if self.settings.databricks_schema:
            parts.append(self.settings.databricks_schema)
        parts.append(table_name)
        return ".".join(parts)

    def _decode_cursor(self, cursor: str | None) -> tuple[datetime | None, str | None]:
        """Decode a pagination cursor to (started_at, correlation_id)."""
        if not cursor:
            return (None, None)
        try:
            decoded = base64.b64decode(cursor.encode()).decode()
            data = json.loads(decoded)
            started_at = datetime.fromisoformat(data["started_at"])
            correlation_id = data["correlation_id"]
            return (started_at, correlation_id)
        except Exception as e:
            logger.warning(f"Failed to decode cursor {cursor}: {e}")
            return (None, None)

    def _encode_cursor(self, started_at: datetime, correlation_id: str) -> str:
        """Encode a pagination cursor from (started_at, correlation_id)."""
        data = {"started_at": started_at.isoformat(), "correlation_id": correlation_id}
        encoded = json.dumps(data).encode()
        return base64.b64encode(encoded).decode()

    def get_run_registry(
        self,
        tenant_id: str,
        primitive_name: str | None = None,
        status: str | None = None,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> RunRegistryResponse:
        """
        Get run registry for a tenant.

        Args:
            tenant_id: Tenant ID to filter by
            primitive_name: Optional primitive name to filter by
            status: Optional status to filter by (STARTED/SUCCESS/FAILED)
            from_ts: Optional start timestamp for started_at filter
            to_ts: Optional end timestamp for started_at filter
            limit: Maximum number of results (default 50)
            cursor: Optional pagination cursor

        Returns:
            RunRegistryResponse with items and next_cursor
        """
        run_registry_table = self._build_table_name(self.run_registry_table_name)
        cursor_started_at, cursor_correlation_id = self._decode_cursor(cursor)

        # Build WHERE conditions
        conditions = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]

        # Add cursor condition for keyset pagination
        if cursor_started_at and cursor_correlation_id:
            conditions.append(
                "((started_at < ?) OR (started_at = ? AND correlation_id < ?))"
            )
            params.extend([cursor_started_at.isoformat(), cursor_started_at.isoformat(), cursor_correlation_id])

        # Add primitive_name filter
        if primitive_name:
            conditions.append("primitive_name = ?")
            params.append(primitive_name)

        # Add status filter
        if status:
            conditions.append("status = ?")
            params.append(status)

        # Add started_at range filters
        if from_ts:
            conditions.append("started_at >= ?")
            params.append(from_ts.isoformat())

        if to_ts:
            conditions.append("started_at <= ?")
            params.append(to_ts.isoformat())

        where_clause = " AND ".join(conditions)

        # SQL query sorted by started_at DESC, correlation_id DESC
        sql = f"""
        SELECT
            correlation_id,
            primitive_name,
            primitive_version,
            status,
            started_at,
            completed_at,
            duration_ms,
            input_count,
            decision_count,
            at_risk_count,
            unknown_count,
            error_message
        FROM {run_registry_table}
        WHERE {where_clause}
        ORDER BY started_at DESC, correlation_id DESC
        LIMIT ?
        """

        params.append(limit + 1)  # Fetch one extra to determine if there's a next page

        try:
            rows = self.client.query(sql, params)
        except Exception as e:
            logger.error(f"Error querying run registry: {e}")
            raise

        # Convert rows to RunRegistryItem
        items: list[RunRegistryItem] = []
        for row in rows[:limit]:  # Take only up to limit
            try:
                started_at = row.get("started_at")
                if isinstance(started_at, str):
                    started_at = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                elif not isinstance(started_at, datetime):
                    logger.warning(f"Invalid started_at format: {started_at}")
                    continue

                completed_at = row.get("completed_at")
                if completed_at:
                    if isinstance(completed_at, str):
                        completed_at = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
                    elif not isinstance(completed_at, datetime):
                        completed_at = None
                else:
                    completed_at = None

                # Calculate duration_ms if both started_at and completed_at are present
                duration_ms = row.get("duration_ms")
                if duration_ms is None and started_at and completed_at:
                    duration_ms = int((completed_at - started_at).total_seconds() * 1000)
                elif duration_ms is not None:
                    try:
                        duration_ms = int(duration_ms)
                    except (ValueError, TypeError):
                        duration_ms = None

                item = RunRegistryItem(
                    correlation_id=str(row["correlation_id"]),
                    primitive_name=str(row["primitive_name"]),
                    primitive_version=str(row["primitive_version"]),
                    status=str(row["status"]),
                    started_at=started_at,
                    completed_at=completed_at,
                    duration_ms=duration_ms,
                    input_count=int(row["input_count"]) if row.get("input_count") is not None else None,
                    decision_count=int(row["decision_count"]) if row.get("decision_count") is not None else None,
                    at_risk_count=int(row["at_risk_count"]) if row.get("at_risk_count") is not None else None,
                    unknown_count=int(row["unknown_count"]) if row.get("unknown_count") is not None else None,
                    error_message=str(row["error_message"]) if row.get("error_message") else None,
                )
                items.append(item)
            except Exception as e:
                logger.warning(f"Error parsing run registry row: {e}")
                continue

        # Generate next cursor if we fetched more than limit
        next_cursor = None
        if len(rows) > limit:
            last_item = items[-1]
            next_cursor = self._encode_cursor(last_item.started_at, last_item.correlation_id)

        return RunRegistryResponse(items=items, next_cursor=next_cursor)

