"""Repository for querying decision and evidence data from Databricks."""

from __future__ import annotations

import base64
import json
import logging
from datetime import datetime
from typing import Any

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.app.api.models.decisions import (
    DecisionBundle,
    DecisionDetail,
    DecisionListResponse,
    DecisionListItem,
    EvidenceRecord,
)
from opsiq_runtime.settings import Settings

logger = logging.getLogger(__name__)


class DecisionsRepository:
    """Repository for querying decisions and evidence from Databricks."""

    def __init__(self, client: DatabricksSqlClient, settings: Settings) -> None:
        self.client = client
        self.settings = settings
        self.decision_table_name = f"{settings.databricks_table_prefix}gold_decision_output_v1"
        self.evidence_table_name = f"{settings.databricks_table_prefix}gold_decision_evidence_v1"

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
        """Decode a pagination cursor to (computed_at, subject_id)."""
        if not cursor:
            return (None, None)
        try:
            decoded = base64.b64decode(cursor.encode()).decode()
            data = json.loads(decoded)
            computed_at = datetime.fromisoformat(data["computed_at"])
            subject_id = data["subject_id"]
            return (computed_at, subject_id)
        except Exception as e:
            logger.warning(f"Failed to decode cursor {cursor}: {e}")
            return (None, None)

    def _encode_cursor(self, computed_at: datetime, subject_id: str) -> str:
        """Encode a pagination cursor from (computed_at, subject_id)."""
        data = {"computed_at": computed_at.isoformat(), "subject_id": subject_id}
        encoded = json.dumps(data).encode()
        return base64.b64encode(encoded).decode()

    def _parse_json_field(self, value: str | None, default: Any = None) -> Any:
        """Parse a JSON field from the database."""
        if not value:
            return default if default is not None else []
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse JSON field: {e}")
            return default if default is not None else []

    def get_worklist(
        self,
        tenant_id: str,
        state: list[str] | None = None,
        confidence: list[str] | None = None,
        subject_id_filter: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> DecisionListResponse:
        """
        Get worklist of latest shopper_health_classification decisions per subject.

        Args:
            tenant_id: Tenant ID to filter by
            state: Optional list of decision states to filter by (URGENT/WATCHLIST/HEALTHY/UNKNOWN)
            confidence: Optional list of confidence levels to filter by (HIGH/MEDIUM/LOW)
            subject_id_filter: Optional substring match on subject_id
            limit: Maximum number of results (default 50, max 200)
            cursor: Optional pagination cursor

        Returns:
            DecisionListResponse with items and next_cursor
        """
        # Enforce max limit
        limit = min(limit, 200)

        decision_table = self._build_table_name(self.decision_table_name)
        cursor_ts, cursor_subject_id = self._decode_cursor(cursor)

        # Build WHERE conditions
        conditions = [
            "tenant_id = ?",
            "subject_type = 'shopper'",
            "primitive_name = 'shopper_health_classification'",
        ]
        params: list[Any] = [tenant_id]

        # Add cursor condition for keyset pagination
        if cursor_ts and cursor_subject_id:
            conditions.append(
                "((computed_at < ?) OR (computed_at = ? AND subject_id < ?))"
            )
            params.extend([cursor_ts.isoformat(), cursor_ts.isoformat(), cursor_subject_id])

        # Add state filter
        if state:
            placeholders = ",".join(["?"] * len(state))
            conditions.append(f"decision_state IN ({placeholders})")
            params.extend(state)

        # Add confidence filter
        if confidence:
            placeholders = ",".join(["?"] * len(confidence))
            conditions.append(f"confidence IN ({placeholders})")
            params.extend(confidence)

        # Add subject_id substring filter
        if subject_id_filter:
            conditions.append("subject_id LIKE ?")
            params.append(f"%{subject_id_filter}%")

        where_clause = " AND ".join(conditions)

        # SQL query with window function to get latest per subject
        sql = f"""
        WITH ranked_decisions AS (
            SELECT
                tenant_id,
                subject_type,
                subject_id,
                primitive_name,
                primitive_version,
                canonical_version,
                config_version,
                as_of_ts,
                decision_state,
                confidence,
                drivers_json,
                metrics_json,
                evidence_refs_json,
                computed_at,
                valid_until,
                correlation_id,
                ROW_NUMBER() OVER (
                    PARTITION BY subject_id
                    ORDER BY computed_at DESC, as_of_ts DESC
                ) as rn
            FROM {decision_table}
            WHERE {where_clause}
        )
        SELECT
            tenant_id,
            subject_type,
            subject_id,
            primitive_name,
            primitive_version,
            canonical_version,
            config_version,
            as_of_ts,
            decision_state,
            confidence,
            drivers_json,
            metrics_json,
            evidence_refs_json,
            computed_at,
            valid_until,
            correlation_id
        FROM ranked_decisions
        WHERE rn = 1
        ORDER BY computed_at DESC, subject_id DESC
        LIMIT ?
        """

        params.append(limit + 1)  # Fetch one extra to determine if there's a next page

        try:
            rows = self.client.query(sql, params)
        except Exception as e:
            logger.error(f"Error querying worklist: {e}")
            raise

        # Convert rows to DecisionListItem
        items: list[DecisionListItem] = []
        for row in rows[:limit]:  # Take only up to limit
            try:
                as_of_ts = row.get("as_of_ts")
                if isinstance(as_of_ts, str):
                    as_of_ts = datetime.fromisoformat(as_of_ts.replace("Z", "+00:00"))
                elif not isinstance(as_of_ts, datetime):
                    logger.warning(f"Invalid as_of_ts format: {as_of_ts}")
                    continue

                computed_at = row.get("computed_at")
                if isinstance(computed_at, str):
                    computed_at = datetime.fromisoformat(computed_at.replace("Z", "+00:00"))
                elif not isinstance(computed_at, datetime):
                    logger.warning(f"Invalid computed_at format: {computed_at}")
                    continue

                item = DecisionListItem(
                    tenant_id=str(row["tenant_id"]),
                    subject_type=str(row["subject_type"]),
                    subject_id=str(row["subject_id"]),
                    primitive_name=str(row["primitive_name"]),
                    primitive_version=str(row["primitive_version"]),
                    as_of_ts=as_of_ts,
                    decision_state=str(row["decision_state"]),
                    confidence=str(row["confidence"]),
                    computed_at=computed_at,
                    drivers=self._parse_json_field(row.get("drivers_json"), []),
                    metrics=self._parse_json_field(row.get("metrics_json"), {}),
                )
                items.append(item)
            except Exception as e:
                logger.warning(f"Error parsing worklist row: {e}")
                continue

        # Generate next cursor if we fetched more than limit
        next_cursor = None
        if len(rows) > limit:
            last_item = items[-1]
            next_cursor = self._encode_cursor(last_item.computed_at, last_item.subject_id)

        return DecisionListResponse(items=items, next_cursor=next_cursor)

    def get_decision_bundle(
        self,
        tenant_id: str,
        subject_id: str,
        as_of_ts: datetime | None = None,
        include_evidence: bool = True,
    ) -> DecisionBundle:
        """
        Get decision bundle for a subject including composite, components, and evidence.

        Args:
            tenant_id: Tenant ID
            subject_id: Subject ID
            as_of_ts: Optional as_of timestamp for the composite decision (if None, gets latest)
            include_evidence: Whether to include evidence (default True)

        Returns:
            DecisionBundle with composite, components, and evidence
        """
        decision_table = self._build_table_name(self.decision_table_name)
        evidence_table = self._build_table_name(self.evidence_table_name)

        # Fetch composite decision
        composite = self._fetch_composite_decision(
            decision_table, tenant_id, subject_id, as_of_ts
        )
        if not composite:
            raise ValueError(
                f"No composite decision found for tenant_id={tenant_id}, subject_id={subject_id}"
            )

        # Fetch component decisions (operational_risk, shopper_frequency_trend)
        components = self._fetch_component_decisions(
            decision_table, tenant_id, subject_id, composite.computed_at
        )

        # Fetch evidence if requested
        evidence: dict[str, list[EvidenceRecord]] = {}
        if include_evidence:
            evidence = self._fetch_evidence(
                evidence_table, tenant_id, composite, components
            )

        return DecisionBundle(composite=composite, components=components, evidence=evidence)

    def _fetch_composite_decision(
        self,
        table_name: str,
        tenant_id: str,
        subject_id: str,
        as_of_ts: datetime | None,
    ) -> DecisionDetail | None:
        """Fetch the composite decision (shopper_health_classification)."""
        conditions = [
            "tenant_id = ?",
            "subject_type = 'shopper'",
            "subject_id = ?",
            "primitive_name = 'shopper_health_classification'",
        ]
        params: list[Any] = [tenant_id, subject_id]

        if as_of_ts:
            conditions.append("as_of_ts = ?")
            params.append(as_of_ts.isoformat())
            order_by = "computed_at DESC"
        else:
            order_by = "computed_at DESC, as_of_ts DESC"

        where_clause = " AND ".join(conditions)

        sql = f"""
        SELECT
            tenant_id,
            subject_type,
            subject_id,
            primitive_name,
            primitive_version,
            canonical_version,
            config_version,
            as_of_ts,
            decision_state,
            confidence,
            drivers_json,
            metrics_json,
            evidence_refs_json,
            computed_at,
            valid_until,
            correlation_id
        FROM {table_name}
        WHERE {where_clause}
        ORDER BY {order_by}
        LIMIT 1
        """

        try:
            rows = self.client.query(sql, params)
        except Exception as e:
            logger.error(f"Error fetching composite decision: {e}")
            raise

        if not rows:
            return None

        row = rows[0]
        return self._row_to_decision_detail(row)

    def _fetch_component_decisions(
        self,
        table_name: str,
        tenant_id: str,
        subject_id: str,
        max_computed_at: datetime,
    ) -> dict[str, DecisionDetail]:
        """Fetch component decisions (operational_risk, shopper_frequency_trend)."""
        sql = f"""
        WITH ranked_decisions AS (
            SELECT
                tenant_id,
                subject_type,
                subject_id,
                primitive_name,
                primitive_version,
                canonical_version,
                config_version,
                as_of_ts,
                decision_state,
                confidence,
                drivers_json,
                metrics_json,
                evidence_refs_json,
                computed_at,
                valid_until,
                correlation_id,
                ROW_NUMBER() OVER (
                    PARTITION BY primitive_name
                    ORDER BY computed_at DESC, as_of_ts DESC
                ) as rn
            FROM {table_name}
            WHERE tenant_id = ?
                AND subject_type = 'shopper'
                AND subject_id = ?
                AND primitive_name IN ('operational_risk', 'shopper_frequency_trend')
                AND computed_at <= ?
        )
        SELECT
            tenant_id,
            subject_type,
            subject_id,
            primitive_name,
            primitive_version,
            canonical_version,
            config_version,
            as_of_ts,
            decision_state,
            confidence,
            drivers_json,
            metrics_json,
            evidence_refs_json,
            computed_at,
            valid_until,
            correlation_id
        FROM ranked_decisions
        WHERE rn = 1
        """

        params = [tenant_id, subject_id, max_computed_at.isoformat()]

        try:
            rows = self.client.query(sql, params)
        except Exception as e:
            logger.error(f"Error fetching component decisions: {e}")
            raise

        components: dict[str, DecisionDetail] = {}
        for row in rows:
            try:
                detail = self._row_to_decision_detail(row)
                components[detail.primitive_name] = detail
            except Exception as e:
                logger.warning(f"Error parsing component decision: {e}")
                continue

        return components

    def _fetch_evidence(
        self,
        table_name: str,
        tenant_id: str,
        composite: DecisionDetail,
        components: dict[str, DecisionDetail],
    ) -> dict[str, list[EvidenceRecord]]:
        """Fetch evidence records by evidence IDs from composite and components."""
        # Collect all evidence IDs
        evidence_ids: set[str] = set()
        evidence_ids.update(composite.evidence_refs)

        for component in components.values():
            evidence_ids.update(component.evidence_refs)

        if not evidence_ids:
            return {}

        # Group evidence by primitive_name
        evidence_by_primitive: dict[str, list[EvidenceRecord]] = {
            "composite": [],
            "operational_risk": [],
            "shopper_frequency_trend": [],
        }

        # Query evidence table
        evidence_ids_list = list(evidence_ids)
        placeholders = ",".join(["?"] * len(evidence_ids_list))
        sql = f"""
        SELECT
            tenant_id,
            evidence_id,
            primitive_name,
            primitive_version,
            as_of_ts,
            computed_at,
            evidence_json
        FROM {table_name}
        WHERE tenant_id = ?
            AND evidence_id IN ({placeholders})
        """

        params: list[Any] = [tenant_id]
        params.extend(evidence_ids_list)

        try:
            rows = self.client.query(sql, params)
        except Exception as e:
            logger.error(f"Error fetching evidence: {e}")
            raise

        for row in rows:
            try:
                evidence_id = str(row["evidence_id"])
                primitive_name = str(row["primitive_name"])

                as_of_ts = row.get("as_of_ts")
                if isinstance(as_of_ts, str):
                    as_of_ts = datetime.fromisoformat(as_of_ts.replace("Z", "+00:00"))
                elif not isinstance(as_of_ts, datetime):
                    logger.warning(f"Invalid as_of_ts format in evidence: {as_of_ts}")
                    continue

                computed_at = row.get("computed_at")
                if isinstance(computed_at, str):
                    computed_at = datetime.fromisoformat(computed_at.replace("Z", "+00:00"))
                elif not isinstance(computed_at, datetime):
                    logger.warning(f"Invalid computed_at format in evidence: {computed_at}")
                    continue

                evidence_json = self._parse_json_field(row.get("evidence_json"), {})

                record = EvidenceRecord(
                    tenant_id=str(row["tenant_id"]),
                    evidence_id=evidence_id,
                    primitive_name=primitive_name,
                    primitive_version=str(row["primitive_version"]),
                    as_of_ts=as_of_ts,
                    computed_at=computed_at,
                    evidence=evidence_json,
                )

                # Group by primitive_name
                if primitive_name == "shopper_health_classification":
                    evidence_by_primitive["composite"].append(record)
                elif primitive_name in evidence_by_primitive:
                    evidence_by_primitive[primitive_name].append(record)
            except Exception as e:
                logger.warning(f"Error parsing evidence record: {e}")
                continue

        # Remove empty lists
        return {k: v for k, v in evidence_by_primitive.items() if v}

    def _row_to_decision_detail(self, row: dict[str, Any]) -> DecisionDetail:
        """Convert a database row to DecisionDetail."""
        as_of_ts = row.get("as_of_ts")
        if isinstance(as_of_ts, str):
            as_of_ts = datetime.fromisoformat(as_of_ts.replace("Z", "+00:00"))
        elif not isinstance(as_of_ts, datetime):
            raise ValueError(f"Invalid as_of_ts format: {as_of_ts}")

        computed_at = row.get("computed_at")
        if isinstance(computed_at, str):
            computed_at = datetime.fromisoformat(computed_at.replace("Z", "+00:00"))
        elif not isinstance(computed_at, datetime):
            raise ValueError(f"Invalid computed_at format: {computed_at}")

        valid_until = row.get("valid_until")
        if valid_until and isinstance(valid_until, str):
            valid_until = datetime.fromisoformat(valid_until.replace("Z", "+00:00"))
        elif valid_until and not isinstance(valid_until, datetime):
            valid_until = None

        return DecisionDetail(
            tenant_id=str(row["tenant_id"]),
            subject_type=str(row["subject_type"]),
            subject_id=str(row["subject_id"]),
            primitive_name=str(row["primitive_name"]),
            primitive_version=str(row["primitive_version"]),
            as_of_ts=as_of_ts,
            canonical_version=str(row["canonical_version"]),
            config_version=str(row["config_version"]),
            decision_state=str(row["decision_state"]),
            confidence=str(row["confidence"]),
            computed_at=computed_at,
            valid_until=valid_until,
            drivers=self._parse_json_field(row.get("drivers_json"), []),
            metrics=self._parse_json_field(row.get("metrics_json"), {}),
            evidence_refs=self._parse_json_field(row.get("evidence_refs_json"), []),
            correlation_id=str(row["correlation_id"]) if row.get("correlation_id") else None,
        )

