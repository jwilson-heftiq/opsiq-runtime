from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Iterable, Optional

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.decision import DecisionResult
from opsiq_runtime.domain.common.evidence import Evidence, EvidenceSet
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.ports.outputs_repository import OutputsRepository
from opsiq_runtime.settings import Settings, get_settings

logger = logging.getLogger(__name__)

# Batch size for writes (configurable range: 500-2000)
BATCH_SIZE = 1000


class DatabricksOutputsRepository(OutputsRepository):
    """Outputs repository that writes to Databricks tables."""

    def __init__(
        self,
        client: DatabricksSqlClient,
        settings: Settings | None = None,
        decision_table_name: str | None = None,
        evidence_table_name: str | None = None,
    ) -> None:
        self.client = client
        self.settings = settings or get_settings()
        prefix = self.settings.databricks_table_prefix
        self.decision_table_name = decision_table_name or f"{prefix}gold_decision_output_operational_risk_v1"
        self.evidence_table_name = evidence_table_name or f"{prefix}gold_decision_evidence_operational_risk_v1"

    def _build_table_name(self, table_name: str) -> str:
        """Build fully qualified table name with catalog and schema if specified."""
        parts = []
        if self.settings.databricks_catalog:
            parts.append(self.settings.databricks_catalog)
        if self.settings.databricks_schema:
            parts.append(self.settings.databricks_schema)
        parts.append(table_name)
        return ".".join(parts)

    def _format_datetime(self, dt: datetime | None) -> str | None:
        """Format datetime to ISO string for SQL."""
        if dt is None:
            return None
        return dt.isoformat()

    def _escape_sql_string(self, value: str) -> str:
        """Escape single quotes in SQL string values."""
        return value.replace("'", "''")

    def write_decisions(
        self, ctx: RunContext, decisions: Iterable[DecisionResult], inputs: Optional[list[OperationalRiskInput]] = None
    ) -> None:
        """
        Write decisions to Databricks table using MERGE or DELETE+INSERT.

        Requires inputs parameter to extract metadata (tenant_id, subject_id, etc.).
        """
        if inputs is None:
            raise ValueError("DatabricksOutputsRepository.write_decisions requires inputs parameter")

        decisions_list = list(decisions)
        if len(decisions_list) != len(inputs):
            raise ValueError(f"Mismatch: {len(decisions_list)} decisions vs {len(inputs)} inputs")

        table_name = self._build_table_name(self.decision_table_name)
        correlation_id = ctx.correlation_id.value if ctx.correlation_id else None

        logger.info(
            f"Writing {len(decisions_list)} decisions to {table_name}",
            extra={"correlation_id": correlation_id} if correlation_id else {},
        )

        # Process in batches
        for batch_start in range(0, len(decisions_list), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(decisions_list))
            batch_decisions = decisions_list[batch_start:batch_end]
            batch_inputs = inputs[batch_start:batch_end]

            if self.settings.databricks_use_merge:
                self._write_decisions_merge(table_name, ctx, batch_decisions, batch_inputs, correlation_id)
            else:
                self._write_decisions_delete_insert(table_name, ctx, batch_decisions, batch_inputs, correlation_id)

    def _write_decisions_merge(
        self,
        table_name: str,
        ctx: RunContext,
        decisions: list[DecisionResult],
        inputs: list[OperationalRiskInput],
        correlation_id: str | None,
    ) -> None:
        """Write decisions using MERGE INTO."""
        # Build VALUES clause for source data
        values_parts = []
        for decision, input_row in zip(decisions, inputs):
            drivers_json = json.dumps(decision.drivers)
            metrics_json = json.dumps(decision.metrics)
            evidence_refs_json = json.dumps(decision.evidence_refs)

            drivers_escaped = self._escape_sql_string(drivers_json)
            metrics_escaped = self._escape_sql_string(metrics_json)
            evidence_refs_escaped = self._escape_sql_string(evidence_refs_json)

            values_parts.append(
                f"("
                f"'{self._escape_sql_string(str(input_row.tenant_id))}', "
                f"'{self._escape_sql_string(input_row.subject_type)}', "
                f"'{self._escape_sql_string(str(input_row.subject_id))}', "
                f"'{self._escape_sql_string(ctx.primitive_name)}', "
                f"'{self._escape_sql_string(ctx.primitive_version)}', "
                f"'{self._escape_sql_string(decision.versions.canonical_version)}', "
                f"'{self._escape_sql_string(input_row.config_version)}', "
                f"'{self._format_datetime(input_row.as_of_ts)}', "
                f"'{self._escape_sql_string(decision.state)}', "
                f"NULL, "  # confidence (not in DecisionResult yet)
                f"'{drivers_escaped}', "
                f"'{metrics_escaped}', "
                f"'{evidence_refs_escaped}', "
                f"'{self._format_datetime(decision.computed_at)}', "
                f"{f"'{self._format_datetime(decision.valid_until)}'" if decision.valid_until else "NULL"}, "
                f"{f"'{self._escape_sql_string(correlation_id)}'" if correlation_id else "NULL"}"
                f")"
            )

        source_values = ",\n            ".join(values_parts)

        sql = f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT * FROM VALUES {source_values} AS source(
                tenant_id, subject_type, subject_id, primitive_name, primitive_version,
                canonical_version, config_version, as_of_ts, decision_state, confidence,
                drivers_json, metrics_json, evidence_refs_json, computed_at, valid_until, correlation_id
            )
        ) AS source
        ON target.tenant_id = source.tenant_id
            AND target.subject_type = source.subject_type
            AND target.subject_id = source.subject_id
            AND target.primitive_name = source.primitive_name
            AND target.primitive_version = source.primitive_version
            AND target.as_of_ts = source.as_of_ts
        WHEN MATCHED THEN
            UPDATE SET
                canonical_version = source.canonical_version,
                config_version = source.config_version,
                decision_state = source.decision_state,
                confidence = source.confidence,
                drivers_json = source.drivers_json,
                metrics_json = source.metrics_json,
                evidence_refs_json = source.evidence_refs_json,
                computed_at = source.computed_at,
                valid_until = source.valid_until,
                correlation_id = source.correlation_id
        WHEN NOT MATCHED THEN
            INSERT (
                tenant_id, subject_type, subject_id, primitive_name, primitive_version,
                canonical_version, config_version, as_of_ts, decision_state, confidence,
                drivers_json, metrics_json, evidence_refs_json, computed_at, valid_until, correlation_id
            ) VALUES (
                source.tenant_id, source.subject_type, source.subject_id, source.primitive_name,
                source.primitive_version, source.canonical_version, source.config_version, source.as_of_ts,
                source.decision_state, source.confidence, source.drivers_json, source.metrics_json,
                source.evidence_refs_json, source.computed_at, source.valid_until, source.correlation_id
            )
        """

        try:
            self.client.execute(sql)
        except Exception as e:
            logger.error(f"Error executing MERGE for decisions: {e}", extra={"correlation_id": correlation_id} if correlation_id else {})
            raise

    def _write_decisions_delete_insert(
        self,
        table_name: str,
        ctx: RunContext,
        decisions: list[DecisionResult],
        inputs: list[OperationalRiskInput],
        correlation_id: str | None,
    ) -> None:
        """Write decisions using DELETE + INSERT for idempotency."""
        # Collect keys for deletion
        where_clauses = []
        for decision, input_row in zip(decisions, inputs):
            where_clauses.append(
                f"(tenant_id = '{self._escape_sql_string(str(input_row.tenant_id))}' "
                f"AND subject_type = '{self._escape_sql_string(input_row.subject_type)}' "
                f"AND subject_id = '{self._escape_sql_string(str(input_row.subject_id))}' "
                f"AND primitive_name = '{self._escape_sql_string(ctx.primitive_name)}' "
                f"AND primitive_version = '{self._escape_sql_string(ctx.primitive_version)}' "
                f"AND as_of_ts = '{self._format_datetime(input_row.as_of_ts)}')"
            )

        delete_sql = f"DELETE FROM {table_name} WHERE {' OR '.join(where_clauses)}"

        try:
            self.client.execute(delete_sql)
        except Exception as e:
            logger.warning(f"Error in DELETE phase: {e}", extra={"correlation_id": correlation_id} if correlation_id else {})
            raise

        # Now insert
        self._write_decisions_merge(table_name, ctx, decisions, inputs, correlation_id)

    def write_evidence(
        self, ctx: RunContext, evidence_sets: Iterable[EvidenceSet], inputs: Optional[list[OperationalRiskInput]] = None
    ) -> None:
        """
        Write evidence to Databricks table using MERGE or DELETE+INSERT.

        Requires inputs parameter to extract metadata.
        """
        if inputs is None:
            raise ValueError("DatabricksOutputsRepository.write_evidence requires inputs parameter")

        evidence_sets_list = list(evidence_sets)
        if len(evidence_sets_list) != len(inputs):
            raise ValueError(f"Mismatch: {len(evidence_sets_list)} evidence_sets vs {len(inputs)} inputs")

        # Flatten evidence sets to individual evidence records, maintaining input pairing
        evidence_records: list[tuple[Evidence, OperationalRiskInput]] = []
        for evidence_set, input_row in zip(evidence_sets_list, inputs):
            for evidence in evidence_set.evidence:
                evidence_records.append((evidence, input_row))

        if not evidence_records:
            logger.info("No evidence records to write")
            return

        table_name = self._build_table_name(self.evidence_table_name)
        correlation_id = ctx.correlation_id.value if ctx.correlation_id else None

        logger.info(
            f"Writing {len(evidence_records)} evidence records to {table_name}",
            extra={"correlation_id": correlation_id} if correlation_id else {},
        )

        # Process in batches
        for batch_start in range(0, len(evidence_records), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(evidence_records))
            batch_evidence = evidence_records[batch_start:batch_end]

            if self.settings.databricks_use_merge:
                self._write_evidence_merge(table_name, ctx, batch_evidence, correlation_id)
            else:
                self._write_evidence_delete_insert(table_name, ctx, batch_evidence, correlation_id)

    def _write_evidence_merge(
        self,
        table_name: str,
        ctx: RunContext,
        evidence_records: list[tuple[Evidence, OperationalRiskInput]],
        correlation_id: str | None,
    ) -> None:
        """Write evidence using MERGE INTO."""
        values_parts = []
        for evidence, input_row in evidence_records:
            evidence_json = json.dumps(
                {
                    "evidence_id": evidence.evidence_id,
                    "rule_ids": evidence.rule_ids,
                    "thresholds": evidence.thresholds,
                    "references": evidence.references,
                    "observed_at": self._format_datetime(evidence.observed_at),
                }
            )

            evidence_json_escaped = self._escape_sql_string(evidence_json)

            values_parts.append(
                f"("
                f"'{self._escape_sql_string(str(input_row.tenant_id))}', "
                f"'{self._escape_sql_string(input_row.subject_type)}', "
                f"'{self._escape_sql_string(str(input_row.subject_id))}', "
                f"'{self._escape_sql_string(ctx.primitive_name)}', "
                f"'{self._escape_sql_string(ctx.primitive_version)}', "
                f"'{self._escape_sql_string(input_row.canonical_version)}', "
                f"'{self._escape_sql_string(input_row.config_version)}', "
                f"'{self._format_datetime(input_row.as_of_ts)}', "
                f"'{self._escape_sql_string(evidence.evidence_id)}', "
                f"'{evidence_json_escaped}', "
                f"'{self._format_datetime(evidence.observed_at)}', "
                f"{f"'{self._escape_sql_string(correlation_id)}'" if correlation_id else "NULL"}"
                f")"
            )

        source_values = ",\n            ".join(values_parts)

        sql = f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT * FROM VALUES {source_values} AS source(
                tenant_id, subject_type, subject_id, primitive_name, primitive_version,
                canonical_version, config_version, as_of_ts, evidence_id, evidence_json,
                computed_at, correlation_id
            )
        ) AS source
        ON target.tenant_id = source.tenant_id
            AND target.subject_type = source.subject_type
            AND target.subject_id = source.subject_id
            AND target.primitive_name = source.primitive_name
            AND target.primitive_version = source.primitive_version
            AND target.as_of_ts = source.as_of_ts
            AND target.evidence_id = source.evidence_id
        WHEN MATCHED THEN
            UPDATE SET
                canonical_version = source.canonical_version,
                config_version = source.config_version,
                evidence_json = source.evidence_json,
                computed_at = source.computed_at,
                correlation_id = source.correlation_id
        WHEN NOT MATCHED THEN
            INSERT (
                tenant_id, subject_type, subject_id, primitive_name, primitive_version,
                canonical_version, config_version, as_of_ts, evidence_id, evidence_json,
                computed_at, correlation_id
            ) VALUES (
                source.tenant_id, source.subject_type, source.subject_id, source.primitive_name,
                source.primitive_version, source.canonical_version, source.config_version, source.as_of_ts,
                source.evidence_id, source.evidence_json, source.computed_at, source.correlation_id
            )
        """

        try:
            self.client.execute(sql)
        except Exception as e:
            logger.error(f"Error executing MERGE for evidence: {e}", extra={"correlation_id": correlation_id} if correlation_id else {})
            raise

    def _write_evidence_delete_insert(
        self,
        table_name: str,
        ctx: RunContext,
        evidence_records: list[tuple[Evidence, OperationalRiskInput]],
        correlation_id: str | None,
    ) -> None:
        """Write evidence using DELETE + INSERT for idempotency."""
        where_clauses = []
        for evidence, input_row in evidence_records:
            where_clauses.append(
                f"(tenant_id = '{self._escape_sql_string(str(input_row.tenant_id))}' "
                f"AND subject_type = '{self._escape_sql_string(input_row.subject_type)}' "
                f"AND subject_id = '{self._escape_sql_string(str(input_row.subject_id))}' "
                f"AND primitive_name = '{self._escape_sql_string(ctx.primitive_name)}' "
                f"AND primitive_version = '{self._escape_sql_string(ctx.primitive_version)}' "
                f"AND as_of_ts = '{self._format_datetime(input_row.as_of_ts)}' "
                f"AND evidence_id = '{self._escape_sql_string(evidence.evidence_id)}')"
            )

        delete_sql = f"DELETE FROM {table_name} WHERE {' OR '.join(where_clauses)}"

        try:
            self.client.execute(delete_sql)
        except Exception as e:
            logger.warning(f"Error in DELETE phase: {e}", extra={"correlation_id": correlation_id} if correlation_id else {})
            raise

        # Now insert
        self._write_evidence_merge(table_name, ctx, evidence_records, correlation_id)
