from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Iterable, Optional

from pathlib import Path

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.application.errors import ProvisioningError
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.decision import DecisionResult
from opsiq_runtime.domain.common.evidence import Evidence, EvidenceSet
from opsiq_runtime.domain.common.input_protocol import CommonInput
from opsiq_runtime.ports.outputs_repository import OutputsRepository
from opsiq_runtime.settings import Settings, get_settings

logger = logging.getLogger(__name__)

# Batch size for writes (configurable range: 500-2000)
BATCH_SIZE = 1000


class DatabricksOutputsRepository(OutputsRepository):
    """Outputs repository that writes to Databricks tables."""

    # Expected columns for each table
    DECISION_TABLE_COLUMNS = {
        "tenant_id",
        "subject_type",
        "subject_id",
        "primitive_name",
        "primitive_version",
        "canonical_version",
        "config_version",
        "as_of_ts",
        "decision_state",
        "confidence",
        "drivers_json",
        "metrics_json",
        "evidence_refs_json",
        "computed_at",
        "valid_until",
        "correlation_id",
        "adapter_mode",
        "created_at",
        "updated_at",
    }

    EVIDENCE_TABLE_COLUMNS = {
        "tenant_id",
        "subject_type",
        "subject_id",
        "primitive_name",
        "primitive_version",
        "canonical_version",
        "config_version",
        "as_of_ts",
        "evidence_id",
        "evidence_json",
        "computed_at",
        "correlation_id",
        "adapter_mode",
        "created_at",
        "updated_at",
    }

    RUN_REGISTRY_TABLE_COLUMNS = {
        "tenant_id",
        "primitive_name",
        "primitive_version",
        "canonical_version",
        "config_version",
        "as_of_ts",
        "correlation_id",
        "adapter_mode",
        "status",
        "started_at",
        "completed_at",
        "duration_ms",
        "input_count",
        "decision_count",
        "at_risk_count",
        "not_at_risk_count",
        "unknown_count",
        "decision_table",
        "evidence_table",
        "error_type",
        "error_message",
        "error_stack",
        "updated_at",
    }

    # Run status constants
    STATUS_STARTED = "STARTED"
    STATUS_SUCCESS = "SUCCESS"
    STATUS_FAILED = "FAILED"

    def __init__(
        self,
        client: DatabricksSqlClient,
        settings: Settings | None = None,
        decision_table_name: str | None = None,
        evidence_table_name: str | None = None,
        run_registry_table_name: str | None = None,
    ) -> None:
        self.client = client
        self.settings = settings or get_settings()
        prefix = self.settings.databricks_table_prefix
        self.decision_table_name = decision_table_name or f"{prefix}gold_decision_output_v1"
        self.evidence_table_name = evidence_table_name or f"{prefix}gold_decision_evidence_v1"
        self.run_registry_table_name = run_registry_table_name or f"{prefix}gold_runtime_run_registry_v1"
        self._validated_tables: set[str] = set()  # Cache validated table names

    def _build_table_name(self, table_name: str) -> str:
        """Build fully qualified table name with catalog and schema if specified."""
        parts = []
        if self.settings.databricks_catalog:
            parts.append(self.settings.databricks_catalog)
        if self.settings.databricks_schema:
            parts.append(self.settings.databricks_schema)
        parts.append(table_name)
        return ".".join(parts)

    def _get_ddl_file_path(self, table_type: str) -> str:
        """Get the path to the DDL file for a table type."""
        base_path = Path(__file__).parent / "ddl"
        if table_type == "decision":
            return str(base_path / "decision_output.sql")
        elif table_type == "evidence":
            return str(base_path / "evidence.sql")
        elif table_type == "run_registry":
            return str(base_path / "run_registry.sql")
        else:
            return str(base_path / f"{table_type}.sql")

    def _get_suggested_command(self, table_name: str, ddl_file: str) -> str:
        """Generate a suggested Databricks command to create the table."""
        catalog_schema = ""
        if self.settings.databricks_catalog:
            catalog_schema = f"{self.settings.databricks_catalog}."
            if self.settings.databricks_schema:
                catalog_schema += f"{self.settings.databricks_schema}."
        return f"Run the DDL from {ddl_file} in Databricks SQL, or use: databricks sql execute --file {ddl_file} --warehouse-id <your-warehouse-id>"

    def _validate_table(self, table_name: str, expected_columns: set[str], table_type: str) -> None:
        """
        Validate that a table exists and has all required columns.

        Args:
            table_name: Fully qualified table name
            expected_columns: Set of required column names
            table_type: Type of table ('decision' or 'evidence') for error messages

        Raises:
            ProvisioningError: If table is missing or columns are missing
        """
        # Skip if already validated
        if table_name in self._validated_tables:
            return

        try:
            # Try to describe the table
            description = self.client.describe_table(table_name)
        except Exception as e:
            # Table doesn't exist or can't be accessed
            ddl_file = self._get_ddl_file_path(table_type)
            suggested_command = self._get_suggested_command(table_name, ddl_file)
            raise ProvisioningError(
                f"Table {table_name} does not exist or cannot be accessed: {e}",
                table_names=[table_name],
                ddl_file_path=ddl_file,
                suggested_command=suggested_command,
            ) from e

        # Extract column names from DESCRIBE output
        # DESCRIBE TABLE returns rows with col_name, data_type, comment, etc.
        # The output format can vary, but typically has 'col_name' as the first column
        actual_columns = set()
        for row in description:
            # Try different possible column name keys
            col_name = (
                row.get("col_name")
                or row.get("column_name")
                or (list(row.values())[0] if row else None)  # First value if dict
            )
            if col_name:
                col_name_str = str(col_name).strip()
                # Skip empty strings, metadata rows starting with #, and table properties
                if col_name_str and not col_name_str.startswith("#") and "=" not in col_name_str:
                    actual_columns.add(col_name_str.lower())

        # Check for missing columns
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            ddl_file = self._get_ddl_file_path(table_type)
            suggested_command = self._get_suggested_command(table_name, ddl_file)
            raise ProvisioningError(
                f"Table {table_name} is missing required columns: {', '.join(sorted(missing_columns))}",
                table_names=[table_name],
                ddl_file_path=ddl_file,
                suggested_command=suggested_command,
            )

        # Table is valid, cache it
        self._validated_tables.add(table_name)
        logger.debug(f"Validated table {table_name} has all required columns", extra=self.client._get_log_extra())

    def _format_datetime(self, dt: datetime | None) -> str | None:
        """Format datetime to ISO string for SQL."""
        if dt is None:
            return None
        return dt.isoformat()

    def _escape_sql_string(self, value: str) -> str:
        """Escape single quotes in SQL string values."""
        return value.replace("'", "''")

    def write_decisions(
        self, ctx: RunContext, decisions: Iterable[DecisionResult], inputs: Optional[list[CommonInput]] = None
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

        # Validate table before writing
        self._validate_table(table_name, self.DECISION_TABLE_COLUMNS, "decision")

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
        inputs: list[CommonInput],
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

            now_ts = self._format_datetime(datetime.now(timezone.utc))
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
                f"'{self._escape_sql_string(decision.confidence)}', "
                f"'{drivers_escaped}', "
                f"'{metrics_escaped}', "
                f"'{evidence_refs_escaped}', "
                f"'{self._format_datetime(decision.computed_at)}', "
                f"{f"'{self._format_datetime(decision.valid_until)}'" if decision.valid_until else "NULL"}, "
                f"'{self._escape_sql_string(correlation_id or 'unknown')}', "
                f"'databricks', "  # adapter_mode
                f"'{now_ts}', "  # created_at
                f"'{now_ts}'"  # updated_at
                f")"
            )

        source_values = ",\n            ".join(values_parts)

        sql = f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT * FROM VALUES {source_values} AS source(
                tenant_id, subject_type, subject_id, primitive_name, primitive_version,
                canonical_version, config_version, as_of_ts, decision_state, confidence,
                drivers_json, metrics_json, evidence_refs_json, computed_at, valid_until,
                correlation_id, adapter_mode, created_at, updated_at
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
                correlation_id = source.correlation_id,
                adapter_mode = source.adapter_mode,
                updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (
                tenant_id, subject_type, subject_id, primitive_name, primitive_version,
                canonical_version, config_version, as_of_ts, decision_state, confidence,
                drivers_json, metrics_json, evidence_refs_json, computed_at, valid_until,
                correlation_id, adapter_mode, created_at, updated_at
            ) VALUES (
                source.tenant_id, source.subject_type, source.subject_id, source.primitive_name,
                source.primitive_version, source.canonical_version, source.config_version, source.as_of_ts,
                source.decision_state, source.confidence, source.drivers_json, source.metrics_json,
                source.evidence_refs_json, source.computed_at, source.valid_until, source.correlation_id,
                source.adapter_mode, source.created_at, source.updated_at
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
        inputs: list[CommonInput],
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
        self,
        ctx: RunContext,
        evidence_sets: Iterable[EvidenceSet],
        inputs: Optional[list[CommonInput]] = None,
        decisions: Optional[Iterable[DecisionResult]] = None,
    ) -> None:
        """
        Write evidence to Databricks table using MERGE or DELETE+INSERT.

        Requires inputs parameter to extract metadata.
        Requires decisions parameter to get canonical_version from config.
        """
        if inputs is None:
            raise ValueError("DatabricksOutputsRepository.write_evidence requires inputs parameter")
        if decisions is None:
            raise ValueError("DatabricksOutputsRepository.write_evidence requires decisions parameter")

        evidence_sets_list = list(evidence_sets)
        decisions_list = list(decisions)
        if len(evidence_sets_list) != len(inputs) or len(decisions_list) != len(inputs):
            raise ValueError(
                f"Mismatch: {len(evidence_sets_list)} evidence_sets, {len(decisions_list)} decisions vs {len(inputs)} inputs"
            )

        # Flatten evidence sets to individual evidence records, maintaining input/decision pairing
        evidence_records: list[tuple[Evidence, CommonInput, DecisionResult]] = []
        for evidence_set, input_row, decision in zip(evidence_sets_list, inputs, decisions_list):
            for evidence in evidence_set.evidence:
                evidence_records.append((evidence, input_row, decision))

        if not evidence_records:
            logger.info("No evidence records to write")
            return

        table_name = self._build_table_name(self.evidence_table_name)
        correlation_id = ctx.correlation_id.value if ctx.correlation_id else None

        # Validate table before writing
        self._validate_table(table_name, self.EVIDENCE_TABLE_COLUMNS, "evidence")

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
        evidence_records: list[tuple[Evidence, CommonInput, DecisionResult]],
        correlation_id: str | None,
    ) -> None:
        """Write evidence using MERGE INTO."""
        values_parts = []
        for evidence, input_row, decision in evidence_records:
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

            now_ts = self._format_datetime(datetime.now(timezone.utc))
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
                f"'{self._escape_sql_string(evidence.evidence_id)}', "
                f"'{evidence_json_escaped}', "
                f"'{self._format_datetime(evidence.observed_at)}', "
                f"'{self._escape_sql_string(correlation_id or 'unknown')}', "
                f"'databricks', "  # adapter_mode
                f"'{now_ts}', "  # created_at
                f"'{now_ts}'"  # updated_at
                f")"
            )

        source_values = ",\n            ".join(values_parts)

        sql = f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT * FROM VALUES {source_values} AS source(
                tenant_id, subject_type, subject_id, primitive_name, primitive_version,
                canonical_version, config_version, as_of_ts, evidence_id, evidence_json,
                computed_at, correlation_id, adapter_mode, created_at, updated_at
            )
        ) AS source
        ON target.tenant_id = source.tenant_id
            AND target.evidence_id = source.evidence_id
        WHEN MATCHED THEN
            UPDATE SET
                subject_type = source.subject_type,
                subject_id = source.subject_id,
                primitive_name = source.primitive_name,
                primitive_version = source.primitive_version,
                canonical_version = source.canonical_version,
                config_version = source.config_version,
                as_of_ts = source.as_of_ts,
                evidence_json = source.evidence_json,
                computed_at = source.computed_at,
                correlation_id = source.correlation_id,
                adapter_mode = source.adapter_mode,
                updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (
                tenant_id, subject_type, subject_id, primitive_name, primitive_version,
                canonical_version, config_version, as_of_ts, evidence_id, evidence_json,
                computed_at, correlation_id, adapter_mode, created_at, updated_at
            ) VALUES (
                source.tenant_id, source.subject_type, source.subject_id, source.primitive_name,
                source.primitive_version, source.canonical_version, source.config_version, source.as_of_ts,
                source.evidence_id, source.evidence_json, source.computed_at, source.correlation_id,
                source.adapter_mode, source.created_at, source.updated_at
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
        evidence_records: list[tuple[Evidence, CommonInput, DecisionResult]],
        correlation_id: str | None,
    ) -> None:
        """Write evidence using DELETE + INSERT for idempotency."""
        # Use simpler MERGE key: (tenant_id, evidence_id)
        where_clauses = []
        for evidence, input_row, _decision in evidence_records:
            where_clauses.append(
                f"(tenant_id = '{self._escape_sql_string(str(input_row.tenant_id))}' "
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

    # =========================================================================
    # Run Registry Methods
    # =========================================================================

    def register_run_started(
        self,
        ctx: RunContext,
        canonical_version: str,
    ) -> None:
        """
        Register the start of a run in the run registry.

        Args:
            ctx: Run context
            canonical_version: Canonical version being used
        """
        table_name = self._build_table_name(self.run_registry_table_name)
        correlation_id = ctx.correlation_id.value if ctx.correlation_id else "unknown"
        now_ts = self._format_datetime(datetime.now(timezone.utc))

        # Validate table before writing
        self._validate_table(table_name, self.RUN_REGISTRY_TABLE_COLUMNS, "run_registry")

        decision_table = self._build_table_name(self.decision_table_name)
        evidence_table = self._build_table_name(self.evidence_table_name)

        sql = f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT
                '{self._escape_sql_string(str(ctx.tenant_id))}' AS tenant_id,
                '{self._escape_sql_string(ctx.primitive_name)}' AS primitive_name,
                '{self._escape_sql_string(ctx.primitive_version)}' AS primitive_version,
                '{self._escape_sql_string(canonical_version)}' AS canonical_version,
                '{self._escape_sql_string(ctx.config_version)}' AS config_version,
                '{self._format_datetime(ctx.as_of_ts)}' AS as_of_ts,
                '{self._escape_sql_string(correlation_id)}' AS correlation_id,
                'databricks' AS adapter_mode,
                '{self.STATUS_STARTED}' AS status,
                '{now_ts}' AS started_at,
                NULL AS completed_at,
                NULL AS duration_ms,
                NULL AS input_count,
                NULL AS decision_count,
                NULL AS at_risk_count,
                NULL AS not_at_risk_count,
                NULL AS unknown_count,
                '{self._escape_sql_string(decision_table)}' AS decision_table,
                '{self._escape_sql_string(evidence_table)}' AS evidence_table,
                NULL AS error_type,
                NULL AS error_message,
                NULL AS error_stack,
                '{now_ts}' AS updated_at
        ) AS source
        ON target.tenant_id = source.tenant_id
            AND target.correlation_id = source.correlation_id
            AND target.primitive_name = source.primitive_name
        WHEN MATCHED THEN
            UPDATE SET
                status = source.status,
                started_at = source.started_at,
                updated_at = source.updated_at,
                primitive_version = source.primitive_version,
                canonical_version = source.canonical_version,
                config_version = source.config_version,
                as_of_ts = source.as_of_ts
        WHEN NOT MATCHED THEN
            INSERT (
                tenant_id, primitive_name, primitive_version, canonical_version, config_version,
                as_of_ts, correlation_id, adapter_mode, status, started_at, completed_at,
                duration_ms, input_count, decision_count, at_risk_count, not_at_risk_count,
                unknown_count, decision_table, evidence_table, error_type, error_message,
                error_stack, updated_at
            ) VALUES (
                source.tenant_id, source.primitive_name, source.primitive_version,
                source.canonical_version, source.config_version, source.as_of_ts,
                source.correlation_id, source.adapter_mode, source.status, source.started_at,
                source.completed_at, source.duration_ms, source.input_count, source.decision_count,
                source.at_risk_count, source.not_at_risk_count, source.unknown_count,
                source.decision_table, source.evidence_table, source.error_type,
                source.error_message, source.error_stack, source.updated_at
            )
        """

        try:
            self.client.execute(sql)
            logger.info(f"Registered run started for correlation_id={correlation_id}")
        except Exception as e:
            logger.warning(f"Failed to register run started: {e}")
            # Don't raise - run registry is best-effort

    def register_run_completed(
        self,
        ctx: RunContext,
        started_at: datetime,
        input_count: int,
        decision_count: int,
        at_risk_count: int,
        not_at_risk_count: int,
        unknown_count: int,
    ) -> None:
        """
        Register the successful completion of a run.

        Args:
            ctx: Run context
            started_at: When the run started
            input_count: Number of inputs processed
            decision_count: Number of decisions written
            at_risk_count: Count of AT_RISK decisions
            not_at_risk_count: Count of NOT_AT_RISK decisions
            unknown_count: Count of UNKNOWN decisions
        """
        table_name = self._build_table_name(self.run_registry_table_name)
        correlation_id = ctx.correlation_id.value if ctx.correlation_id else "unknown"
        now = datetime.now(timezone.utc)
        now_ts = self._format_datetime(now)
        duration_ms = int((now - started_at).total_seconds() * 1000)

        sql = f"""
        UPDATE {table_name}
        SET
            status = '{self.STATUS_SUCCESS}',
            completed_at = '{now_ts}',
            duration_ms = {duration_ms},
            input_count = {input_count},
            decision_count = {decision_count},
            at_risk_count = {at_risk_count},
            not_at_risk_count = {not_at_risk_count},
            unknown_count = {unknown_count},
            updated_at = '{now_ts}'
        WHERE tenant_id = '{self._escape_sql_string(str(ctx.tenant_id))}'
            AND correlation_id = '{self._escape_sql_string(correlation_id)}'
            AND primitive_name = '{self._escape_sql_string(ctx.primitive_name)}'
        """

        try:
            self.client.execute(sql)
            logger.info(f"Registered run completed for correlation_id={correlation_id}, primitive={ctx.primitive_name}, duration={duration_ms}ms")
        except Exception as e:
            logger.warning(f"Failed to register run completed: {e}")
            # Don't raise - run registry is best-effort

    def register_run_failed(
        self,
        ctx: RunContext,
        started_at: datetime,
        error: Exception,
    ) -> None:
        """
        Register a failed run.

        Args:
            ctx: Run context
            started_at: When the run started
            error: The exception that caused the failure
        """
        import traceback

        table_name = self._build_table_name(self.run_registry_table_name)
        correlation_id = ctx.correlation_id.value if ctx.correlation_id else "unknown"
        now = datetime.now(timezone.utc)
        now_ts = self._format_datetime(now)
        duration_ms = int((now - started_at).total_seconds() * 1000)

        error_type = type(error).__name__
        error_message = str(error)[:1000]  # Truncate long messages
        error_stack = traceback.format_exc()[:4000]  # Truncate long stacks

        sql = f"""
        UPDATE {table_name}
        SET
            status = '{self.STATUS_FAILED}',
            completed_at = '{now_ts}',
            duration_ms = {duration_ms},
            error_type = '{self._escape_sql_string(error_type)}',
            error_message = '{self._escape_sql_string(error_message)}',
            error_stack = '{self._escape_sql_string(error_stack)}',
            updated_at = '{now_ts}'
        WHERE tenant_id = '{self._escape_sql_string(str(ctx.tenant_id))}'
            AND correlation_id = '{self._escape_sql_string(correlation_id)}'
            AND primitive_name = '{self._escape_sql_string(ctx.primitive_name)}'
        """

        try:
            self.client.execute(sql)
            logger.info(f"Registered run failed for correlation_id={correlation_id}, primitive={ctx.primitive_name}")
        except Exception as e:
            logger.warning(f"Failed to register run failed: {e}")
