from __future__ import annotations

import logging
import time
from typing import Any, Optional

from databricks import sql as databricks_sql

from opsiq_runtime.domain.common.ids import CorrelationId
from opsiq_runtime.settings import Settings

logger = logging.getLogger(__name__)


class DatabricksSqlClient:
    """Client for executing SQL queries against Databricks using the SQL Connector."""

    def __init__(self, settings: Settings, correlation_id: Optional[CorrelationId] = None) -> None:
        self.settings = settings
        self.correlation_id = correlation_id
        self._connection: Optional[Any] = None

    def _get_log_extra(self) -> dict[str, str]:
        """Get extra context for structured logging."""
        extra = {}
        if self.correlation_id:
            extra["correlation_id"] = self.correlation_id.value
        return extra

    def _connect(self) -> Any:
        """Create a connection to Databricks."""
        if self._connection is None:
            if not all(
                [
                    self.settings.databricks_server_hostname,
                    self.settings.databricks_http_path,
                    self.settings.databricks_access_token,
                ]
            ):
                raise ValueError(
                    "Databricks connection requires DATABRICKS_SERVER_HOSTNAME, "
                    "DATABRICKS_HTTP_PATH, and DATABRICKS_ACCESS_TOKEN"
                )

            logger.info(
                f"Connecting to Databricks server: {self.settings.databricks_server_hostname}",
                extra=self._get_log_extra(),
            )

            connection_params = {
                "server_hostname": self.settings.databricks_server_hostname,
                "http_path": self.settings.databricks_http_path,
                "access_token": self.settings.databricks_access_token,
            }

            if self.settings.databricks_catalog:
                connection_params["catalog"] = self.settings.databricks_catalog
            if self.settings.databricks_schema:
                connection_params["schema"] = self.settings.databricks_schema

            self._connection = databricks_sql.connect(**connection_params)

        return self._connection

    def _retry_on_error(self, operation, max_retries: int = 3, initial_delay: float = 1.0):
        """Execute operation with retry logic for transient errors."""
        last_exception = None
        for attempt in range(max_retries):
            try:
                return operation()
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    delay = initial_delay * (2**attempt)
                    logger.warning(
                        f"Operation failed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}",
                        extra=self._get_log_extra(),
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"Operation failed after {max_retries} attempts: {e}",
                        extra=self._get_log_extra(),
                    )
                    raise
        if last_exception:
            raise last_exception

    def query(self, sql: str, params: Optional[list[Any] | dict[str, Any]] = None) -> list[dict[str, Any]]:
        """
        Execute a SELECT query and return results as a list of dictionaries.

        Args:
            sql: SQL query string with ? placeholders for positional params
            params: List of parameters (positional) or dict (named) to substitute

        Returns:
            List of dictionaries, one per row
        """
        logger.debug(f"Executing query: {sql[:200]}...", extra=self._get_log_extra())

        def _execute_query():
            conn = self._connect()
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(sql, parameters=params)
                else:
                    cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            finally:
                cursor.close()

        return self._retry_on_error(_execute_query)

    def execute(self, sql: str, params: Optional[list[Any] | dict[str, Any]] = None) -> None:
        """
        Execute a DML statement (INSERT, UPDATE, DELETE, MERGE).

        Args:
            sql: SQL statement string with ? placeholders for positional params
            params: List of parameters (positional) or dict (named) to substitute
        """
        logger.debug(f"Executing statement: {sql[:200]}...", extra=self._get_log_extra())

        def _execute_stmt():
            conn = self._connect()
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(sql, parameters=params)
                else:
                    cursor.execute(sql)
                conn.commit()
            finally:
                cursor.close()

        self._retry_on_error(_execute_stmt)

    def describe_table(self, table_name: str) -> list[dict[str, Any]]:
        """
        Describe a table and return column information.

        Args:
            table_name: Fully qualified table name

        Returns:
            List of dictionaries with column information (col_name, data_type, comment, etc.)
        """
        # Use DESCRIBE TABLE (without EXTENDED) to get just column information
        sql = f"DESCRIBE TABLE {table_name}"
        logger.debug(f"Describing table: {table_name}", extra=self._get_log_extra())

        try:
            return self.query(sql)
        except Exception as e:
            # Table might not exist
            logger.debug(f"Error describing table {table_name}: {e}", extra=self._get_log_extra())
            raise

    def close(self) -> None:
        """Close the connection."""
        if self._connection:
            try:
                self._connection.close()
                logger.info("Closed Databricks connection", extra=self._get_log_extra())
            except Exception as e:
                logger.warning(f"Error closing connection: {e}", extra=self._get_log_extra())
            finally:
                self._connection = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
