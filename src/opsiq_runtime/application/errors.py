class UnknownPrimitiveError(Exception):
    pass


class PrimitiveVersionMismatch(Exception):
    pass


class ProvisioningError(Exception):
    """Raised when required Databricks tables or columns are missing."""

    def __init__(self, message: str, table_names: list[str] | None = None, ddl_file_path: str | None = None, suggested_command: str | None = None) -> None:
        super().__init__(message)
        self.table_names = table_names or []
        self.ddl_file_path = ddl_file_path
        self.suggested_command = suggested_command

