from __future__ import annotations

from typing import List, Any

from airflow.models.baseoperator import BaseOperator


class GalaxyValidationOperator(BaseOperator):
    """Minimal GalaxyValidationOperator used by the demo quality DAG.

    This operator is intentionally simple: it logs provided params and
    returns a dictionary describing the validation result. It's sufficient
    to prevent import errors during tests and to serve as a stub
    implementation for local development.
    """

    def __init__(self, *, table_name: str, min_rows: int = 0, required_columns: List[str] | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.min_rows = min_rows
        self.required_columns = required_columns or []

    def execute(self, context: dict) -> dict:
        self.log.info("Running GalaxyValidationOperator for table=%s", self.table_name)
        self.log.info("min_rows=%s required_columns=%s", self.min_rows, self.required_columns)

        # Return a simple result dict for downstream tasks or tests
        return {
            "table": self.table_name,
            "min_rows": self.min_rows,
            "required_columns": self.required_columns,
            "status": "ok",
        }
