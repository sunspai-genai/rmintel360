from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

try:
    import sqlglot
    from sqlglot import exp
except ModuleNotFoundError:  # pragma: no cover - exercised only when optional dependency is absent locally.
    sqlglot = None
    exp = None

from backend.app.catalog.service import catalog_service
from backend.app.db.connection import connect


class SqlValidationStatus:
    VALID = "valid"
    INVALID = "invalid"


BLOCKED_TOKENS = {
    "alter",
    "attach",
    "call",
    "copy",
    "create",
    "delete",
    "detach",
    "drop",
    "execute",
    "insert",
    "merge",
    "pragma",
    "replace",
    "truncate",
    "update",
}

DATE_COLUMNS_BY_BASE_TABLE = {
    "fact_deposit_balance_daily": "as_of_date",
    "fact_deposit_transaction": "transaction_date",
    "fact_loan_balance_monthly": "as_of_month",
    "fact_loan_payment": "payment_date",
    "fact_credit_risk_snapshot": "as_of_month",
    "fact_relationship_profitability": "as_of_month",
}


@dataclass(frozen=True)
class SqlValidationResult:
    status: str
    is_valid: bool
    errors: list[str]
    warnings: list[str]
    allowed_tables: list[str]
    allowed_columns: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "allowed_tables": self.allowed_tables,
            "allowed_columns": self.allowed_columns,
        }


class GovernedSqlValidator:
    """Validate generated SQL against the governed query plan before execution."""

    def validate(self, sql: str, governed_query_plan: dict[str, Any]) -> SqlValidationResult:
        errors: list[str] = []
        warnings: list[str] = []

        self._validate_select_only(sql=sql, errors=errors)
        self._validate_plan(governed_query_plan=governed_query_plan, errors=errors)

        allowed_tables = self._allowed_tables(governed_query_plan)
        allowed_columns = self._allowed_columns(governed_query_plan)
        self._validate_access_controls(allowed_columns=allowed_columns, errors=errors)
        self._validate_sql_references(
            sql=sql,
            allowed_tables=allowed_tables,
            allowed_columns=allowed_columns,
            errors=errors,
        )

        if not errors:
            self._validate_duckdb_compile(sql=sql, errors=errors)

        status = SqlValidationStatus.VALID if not errors else SqlValidationStatus.INVALID
        return SqlValidationResult(
            status=status,
            is_valid=not errors,
            errors=errors,
            warnings=warnings,
            allowed_tables=sorted(allowed_tables),
            allowed_columns=sorted(allowed_columns),
        )

    def _validate_select_only(self, sql: str, errors: list[str]) -> None:
        normalized = sql.strip()
        if not normalized:
            errors.append("Generated SQL is empty.")
            return

        if ";" in normalized[:-1] or "--" in normalized or "/*" in normalized or "*/" in normalized:
            errors.append("Generated SQL contains disallowed statement separators or comments.")

        tokens = {token.lower() for token in re.findall(r"[A-Za-z_]+", normalized)}
        blocked = sorted(tokens & BLOCKED_TOKENS)
        if blocked:
            errors.append(f"Generated SQL contains blocked tokens: {', '.join(blocked)}.")

        if sqlglot is not None:
            try:
                expression = sqlglot.parse_one(normalized, read="duckdb")
            except sqlglot.errors.SqlglotError as exc:
                errors.append(f"Generated SQL could not be parsed: {exc}.")
                return

            if expression.key != "select":
                errors.append("Generated SQL must be a single SELECT statement.")
        elif not normalized.lower().startswith("select"):
            errors.append("Generated SQL must be a SELECT statement.")

    def _validate_plan(self, governed_query_plan: dict[str, Any], errors: list[str]) -> None:
        metric = governed_query_plan.get("metric") or {}
        if not metric.get("certified"):
            errors.append("Metric is not certified in the governed catalog.")

        for dimension in governed_query_plan.get("dimensions") or []:
            if not dimension.get("certified"):
                errors.append(f"Dimension `{dimension.get('id')}` is not certified in the governed catalog.")

        for join in governed_query_plan.get("joins") or []:
            if not join.get("certified_flag"):
                errors.append(f"Join path `{join.get('join_path_id')}` is not certified in the governed catalog.")

    def _allowed_tables(self, governed_query_plan: dict[str, Any]) -> set[str]:
        allowed = {governed_query_plan["base_table"]}
        for join in governed_query_plan.get("joins") or []:
            allowed.add(join["from_table"])
            allowed.add(join["to_table"])
        for dimension in governed_query_plan.get("dimensions") or []:
            allowed.add(dimension["table_name"])
        return allowed

    def _allowed_columns(self, governed_query_plan: dict[str, Any]) -> set[str]:
        allowed: set[str] = set()
        metric = governed_query_plan.get("metric") or {}
        base_table = governed_query_plan.get("base_table")

        for column in metric.get("required_columns") or []:
            allowed.add(f"{base_table}.{column}")

        date_column = DATE_COLUMNS_BY_BASE_TABLE.get(base_table)
        if date_column:
            allowed.add(f"{base_table}.{date_column}")

        for dimension in governed_query_plan.get("dimensions") or []:
            allowed.add(f"{dimension['table_name']}.{dimension['column_name']}")

        for join in governed_query_plan.get("joins") or []:
            allowed.add(f"{join['from_table']}.{join['from_column']}")
            allowed.add(f"{join['to_table']}.{join['to_column']}")

        return allowed

    def _validate_sql_references(
        self,
        sql: str,
        allowed_tables: set[str],
        allowed_columns: set[str],
        errors: list[str],
    ) -> None:
        if sqlglot is None or exp is None:
            return

        try:
            expression = sqlglot.parse_one(sql, read="duckdb")
        except sqlglot.errors.SqlglotError:
            return

        alias_to_table: dict[str, str] = {}
        referenced_tables: set[str] = set()
        for table in expression.find_all(exp.Table):
            table_name = table.name
            referenced_tables.add(table_name)
            alias_to_table[table.alias_or_name] = table_name
            alias_to_table[table_name] = table_name

        unknown_tables = sorted(table for table in referenced_tables if table not in allowed_tables)
        if unknown_tables:
            errors.append(f"Generated SQL references tables outside the governed plan: {', '.join(unknown_tables)}.")

        allowed_column_names = {qualified.split(".", 1)[1] for qualified in allowed_columns}
        actual_columns: set[str] = set()
        unknown_columns: set[str] = set()

        for column in expression.find_all(exp.Column):
            column_name = column.name
            qualifier = column.table
            if qualifier:
                table_name = alias_to_table.get(qualifier)
                if not table_name:
                    unknown_columns.add(f"{qualifier}.{column_name}")
                    continue
                qualified = f"{table_name}.{column_name}"
                actual_columns.add(qualified)
                if qualified not in allowed_columns:
                    unknown_columns.add(qualified)
            else:
                if column_name in allowed_column_names:
                    continue
                unknown_columns.add(column_name)

        if unknown_columns:
            errors.append(
                "Generated SQL references columns outside the governed plan: "
                + ", ".join(sorted(unknown_columns))
                + "."
            )

    def _validate_access_controls(self, allowed_columns: set[str], errors: list[str]) -> None:
        for qualified_column in allowed_columns:
            table_name, column_name = qualified_column.split(".", 1)
            column = self._get_column(table_name=table_name, column_name=column_name)
            if not column:
                errors.append(f"Column `{qualified_column}` is not registered in metadata_column.")
                continue
            if column["restricted_flag"]:
                errors.append(f"Column `{qualified_column}` is restricted and cannot be used in generated SQL.")

    def _validate_duckdb_compile(self, sql: str, errors: list[str]) -> None:
        try:
            with connect(read_only=True) as conn:
                conn.execute(f"EXPLAIN {sql}")
        except Exception as exc:
            errors.append(f"DuckDB could not compile generated SQL: {exc}.")

    def _get_column(self, table_name: str, column_name: str) -> dict[str, Any] | None:
        for column in catalog_service.list_columns(table_name=table_name):
            if column["column_name"].lower() == column_name.lower():
                return column
        return None


governed_sql_validator = GovernedSqlValidator()
