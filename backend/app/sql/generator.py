from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from backend.app.sql.validator import governed_sql_validator


class SqlGenerationStatus:
    GENERATED = "generated"
    INVALID = "invalid"


TABLE_ALIASES = {
    "fact_deposit_balance_daily": "fdb",
    "fact_deposit_transaction": "fdt",
    "fact_loan_balance_monthly": "flbm",
    "fact_loan_payment": "flp",
    "fact_credit_risk_snapshot": "fcrs",
    "fact_relationship_profitability": "frp",
    "dim_account": "da",
    "dim_branch": "db",
    "dim_customer": "dc",
    "dim_date": "dd",
    "dim_loan": "dl",
    "dim_product": "dp",
}

DATE_COLUMNS_BY_BASE_TABLE = {
    "fact_deposit_balance_daily": "as_of_date",
    "fact_deposit_transaction": "transaction_date",
    "fact_loan_balance_monthly": "as_of_month",
    "fact_loan_payment": "payment_date",
    "fact_credit_risk_snapshot": "as_of_month",
    "fact_relationship_profitability": "as_of_month",
}

MONTHLY_FACT_TABLES = {
    "fact_loan_balance_monthly",
    "fact_credit_risk_snapshot",
    "fact_relationship_profitability",
}


@dataclass(frozen=True)
class SqlGenerationResult:
    status: str
    sql: str | None
    sql_summary: str | None
    validation: dict[str, Any]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "sql": self.sql,
            "sql_summary": self.sql_summary,
            "validation": self.validation,
            "warnings": self.warnings,
        }


class GovernedSqlGenerator:
    """Generate constrained aggregate SELECT SQL from a governed query plan."""

    def generate(self, governed_query_plan: dict[str, Any], limit: int = 100) -> SqlGenerationResult:
        aliases = self._aliases_for_plan(governed_query_plan)
        base_table = governed_query_plan["base_table"]
        base_alias = aliases[base_table]

        select_parts = self._select_parts(governed_query_plan=governed_query_plan, aliases=aliases)
        from_part = f"FROM {base_table} {base_alias}"
        join_parts = self._join_parts(governed_query_plan=governed_query_plan, aliases=aliases)
        where_parts = self._where_parts(governed_query_plan=governed_query_plan, aliases=aliases)
        group_by_parts = self._group_by_parts(governed_query_plan=governed_query_plan, aliases=aliases)

        lines = ["SELECT", "    " + ",\n    ".join(select_parts), from_part]
        lines.extend(join_parts)
        if where_parts:
            lines.append("WHERE " + "\n  AND ".join(where_parts))
        if group_by_parts:
            lines.append("GROUP BY " + ", ".join(group_by_parts))
            lines.append("ORDER BY " + ", ".join(group_by_parts))
        lines.append(f"LIMIT {limit}")

        sql = "\n".join(lines)
        validation = governed_sql_validator.validate(sql=sql, governed_query_plan=governed_query_plan)
        return SqlGenerationResult(
            status=SqlGenerationStatus.GENERATED if validation.is_valid else SqlGenerationStatus.INVALID,
            sql=sql,
            sql_summary=self._summary(governed_query_plan),
            validation=validation.to_dict(),
            warnings=validation.warnings,
        )

    def _select_parts(self, governed_query_plan: dict[str, Any], aliases: dict[str, str]) -> list[str]:
        select_parts = []
        for dimension in governed_query_plan.get("dimensions") or []:
            table_alias = aliases[dimension["table_name"]]
            column_name = dimension["column_name"]
            output_name = self._output_name(dimension["id"])
            select_parts.append(f"{table_alias}.{column_name} AS {output_name}")

        metric = governed_query_plan["metric"]
        metric_expression = self._metric_expression(metric["calculation_sql"], aliases)
        select_parts.append(f"{metric_expression} AS {self._output_name(metric['id'])}")
        return select_parts

    def _join_parts(self, governed_query_plan: dict[str, Any], aliases: dict[str, str]) -> list[str]:
        joined_tables = {governed_query_plan["base_table"]}
        join_parts = []

        for join in governed_query_plan.get("joins") or []:
            from_table = join["from_table"]
            to_table = join["to_table"]
            join_type = join["join_type"].upper()

            if from_table in joined_tables and to_table not in joined_tables:
                left_alias = aliases[from_table]
                right_alias = aliases[to_table]
                table_to_join = to_table
                condition = f"{left_alias}.{join['from_column']} = {right_alias}.{join['to_column']}"
                joined_tables.add(to_table)
            elif to_table in joined_tables and from_table not in joined_tables:
                left_alias = aliases[to_table]
                right_alias = aliases[from_table]
                table_to_join = from_table
                condition = f"{right_alias}.{join['from_column']} = {left_alias}.{join['to_column']}"
                joined_tables.add(from_table)
            else:
                continue

            join_parts.append(f"{join_type} JOIN {table_to_join} {right_alias} ON {condition}")

        return join_parts

    def _where_parts(self, governed_query_plan: dict[str, Any], aliases: dict[str, str]) -> list[str]:
        base_table = governed_query_plan["base_table"]
        base_alias = aliases[base_table]
        date_column = DATE_COLUMNS_BY_BASE_TABLE.get(base_table)
        if not date_column:
            return []

        filters = []
        for filter_item in governed_query_plan.get("filters") or []:
            filter_sql = self._date_filter_sql(
                filter_id=filter_item["filter_id"],
                base_table=base_table,
                base_alias=base_alias,
                date_column=date_column,
            )
            if filter_sql:
                filters.append(filter_sql)
        return filters

    def _date_filter_sql(self, filter_id: str, base_table: str, base_alias: str, date_column: str) -> str | None:
        max_date = f"(SELECT MAX({date_column}) FROM {base_table})"
        column_ref = f"{base_alias}.{date_column}"

        if filter_id == "latest_complete_month":
            if base_table in MONTHLY_FACT_TABLES:
                return f"{column_ref} = {max_date}"
            return f"{column_ref} >= date_trunc('month', {max_date}) AND {column_ref} <= {max_date}"

        if filter_id == "current_month_to_date":
            return f"{column_ref} >= date_trunc('month', {max_date}) AND {column_ref} <= {max_date}"

        if filter_id in {"last_12_months", "month_trend"}:
            return f"{column_ref} >= date_trunc('month', {max_date}) - INTERVAL 11 MONTH AND {column_ref} <= {max_date}"

        if filter_id in {"latest_complete_quarter", "quarter_trend"}:
            return f"{column_ref} >= date_trunc('quarter', {max_date}) AND {column_ref} <= {max_date}"

        if filter_id == "current_quarter_to_date":
            return f"{column_ref} >= date_trunc('quarter', {max_date}) AND {column_ref} <= {max_date}"

        if filter_id in {"latest_complete_year", "current_year_to_date", "year_trend"}:
            return f"{column_ref} >= date_trunc('year', {max_date}) AND {column_ref} <= {max_date}"

        return None

    def _group_by_parts(self, governed_query_plan: dict[str, Any], aliases: dict[str, str]) -> list[str]:
        return [
            f"{aliases[dimension['table_name']]}.{dimension['column_name']}"
            for dimension in governed_query_plan.get("dimensions") or []
        ]

    def _aliases_for_plan(self, governed_query_plan: dict[str, Any]) -> dict[str, str]:
        tables = [governed_query_plan["base_table"]]
        for join in governed_query_plan.get("joins") or []:
            tables.extend([join["from_table"], join["to_table"]])
        for dimension in governed_query_plan.get("dimensions") or []:
            tables.append(dimension["table_name"])

        aliases = {}
        used_aliases = set()
        for table_name in tables:
            if table_name in aliases:
                continue
            alias = TABLE_ALIASES.get(table_name, self._fallback_alias(table_name))
            if alias in used_aliases:
                alias = f"{alias}{len(used_aliases) + 1}"
            aliases[table_name] = alias
            used_aliases.add(alias)
        return aliases

    def _metric_expression(self, calculation_sql: str, aliases: dict[str, str]) -> str:
        expression = calculation_sql
        for table_name, alias in aliases.items():
            expected_alias = TABLE_ALIASES.get(table_name)
            if expected_alias and expected_alias != alias:
                expression = re.sub(rf"\b{re.escape(expected_alias)}\.", f"{alias}.", expression)
        return expression

    def _summary(self, governed_query_plan: dict[str, Any]) -> str:
        metric = governed_query_plan["metric"]["business_name"]
        dimensions = governed_query_plan.get("dimensions") or []
        if not dimensions:
            return f"Aggregate {metric} using certified governed metadata."
        dimension_names = ", ".join(dimension["business_name"] for dimension in dimensions)
        return f"Aggregate {metric} by {dimension_names} using certified governed metadata."

    def _output_name(self, governed_id: str) -> str:
        return re.sub(r"[^a-z0-9_]+", "_", governed_id.split(".")[-1].lower()).strip("_")

    def _fallback_alias(self, table_name: str) -> str:
        parts = table_name.split("_")
        return "".join(part[0] for part in parts if part)[:4] or "t"


governed_sql_generator = GovernedSqlGenerator()

