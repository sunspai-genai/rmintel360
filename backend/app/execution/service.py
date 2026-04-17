from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from backend.app.db.connection import connect
from backend.app.sql.service import SqlServiceStatus, governed_sql_service


class QueryExecutionStatus:
    EXECUTED = "executed"
    NEEDS_CLARIFICATION = "needs_clarification"
    INFORMATION_ONLY = "information_only"
    UNSUPPORTED = "unsupported"
    INVALID = "invalid"
    FAILED = "failed"


@dataclass(frozen=True)
class QueryExecutionResult:
    message: str
    status: str
    sql_result: dict[str, Any]
    result_table: dict[str, Any] | None
    execution_ms: int | None
    answer_summary: str | None
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "status": self.status,
            "sql_result": self.sql_result,
            "result_table": self.result_table,
            "execution_ms": self.execution_ms,
            "answer_summary": self.answer_summary,
            "warnings": self.warnings,
        }


class GovernedQueryExecutor:
    """Execute only SQL generated and validated from governed semantic plans."""

    def execute_from_message(
        self,
        message: str,
        intent: str | None = None,
        selected_metric_id: str | None = None,
        selected_dimension_ids: list[str] | None = None,
        awaiting_clarification: bool = False,
        user_role: str = "business_user",
        technical_mode: bool = False,
        context: dict[str, Any] | None = None,
        governed_query_plan: dict[str, Any] | None = None,
        limit: int = 100,
    ) -> QueryExecutionResult:
        generated = governed_sql_service.generate_from_message(
            message=message,
            intent=intent,
            selected_metric_id=selected_metric_id,
            selected_dimension_ids=selected_dimension_ids or [],
            awaiting_clarification=awaiting_clarification,
            user_role=user_role,
            technical_mode=technical_mode,
            context=context,
            governed_query_plan=governed_query_plan,
            limit=limit,
        )
        sql_result = generated.to_dict()

        if sql_result["status"] != SqlServiceStatus.GENERATED:
            return self._blocked_result(message=message, sql_result=sql_result)

        validation = sql_result.get("validation") or {}
        if not validation.get("is_valid"):
            return QueryExecutionResult(
                message=message,
                status=QueryExecutionStatus.INVALID,
                sql_result=sql_result,
                result_table=None,
                execution_ms=None,
                answer_summary=None,
                warnings=["Execution blocked because SQL validation did not pass."],
            )

        internal_sql = generated.internal_sql
        if not internal_sql:
            return QueryExecutionResult(
                message=message,
                status=QueryExecutionStatus.INVALID,
                sql_result=sql_result,
                result_table=None,
                execution_ms=None,
                answer_summary=None,
                warnings=["Execution blocked because the internal SQL text is unavailable."],
            )

        started = time.perf_counter()
        try:
            result_table = self._execute_sql(sql=internal_sql, limit=limit)
        except Exception as exc:
            return QueryExecutionResult(
                message=message,
                status=QueryExecutionStatus.FAILED,
                sql_result=sql_result,
                result_table=None,
                execution_ms=int((time.perf_counter() - started) * 1000),
                answer_summary=None,
                warnings=[f"DuckDB execution failed: {exc}"],
            )

        execution_ms = int((time.perf_counter() - started) * 1000)
        return QueryExecutionResult(
            message=message,
            status=QueryExecutionStatus.EXECUTED,
            sql_result=sql_result,
            result_table=result_table,
            execution_ms=execution_ms,
            answer_summary=self._answer_summary(sql_result=sql_result, result_table=result_table),
            warnings=sql_result.get("warnings") or [],
        )

    def _blocked_result(self, message: str, sql_result: dict[str, Any]) -> QueryExecutionResult:
        sql_status = sql_result["status"]
        if sql_status == SqlServiceStatus.NEEDS_CLARIFICATION:
            status = QueryExecutionStatus.NEEDS_CLARIFICATION
        elif sql_status == SqlServiceStatus.INFORMATION_ONLY:
            status = QueryExecutionStatus.INFORMATION_ONLY
        elif sql_status == SqlServiceStatus.UNSUPPORTED:
            status = QueryExecutionStatus.UNSUPPORTED
        else:
            status = QueryExecutionStatus.INVALID

        return QueryExecutionResult(
            message=message,
            status=status,
            sql_result=sql_result,
            result_table=None,
            execution_ms=None,
            answer_summary=None,
            warnings=[],
        )

    def _execute_sql(self, sql: str, limit: int) -> dict[str, Any]:
        with connect(read_only=True) as conn:
            result = conn.execute(sql)
            columns = [column[0] for column in result.description or []]
            rows = result.fetchall()

        normalized_rows = [
            {column: self._normalize_value(value) for column, value in zip(columns, row)}
            for row in rows
        ]
        return {
            "columns": columns,
            "rows": normalized_rows,
            "row_count": len(normalized_rows),
            "limit": limit,
            "truncated": len(normalized_rows) >= limit,
        }

    def _answer_summary(self, sql_result: dict[str, Any], result_table: dict[str, Any]) -> str:
        row_count = result_table["row_count"]
        sql_summary = sql_result.get("sql_summary") or "Governed query"
        if row_count == 1:
            return f"{sql_summary} returned 1 row."
        return f"{sql_summary} returned {row_count} rows."

    def _normalize_value(self, value: Any) -> Any:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        return value


governed_query_executor = GovernedQueryExecutor()
