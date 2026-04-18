from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from backend.app.execution.service import QueryExecutionStatus, governed_query_executor


class AnswerStatus:
    ANSWERED = "answered"
    NEEDS_CLARIFICATION = "needs_clarification"
    INFORMATION_ONLY = "information_only"
    UNSUPPORTED = "unsupported"
    INVALID = "invalid"
    FAILED = "failed"


@dataclass(frozen=True)
class AnalyticalAnswerResult:
    message: str
    status: str
    answer: str
    key_points: list[str]
    result_overview: dict[str, Any]
    execution_result: dict[str, Any]
    assumptions: list[str]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "status": self.status,
            "answer": self.answer,
            "key_points": self.key_points,
            "result_overview": self.result_overview,
            "execution_result": self.execution_result,
            "assumptions": self.assumptions,
            "warnings": self.warnings,
        }


class GovernedAnswerGenerator:
    """Create concise analytical answers from governed execution results."""

    def answer_from_message(
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
    ) -> AnalyticalAnswerResult:
        execution = governed_query_executor.execute_from_message(
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
        ).to_dict()

        if execution["status"] != QueryExecutionStatus.EXECUTED:
            return self._blocked_answer(message=message, execution_result=execution)

        return self.answer_from_execution(message=message, execution_result=execution)

    def answer_from_execution(self, message: str, execution_result: dict[str, Any]) -> AnalyticalAnswerResult:
        if execution_result["status"] != QueryExecutionStatus.EXECUTED:
            return self._blocked_answer(message=message, execution_result=execution_result)

        analytical = self._build_analytical_answer(execution_result)
        sql_result = execution_result["sql_result"]
        return AnalyticalAnswerResult(
            message=message,
            status=AnswerStatus.ANSWERED,
            answer=analytical["answer"],
            key_points=analytical["key_points"],
            result_overview=analytical["result_overview"],
            execution_result=execution_result,
            assumptions=sql_result.get("assumptions") or [],
            warnings=execution_result.get("warnings") or [],
        )

    def _blocked_answer(self, message: str, execution_result: dict[str, Any]) -> AnalyticalAnswerResult:
        status_map = {
            QueryExecutionStatus.NEEDS_CLARIFICATION: AnswerStatus.NEEDS_CLARIFICATION,
            QueryExecutionStatus.INFORMATION_ONLY: AnswerStatus.INFORMATION_ONLY,
            QueryExecutionStatus.UNSUPPORTED: AnswerStatus.UNSUPPORTED,
            QueryExecutionStatus.FAILED: AnswerStatus.FAILED,
        }
        status = status_map.get(execution_result["status"], AnswerStatus.INVALID)
        if status == AnswerStatus.NEEDS_CLARIFICATION:
            answer = "I need one governed choice before I can answer this analytically."
        elif status == AnswerStatus.INFORMATION_ONLY:
            answer = "This request is informational and should be answered from governed metadata."
        elif status == AnswerStatus.UNSUPPORTED:
            answer = "I can help with governed commercial banking metadata and analytics questions."
        else:
            answer = "I could not generate a governed analytical answer for this request."

        return AnalyticalAnswerResult(
            message=message,
            status=status,
            answer=answer,
            key_points=[],
            result_overview={},
            execution_result=execution_result,
            assumptions=[],
            warnings=execution_result.get("warnings") or [],
        )

    def _build_analytical_answer(self, execution: dict[str, Any]) -> dict[str, Any]:
        result_table = execution["result_table"]
        sql_result = execution["sql_result"]
        semantic_result = sql_result["semantic_result"]
        query_plan = semantic_result["governed_query_plan"]
        rows = result_table["rows"]
        metric = query_plan["metric"]
        dimensions = query_plan.get("dimensions") or []
        metric_column = self._metric_column(result_table=result_table, metric=metric)
        dimension_columns = [self._output_name(dimension["id"]) for dimension in dimensions]
        metric_label = metric["business_name"]
        dimension_label = ", ".join(dimension["business_name"] for dimension in dimensions) or "overall portfolio"

        if not rows:
            return {
                "answer": f"{metric_label} by {dimension_label} returned no rows for the governed filters.",
                "key_points": ["No result rows were returned."],
                "result_overview": {
                    "row_count": 0,
                    "metric": metric_label,
                    "dimensions": [dimension["business_name"] for dimension in dimensions],
                    "highest": None,
                    "lowest": None,
                },
            }

        numeric_rows = [row for row in rows if isinstance(row.get(metric_column), (int, float))]
        if not numeric_rows:
            return {
                "answer": f"{metric_label} returned {len(rows)} rows, but no numeric metric column was available to summarize.",
                "key_points": [f"Returned {len(rows)} rows."],
                "result_overview": {
                    "row_count": len(rows),
                    "metric": metric_label,
                    "dimensions": [dimension["business_name"] for dimension in dimensions],
                    "highest": None,
                    "lowest": None,
                },
            }

        sorted_rows = sorted(numeric_rows, key=lambda row: row[metric_column], reverse=True)
        highest = sorted_rows[0]
        lowest = sorted_rows[-1]
        latest = rows[-1]
        first = rows[0]
        value_style = self._value_style(metric)

        highest_label = self._row_label(highest, dimension_columns)
        lowest_label = self._row_label(lowest, dimension_columns)
        highest_value = self._format_value(highest[metric_column], value_style)
        lowest_value = self._format_value(lowest[metric_column], value_style)

        key_points = [
            f"Returned {result_table['row_count']} governed result rows.",
            f"Highest {metric_label}: {highest_label} at {highest_value}.",
            f"Lowest {metric_label}: {lowest_label} at {lowest_value}.",
        ]

        trend_point = self._trend_point(
            first=first,
            latest=latest,
            metric_column=metric_column,
            dimension_columns=dimension_columns,
            value_style=value_style,
        )
        if trend_point:
            key_points.append(trend_point)
        if result_table.get("truncated"):
            key_points.append(f"Result rows were limited to {result_table['limit']} rows.")

        answer = (
            f"{metric_label} by {dimension_label} returned {result_table['row_count']} rows. "
            f"{highest_label} is highest at {highest_value}, while {lowest_label} is lowest at {lowest_value}."
        )
        if trend_point:
            answer = f"{answer} {trend_point}"

        return {
            "answer": answer,
            "key_points": key_points,
            "result_overview": {
                "row_count": result_table["row_count"],
                "metric": metric_label,
                "metric_column": metric_column,
                "dimensions": [dimension["business_name"] for dimension in dimensions],
                "highest": {
                    "label": highest_label,
                    "value": highest[metric_column],
                    "formatted_value": highest_value,
                },
                "lowest": {
                    "label": lowest_label,
                    "value": lowest[metric_column],
                    "formatted_value": lowest_value,
                },
                "truncated": result_table.get("truncated", False),
            },
        }

    def _metric_column(self, result_table: dict[str, Any], metric: dict[str, Any]) -> str:
        preferred = metric["id"].split(".")[-1]
        if preferred in result_table["columns"]:
            return preferred
        return result_table["columns"][-1]

    def _row_label(self, row: dict[str, Any], dimension_columns: list[str]) -> str:
        if not dimension_columns:
            return "Overall"
        values = [str(row.get(column)) for column in dimension_columns]
        return " / ".join(values)

    def _trend_point(
        self,
        first: dict[str, Any],
        latest: dict[str, Any],
        metric_column: str,
        dimension_columns: list[str],
        value_style: str,
    ) -> str | None:
        if not dimension_columns:
            return None
        first_value = first.get(metric_column)
        latest_value = latest.get(metric_column)
        if not isinstance(first_value, (int, float)) or not isinstance(latest_value, (int, float)):
            return None
        dimension_name = dimension_columns[0]
        if "month" not in dimension_name and "date" not in dimension_name:
            return None

        delta = latest_value - first_value
        direction = "increased" if delta >= 0 else "decreased"
        first_label = str(first.get(dimension_name))
        latest_label = str(latest.get(dimension_name))
        return (
            f"From {first_label} to {latest_label}, the metric {direction} by "
            f"{self._format_delta(abs(delta), value_style)}."
        )

    def _value_style(self, metric: dict[str, Any]) -> str:
        text = f"{metric['business_name']} {metric['id']}".lower()
        if any(term in text for term in ["rate", "utilization", "delinquency", "percent"]):
            return "percent"
        if any(term in text for term in ["balance", "amount", "deposit", "income", "profit", "exposure"]):
            return "currency"
        return "number"

    def _format_value(self, value: float, value_style: str) -> str:
        if value_style == "percent":
            return f"{value * 100:.2f}%"
        if value_style == "currency":
            return f"${value:,.2f}"
        return f"{value:,.2f}"

    def _format_delta(self, value: float, value_style: str) -> str:
        if value_style == "percent":
            return f"{value * 100:.2f} percentage points"
        return self._format_value(value, value_style)

    def _output_name(self, governed_id: str) -> str:
        return re.sub(r"[^a-z0-9_]+", "_", governed_id.split(".")[-1].lower()).strip("_")


governed_answer_generator = GovernedAnswerGenerator()
