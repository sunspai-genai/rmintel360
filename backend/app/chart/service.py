from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import plotly.graph_objects as go

from backend.app.answer.service import AnswerStatus, governed_answer_generator


class ChartStatus:
    GENERATED = "generated"
    NEEDS_CLARIFICATION = "needs_clarification"
    INFORMATION_ONLY = "information_only"
    UNSUPPORTED = "unsupported"
    INVALID = "invalid"
    FAILED = "failed"


@dataclass(frozen=True)
class ChartGenerationResult:
    message: str
    status: str
    chart_spec: dict[str, Any] | None
    answer_result: dict[str, Any]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "status": self.status,
            "chart_spec": self.chart_spec,
            "answer_result": self.answer_result,
            "warnings": self.warnings,
        }


class GovernedChartGenerator:
    """Build simple Plotly chart specs from governed analytical answers."""

    def chart_from_message(
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
        chart_type: str | None = None,
    ) -> ChartGenerationResult:
        answer_result = governed_answer_generator.answer_from_message(
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
        return self.chart_from_answer_result(
            message=message,
            answer_result=answer_result,
            chart_type=chart_type,
        )

    def chart_from_answer_result(
        self,
        message: str,
        answer_result: dict[str, Any],
        chart_type: str | None = None,
    ) -> ChartGenerationResult:
        if answer_result["status"] != AnswerStatus.ANSWERED:
            return ChartGenerationResult(
                message=message,
                status=self._blocked_status(answer_result["status"]),
                chart_spec=None,
                answer_result=answer_result,
                warnings=["Chart generation blocked because a governed analytical answer was not available."],
            )

        execution_result = answer_result["execution_result"]
        result_table = execution_result.get("result_table")
        if not result_table or not result_table.get("rows"):
            return ChartGenerationResult(
                message=message,
                status=ChartStatus.INVALID,
                chart_spec=None,
                answer_result=answer_result,
                warnings=["Chart generation blocked because the result table is empty."],
            )

        chart_spec = self._build_chart_spec(
            answer_result=answer_result,
            chart_type=chart_type,
        )
        return ChartGenerationResult(
            message=message,
            status=ChartStatus.GENERATED,
            chart_spec=chart_spec,
            answer_result=answer_result,
            warnings=answer_result.get("warnings") or [],
        )

    def _build_chart_spec(self, answer_result: dict[str, Any], chart_type: str | None) -> dict[str, Any]:
        execution_result = answer_result["execution_result"]
        result_table = execution_result["result_table"]
        sql_result = execution_result["sql_result"]
        query_plan = sql_result["semantic_result"]["governed_query_plan"]
        rows = result_table["rows"]
        columns = result_table["columns"]
        metric = query_plan["metric"]
        dimensions = query_plan.get("dimensions") or []
        metric_column = self._metric_column(
            result_table=result_table,
            answer_result=answer_result,
            metric=metric,
            dimensions=dimensions,
        )
        x_column = self._x_column(columns=columns, metric_column=metric_column, dimensions=dimensions)
        resolved_chart_type = self._chart_type(
            requested=chart_type,
            x_column=x_column or "",
            dimensions=dimensions,
        )

        x_values = [row[x_column] for row in rows] if x_column else ["Overall" for _ in rows]
        y_values = [row[metric_column] for row in rows]
        title = self._title(metric_name=metric["business_name"], dimensions=dimensions)
        x_axis_column = x_column or "__category__"
        x_axis_label = self._axis_label(x_column) if x_column else "Category"

        if resolved_chart_type == "line":
            figure = go.Figure(
                data=[
                    go.Scatter(
                        x=x_values,
                        y=y_values,
                        mode="lines+markers",
                        name=metric["business_name"],
                    )
                ]
            )
        else:
            figure = go.Figure(
                data=[
                    go.Bar(
                        x=x_values,
                        y=y_values,
                        name=metric["business_name"],
                    )
                ]
            )

        figure.update_layout(
            title=title,
            xaxis_title=x_axis_label,
            yaxis_title=metric["business_name"],
            template="plotly_white",
            margin={"l": 56, "r": 24, "t": 72, "b": 72},
        )

        return {
            "chart_type": resolved_chart_type,
            "title": title,
            "x_axis": {"column": x_axis_column, "label": x_axis_label},
            "y_axis": {"column": metric_column, "label": metric["business_name"]},
            "plotly_json": figure.to_plotly_json(),
            "data_summary": {
                "row_count": result_table["row_count"],
                "truncated": result_table.get("truncated", False),
                "metric": metric["business_name"],
                "dimensions": [dimension["business_name"] for dimension in dimensions],
            },
        }

    def _blocked_status(self, answer_status: str) -> str:
        if answer_status == AnswerStatus.NEEDS_CLARIFICATION:
            return ChartStatus.NEEDS_CLARIFICATION
        if answer_status == AnswerStatus.INFORMATION_ONLY:
            return ChartStatus.INFORMATION_ONLY
        if answer_status == AnswerStatus.UNSUPPORTED:
            return ChartStatus.UNSUPPORTED
        if answer_status == AnswerStatus.FAILED:
            return ChartStatus.FAILED
        return ChartStatus.INVALID

    def _chart_type(self, requested: str | None, x_column: str, dimensions: list[dict[str, Any]]) -> str:
        if requested in {"bar", "line"}:
            return requested
        if "month" in x_column or "date" in x_column:
            return "line"
        if dimensions and any("month" in dimension["column_name"] for dimension in dimensions):
            return "line"
        return "bar"

    def _metric_column(
        self,
        result_table: dict[str, Any],
        answer_result: dict[str, Any],
        metric: dict[str, Any],
        dimensions: list[dict[str, Any]],
    ) -> str:
        columns = result_table["columns"]
        dimension_columns = {dimension["column_name"] for dimension in dimensions}
        overview_metric_column = (answer_result.get("result_overview") or {}).get("metric_column")
        if overview_metric_column in columns and overview_metric_column not in dimension_columns:
            return overview_metric_column

        preferred = metric["id"].split(".")[-1]
        if preferred in columns and preferred not in dimension_columns:
            return preferred

        numeric_columns = [
            column
            for column in columns
            if column not in dimension_columns
            and any(isinstance(row.get(column), (int, float)) for row in result_table["rows"])
        ]
        if numeric_columns:
            return numeric_columns[-1]
        return columns[-1]

    def _x_column(self, columns: list[str], metric_column: str, dimensions: list[dict[str, Any]]) -> str | None:
        for dimension in dimensions:
            dimension_column = dimension["column_name"]
            if dimension_column in columns and dimension_column != metric_column:
                return dimension_column
        for column in columns:
            if column != metric_column:
                return column
        return None

    def _axis_label(self, column_name: str) -> str:
        return " ".join(part.capitalize() for part in column_name.split("_"))

    def _title(self, metric_name: str, dimensions: list[dict[str, Any]]) -> str:
        if not dimensions:
            return metric_name
        dimension_names = ", ".join(dimension["business_name"] for dimension in dimensions)
        return f"{metric_name} by {dimension_names}"


governed_chart_generator = GovernedChartGenerator()
