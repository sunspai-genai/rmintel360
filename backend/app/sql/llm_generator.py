from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable

from backend.app.llm.client import llm_client
from backend.app.sql.generator import SqlGenerationResult, SqlGenerationStatus, governed_sql_generator
from backend.app.sql.validator import governed_sql_validator


@dataclass(frozen=True)
class LlmSqlGenerationResult:
    status: str
    sql: str | None
    sql_summary: str | None
    validation: dict[str, Any]
    warnings: list[str]
    llm_generation: dict[str, Any]

    def to_sql_generation_result(self) -> SqlGenerationResult:
        return SqlGenerationResult(
            status=self.status,
            sql=self.sql,
            sql_summary=self.sql_summary,
            validation=self.validation,
            warnings=self.warnings,
        )


class LlmGovernedSqlGenerator:
    """Generate SQL with an LLM, then enforce governed plan validation."""

    def generate(self, governed_query_plan: dict[str, Any], limit: int = 100) -> LlmSqlGenerationResult:
        fallback_result = governed_sql_generator.generate(governed_query_plan=governed_query_plan, limit=limit)
        first = self._invoke_sql_generation(
            task_name="sql_generation",
            system_prompt=self._generation_prompt(),
            input_payload={
                "governed_query_plan": governed_query_plan,
                "limit": limit,
                "dialect": "duckdb",
                "rules": self._rules(),
            },
            fallback=lambda: self._fallback_payload(fallback_result),
        )
        result = self._result_from_payload(
            payload=first,
            governed_query_plan=governed_query_plan,
            limit=limit,
            fallback_result=fallback_result,
        )
        if result.status == SqlGenerationStatus.GENERATED:
            return result

        repaired = self._invoke_sql_generation(
            task_name="sql_repair",
            system_prompt=self._repair_prompt(),
            input_payload={
                "governed_query_plan": governed_query_plan,
                "limit": limit,
                "dialect": "duckdb",
                "candidate_sql": result.sql,
                "validation_errors": result.validation.get("errors") or [],
                "rules": self._rules(),
            },
            fallback=lambda: self._fallback_payload(fallback_result),
        )
        return self._result_from_payload(
            payload=repaired,
            governed_query_plan=governed_query_plan,
            limit=limit,
            fallback_result=fallback_result,
        )

    def _invoke_sql_generation(
        self,
        *,
        task_name: str,
        system_prompt: str,
        input_payload: dict[str, Any],
        fallback: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        payload = llm_client.invoke_json(
            task_name=task_name,
            system_prompt=system_prompt,
            input_payload=input_payload,
            fallback=fallback,
        )
        if not isinstance(payload.get("sql"), str) or not payload.get("sql", "").strip():
            payload = fallback()
            payload.setdefault("llm_provider", "local_sql_template_fallback")
            payload.setdefault("llm_task", task_name)
        return payload

    def _result_from_payload(
        self,
        *,
        payload: dict[str, Any],
        governed_query_plan: dict[str, Any],
        limit: int,
        fallback_result: SqlGenerationResult,
    ) -> LlmSqlGenerationResult:
        sql = self._normalize_sql(payload.get("sql"), limit=limit)
        validation = governed_sql_validator.validate(sql=sql or "", governed_query_plan=governed_query_plan)
        warnings = list(validation.warnings)
        if payload.get("warning"):
            warnings.append(str(payload["warning"]))
        return LlmSqlGenerationResult(
            status=SqlGenerationStatus.GENERATED if validation.is_valid else SqlGenerationStatus.INVALID,
            sql=sql,
            sql_summary=payload.get("sql_summary") or fallback_result.sql_summary,
            validation=validation.to_dict(),
            warnings=warnings,
            llm_generation={
                "provider": payload.get("llm_provider"),
                "task": payload.get("llm_task"),
                "error": payload.get("llm_error"),
                "rationale": payload.get("rationale"),
                "referenced_tables": payload.get("referenced_tables") or [],
                "referenced_columns": payload.get("referenced_columns") or [],
            },
        )

    def _fallback_payload(self, fallback_result: SqlGenerationResult) -> dict[str, Any]:
        return {
            "sql": fallback_result.sql,
            "sql_summary": fallback_result.sql_summary,
            "rationale": "Local governed template generated SQL because the LLM was unavailable.",
            "referenced_tables": fallback_result.validation.get("allowed_tables") if fallback_result.validation else [],
            "referenced_columns": fallback_result.validation.get("allowed_columns") if fallback_result.validation else [],
        }

    def _normalize_sql(self, sql: Any, limit: int) -> str | None:
        if not isinstance(sql, str):
            return None
        normalized = sql.strip().rstrip(";")
        if not normalized:
            return None
        if not self._has_trailing_limit(normalized):
            normalized = f"{normalized}\nLIMIT {limit}"
        return normalized

    def _has_trailing_limit(self, sql: str) -> bool:
        return bool(re.search(r"\s+LIMIT\s+\d+\s*$", sql, flags=re.IGNORECASE))

    def _generation_prompt(self) -> str:
        return (
            "You are an enterprise banking SQL generator. Generate one DuckDB SELECT statement only from the "
            "provided governed query plan. Use only the plan's metric, dimensions, joins, filters, tables, and "
            "columns. Do not invent columns, do not use restricted columns, and do not add unapproved joins. "
            "Return JSON with sql, sql_summary, rationale, referenced_tables, and referenced_columns."
        )

    def _repair_prompt(self) -> str:
        return (
            "Repair the DuckDB SQL so it passes the governed validator. Keep the same business meaning, use only "
            "the governed query plan, and return JSON with sql, sql_summary, rationale, referenced_tables, and "
            "referenced_columns."
        )

    def _rules(self) -> list[str]:
        return [
            "single SELECT only",
            "no comments or statement separators",
            "include the requested LIMIT",
            "group by every non-aggregated selected dimension",
            "use the metric calculation_sql exactly unless alias changes are required",
            "use only certified joins from the governed query plan",
        ]


llm_governed_sql_generator = LlmGovernedSqlGenerator()
