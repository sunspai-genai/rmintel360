from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.app.semantic.resolver import ResolutionStatus, semantic_resolver
from backend.app.sql.generator import SqlGenerationStatus
from backend.app.sql.llm_generator import llm_governed_sql_generator


class SqlServiceStatus:
    GENERATED = "generated"
    NEEDS_CLARIFICATION = "needs_clarification"
    INFORMATION_ONLY = "information_only"
    UNSUPPORTED = "unsupported"
    INVALID = "invalid"


@dataclass(frozen=True)
class SqlServiceResult:
    message: str
    status: str
    semantic_status: str
    requires_clarification: bool
    requires_sql: bool
    sql_visible: bool
    generated_sql: str | None
    internal_sql: str | None
    sql_summary: str | None
    validation: dict[str, Any]
    semantic_result: dict[str, Any]
    governed_query_plan: dict[str, Any] | None
    assumptions: list[str]
    warnings: list[str]
    llm_generation: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "status": self.status,
            "semantic_status": self.semantic_status,
            "requires_clarification": self.requires_clarification,
            "requires_sql": self.requires_sql,
            "sql_visible": self.sql_visible,
            "generated_sql": self.generated_sql,
            "sql_summary": self.sql_summary,
            "validation": self.validation,
            "semantic_result": self.semantic_result,
            "governed_query_plan": self.governed_query_plan,
            "assumptions": self.assumptions,
            "warnings": self.warnings,
            "llm_generation": self.llm_generation,
        }


class GovernedSqlService:
    """Resolve user language and generate SQL only from governed semantics."""

    def generate_from_message(
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
    ) -> SqlServiceResult:
        trusted_plan = bool(context and context.get("trusted_governed_query_plan"))
        if governed_query_plan is not None and not trusted_plan:
            governed_query_plan = None

        semantic_result = None
        if governed_query_plan is None:
            semantic_result = semantic_resolver.resolve(
                message=message,
                intent=intent,
                selected_metric_id=selected_metric_id,
                selected_dimension_ids=selected_dimension_ids or [],
                awaiting_clarification=awaiting_clarification,
                context=context,
            ).to_dict()
            governed_query_plan = semantic_result["governed_query_plan"]
        else:
            semantic_result = {
                "message": message,
                "intent": governed_query_plan.get("intent", intent),
                "status": ResolutionStatus.RESOLVED,
                "requires_sql": True,
                "confidence": 1.0,
                "rationale": "Governed query plan was supplied directly.",
                "ambiguities": [],
                "resolved": {
                    "metric": governed_query_plan.get("metric"),
                    "dimensions": governed_query_plan.get("dimensions") or [],
                },
                "governed_query_plan": governed_query_plan,
                "assumptions": governed_query_plan.get("assumptions") or [],
                "retrieval_context": [],
            }

        if semantic_result["status"] != ResolutionStatus.RESOLVED or not governed_query_plan:
            return self._blocked_result(
                message=message,
                semantic_result=semantic_result,
                user_role=user_role,
                technical_mode=technical_mode,
            )

        generation_result = llm_governed_sql_generator.generate(governed_query_plan=governed_query_plan, limit=limit)
        sql_visible = self._is_sql_visible(user_role=user_role, technical_mode=technical_mode)
        status = (
            SqlServiceStatus.GENERATED
            if generation_result.status == SqlGenerationStatus.GENERATED
            else SqlServiceStatus.INVALID
        )

        return SqlServiceResult(
            message=message,
            status=status,
            semantic_status=semantic_result["status"],
            requires_clarification=False,
            requires_sql=True,
            sql_visible=sql_visible,
            generated_sql=generation_result.sql if sql_visible else None,
            internal_sql=generation_result.sql,
            sql_summary=generation_result.sql_summary,
            validation=generation_result.validation,
            semantic_result=semantic_result,
            governed_query_plan=governed_query_plan,
            assumptions=semantic_result.get("assumptions") or [],
            warnings=generation_result.warnings,
            llm_generation=generation_result.llm_generation,
        )

    def _blocked_result(
        self,
        message: str,
        semantic_result: dict[str, Any],
        user_role: str,
        technical_mode: bool,
    ) -> SqlServiceResult:
        status = semantic_result["status"]
        if status == ResolutionStatus.NEEDS_CLARIFICATION:
            service_status = SqlServiceStatus.NEEDS_CLARIFICATION
        elif status == ResolutionStatus.INFORMATION_ONLY:
            service_status = SqlServiceStatus.INFORMATION_ONLY
        elif status == ResolutionStatus.UNSUPPORTED:
            service_status = SqlServiceStatus.UNSUPPORTED
        else:
            service_status = SqlServiceStatus.INVALID

        return SqlServiceResult(
            message=message,
            status=service_status,
            semantic_status=status,
            requires_clarification=status == ResolutionStatus.NEEDS_CLARIFICATION,
            requires_sql=semantic_result["requires_sql"],
            sql_visible=self._is_sql_visible(user_role=user_role, technical_mode=technical_mode),
            generated_sql=None,
            internal_sql=None,
            sql_summary=None,
            validation={
                "status": "not_applicable",
                "is_valid": False,
                "errors": ["SQL generation blocked because semantic resolution is not complete."],
                "warnings": [],
                "allowed_tables": [],
                "allowed_columns": [],
            },
            semantic_result=semantic_result,
            governed_query_plan=semantic_result.get("governed_query_plan"),
            assumptions=semantic_result.get("assumptions") or [],
            warnings=[],
            llm_generation={},
        )

    def _is_sql_visible(self, user_role: str, technical_mode: bool) -> bool:
        return technical_mode or user_role in {"technical_user", "admin"}


governed_sql_service = GovernedSqlService()
