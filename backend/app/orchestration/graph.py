from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, StateGraph

from backend.app.answer.service import governed_answer_generator
from backend.app.catalog.service import catalog_service
from backend.app.chart.service import governed_chart_generator
from backend.app.execution.service import QueryExecutionStatus, governed_query_executor
from backend.app.intent.classifier import IntentType, intent_classifier
from backend.app.sql.service import SqlServiceStatus, governed_sql_service


class OrchestrationStatus:
    ANSWERED = "answered"
    EXECUTED = "executed"
    SQL_GENERATED = "sql_generated"
    NEEDS_CLARIFICATION = "needs_clarification"
    UNSUPPORTED = "unsupported"
    INVALID = "invalid"


class OrchestrationState(TypedDict, total=False):
    message: str
    intent: Optional[str]
    selected_metric_id: Optional[str]
    selected_dimension_ids: List[str]
    awaiting_clarification: bool
    user_role: str
    technical_mode: bool
    conversation_id: Optional[str]
    context: Optional[Dict[str, Any]]
    governed_query_plan: Optional[Dict[str, Any]]
    limit: int
    execute_sql: bool
    intent_result: Dict[str, Any]
    route: str
    status: str
    answer: str
    next_action: str
    requires_clarification: bool
    clarification_options: List[Dict[str, Any]]
    sql_result: Optional[Dict[str, Any]]
    execution_result: Optional[Dict[str, Any]]
    answer_result: Optional[Dict[str, Any]]
    chart_result: Optional[Dict[str, Any]]
    semantic_result: Optional[Dict[str, Any]]
    metadata_context: List[Dict[str, Any]]
    assumptions: List[str]
    warnings: List[str]
    graph_trace: List[str]


class GovernedAssistantGraph:
    """LangGraph workflow for governed commercial banking assistant routing."""

    def __init__(self) -> None:
        graph = StateGraph(OrchestrationState)
        graph.add_node("classify", self._classify)
        graph.add_node("information", self._information)
        graph.add_node("analytics", self._analytics)
        graph.add_node("clarification", self._clarification)
        graph.add_node("unsupported", self._unsupported)
        graph.set_entry_point("classify")
        graph.add_conditional_edges(
            "classify",
            self._route_after_classification,
            {
                "information": "information",
                "analytics": "analytics",
                "clarification": "clarification",
                "unsupported": "unsupported",
            },
        )
        graph.add_edge("information", END)
        graph.add_edge("analytics", END)
        graph.add_edge("clarification", END)
        graph.add_edge("unsupported", END)
        self._graph = graph.compile()

    def invoke(
        self,
        message: str,
        intent: str | None = None,
        selected_metric_id: str | None = None,
        selected_dimension_ids: list[str] | None = None,
        awaiting_clarification: bool = False,
        user_role: str = "business_user",
        technical_mode: bool = False,
        conversation_id: str | None = None,
        context: dict[str, Any] | None = None,
        governed_query_plan: dict[str, Any] | None = None,
        limit: int = 100,
        execute_sql: bool = True,
    ) -> dict[str, Any]:
        initial_state: OrchestrationState = {
            "message": message,
            "intent": intent,
            "selected_metric_id": selected_metric_id,
            "selected_dimension_ids": selected_dimension_ids or [],
            "awaiting_clarification": awaiting_clarification,
            "user_role": user_role,
            "technical_mode": technical_mode,
            "conversation_id": conversation_id,
            "context": context,
            "governed_query_plan": governed_query_plan,
            "limit": limit,
            "execute_sql": execute_sql,
            "graph_trace": [],
        }
        final_state = self._graph.invoke(initial_state)
        return self._response(final_state)

    def _classify(self, state: OrchestrationState) -> dict[str, Any]:
        result = intent_classifier.classify(
            message=state["message"],
            awaiting_clarification=state.get("awaiting_clarification", False),
            context=state.get("context"),
        ).to_dict()

        route = result["route"]
        if state.get("selected_metric_id") or state.get("selected_dimension_ids") or state.get("governed_query_plan"):
            route = "governed_analytics_flow"
        if state.get("intent") in {IntentType.ANALYTICAL_QUERY, IntentType.CHART_QUERY}:
            route = "governed_analytics_flow"

        return {
            "intent_result": result,
            "route": route,
            "graph_trace": state.get("graph_trace", []) + [f"classify:{result['intent']}"],
        }

    def _route_after_classification(self, state: OrchestrationState) -> str:
        route = state["route"]
        if route == "information_flow":
            return "information"
        if route == "governed_analytics_flow":
            return "analytics"
        if route == "clarification_flow":
            return "clarification"
        return "unsupported"

    def _information(self, state: OrchestrationState) -> dict[str, Any]:
        intent_result = state["intent_result"]
        context_items = intent_result.get("retrieval_context") or []
        answer = self._metadata_answer(intent=intent_result["intent"], context_items=context_items)
        status = OrchestrationStatus.ANSWERED if context_items else OrchestrationStatus.UNSUPPORTED
        next_action = (
            "answer_from_governed_metadata"
            if context_items
            else "ask_user_for_governed_commercial_banking_context"
        )
        return {
            "status": status,
            "answer": answer,
            "next_action": next_action,
            "requires_clarification": False,
            "clarification_options": [],
            "sql_result": None,
            "semantic_result": None,
            "metadata_context": self._compact_metadata_context(context_items),
            "assumptions": [],
            "warnings": [],
            "graph_trace": state.get("graph_trace", []) + ["information"],
        }

    def _analytics(self, state: OrchestrationState) -> dict[str, Any]:
        if state.get("execute_sql", True):
            return self._execute_analytics(state)

        sql_result = governed_sql_service.generate_from_message(
            message=state["message"],
            intent=state.get("intent"),
            selected_metric_id=state.get("selected_metric_id"),
            selected_dimension_ids=state.get("selected_dimension_ids") or [],
            awaiting_clarification=state.get("awaiting_clarification", False),
            user_role=state.get("user_role", "business_user"),
            technical_mode=state.get("technical_mode", False),
            context=state.get("context"),
            governed_query_plan=state.get("governed_query_plan"),
            limit=state.get("limit", 100),
        ).to_dict()
        semantic_result = sql_result["semantic_result"]

        if sql_result["status"] == SqlServiceStatus.GENERATED:
            status = OrchestrationStatus.SQL_GENERATED
            answer = sql_result["sql_summary"] or "Governed SQL was generated from certified metadata."
            next_action = "execute_validated_sql_in_next_phase"
            clarification_options: list[dict[str, Any]] = []
        elif sql_result["status"] == SqlServiceStatus.NEEDS_CLARIFICATION:
            status = OrchestrationStatus.NEEDS_CLARIFICATION
            answer = "I need one governed choice before I can generate SQL."
            next_action = "ask_user_to_select_governed_metric_or_dimension"
            clarification_options = semantic_result.get("ambiguities") or []
        elif sql_result["status"] == SqlServiceStatus.INFORMATION_ONLY:
            status = OrchestrationStatus.ANSWERED
            answer = self._metadata_answer(
                intent=semantic_result["intent"],
                context_items=semantic_result.get("retrieval_context") or [],
            )
            next_action = "answer_from_governed_metadata"
            clarification_options = []
        elif sql_result["status"] == SqlServiceStatus.UNSUPPORTED:
            status = OrchestrationStatus.UNSUPPORTED
            answer = "I can help with governed commercial banking metadata and analytics questions."
            next_action = "ask_user_for_supported_commercial_banking_request"
            clarification_options = []
        else:
            status = OrchestrationStatus.INVALID
            answer = "I could not produce valid governed SQL for this request."
            next_action = "review_validation_errors"
            clarification_options = []

        return {
            "status": status,
            "answer": answer,
            "next_action": next_action,
            "requires_clarification": bool(clarification_options),
            "clarification_options": clarification_options,
            "sql_result": sql_result,
            "execution_result": None,
            "semantic_result": semantic_result,
            "metadata_context": self._compact_metadata_context(semantic_result.get("retrieval_context") or []),
            "assumptions": sql_result.get("assumptions") or [],
            "warnings": sql_result.get("warnings") or [],
            "graph_trace": state.get("graph_trace", []) + ["analytics"],
        }

    def _execute_analytics(self, state: OrchestrationState) -> dict[str, Any]:
        execution_result = governed_query_executor.execute_from_message(
            message=state["message"],
            intent=state.get("intent"),
            selected_metric_id=state.get("selected_metric_id"),
            selected_dimension_ids=state.get("selected_dimension_ids") or [],
            awaiting_clarification=state.get("awaiting_clarification", False),
            user_role=state.get("user_role", "business_user"),
            technical_mode=state.get("technical_mode", False),
            context=state.get("context"),
            governed_query_plan=state.get("governed_query_plan"),
            limit=state.get("limit", 100),
        ).to_dict()
        sql_result = execution_result["sql_result"]
        semantic_result = sql_result["semantic_result"]

        if execution_result["status"] == QueryExecutionStatus.EXECUTED:
            answer_result = governed_answer_generator.answer_from_execution(
                message=state["message"],
                execution_result=execution_result,
            ).to_dict()
            chart_result = self._maybe_chart_result(state=state, answer_result=answer_result)
            status = OrchestrationStatus.ANSWERED
            answer = answer_result["answer"]
            next_action = "show_answer_chart_and_table" if chart_result else "show_answer_and_table"
            clarification_options: list[dict[str, Any]] = []
        elif execution_result["status"] == QueryExecutionStatus.NEEDS_CLARIFICATION:
            answer_result = None
            chart_result = None
            status = OrchestrationStatus.NEEDS_CLARIFICATION
            answer = "I need one governed choice before I can run the query."
            next_action = "ask_user_to_select_governed_metric_or_dimension"
            clarification_options = semantic_result.get("ambiguities") or []
        elif execution_result["status"] == QueryExecutionStatus.INFORMATION_ONLY:
            answer_result = None
            chart_result = None
            status = OrchestrationStatus.ANSWERED
            answer = self._metadata_answer(
                intent=semantic_result["intent"],
                context_items=semantic_result.get("retrieval_context") or [],
            )
            next_action = "answer_from_governed_metadata"
            clarification_options = []
        elif execution_result["status"] == QueryExecutionStatus.UNSUPPORTED:
            answer_result = None
            chart_result = None
            status = OrchestrationStatus.UNSUPPORTED
            answer = "I can help with governed commercial banking metadata and analytics questions."
            next_action = "ask_user_for_supported_commercial_banking_request"
            clarification_options = []
        else:
            answer_result = None
            chart_result = None
            status = OrchestrationStatus.INVALID
            answer = "I could not execute a valid governed query for this request."
            next_action = "review_execution_errors"
            clarification_options = []

        if chart_result:
            trace_suffix = ["analytics", "execute", "answer", "chart"]
        elif answer_result:
            trace_suffix = ["analytics", "execute", "answer"]
        else:
            trace_suffix = ["analytics"]
        return {
            "status": status,
            "answer": answer,
            "next_action": next_action,
            "requires_clarification": bool(clarification_options),
            "clarification_options": clarification_options,
            "sql_result": sql_result,
            "execution_result": execution_result,
            "answer_result": answer_result,
            "chart_result": chart_result,
            "semantic_result": semantic_result,
            "metadata_context": self._compact_metadata_context(semantic_result.get("retrieval_context") or []),
            "assumptions": sql_result.get("assumptions") or [],
            "warnings": execution_result.get("warnings") or [],
            "graph_trace": state.get("graph_trace", []) + trace_suffix,
        }

    def _maybe_chart_result(
        self,
        state: OrchestrationState,
        answer_result: dict[str, Any],
    ) -> dict[str, Any] | None:
        intent_result = state.get("intent_result") or {}
        if intent_result.get("intent") != IntentType.CHART_QUERY:
            return None
        chart_type = (intent_result.get("extracted_entities") or {}).get("chart_type")
        result = governed_chart_generator.chart_from_answer_result(
            message=state["message"],
            answer_result=answer_result,
            chart_type=chart_type,
        ).to_dict()
        return result if result["chart_spec"] else None

    def _clarification(self, state: OrchestrationState) -> dict[str, Any]:
        return {
            "status": OrchestrationStatus.NEEDS_CLARIFICATION,
            "answer": "Please select one of the governed metric or dimension options from the prior clarification prompt.",
            "next_action": "await_governed_selection",
            "requires_clarification": True,
            "clarification_options": [],
            "sql_result": None,
            "execution_result": None,
            "semantic_result": None,
            "metadata_context": self._compact_metadata_context(state["intent_result"].get("retrieval_context") or []),
            "assumptions": [],
            "warnings": [],
            "graph_trace": state.get("graph_trace", []) + ["clarification"],
        }

    def _unsupported(self, state: OrchestrationState) -> dict[str, Any]:
        return {
            "status": OrchestrationStatus.UNSUPPORTED,
            "answer": "I can help with governed commercial banking metadata, lineage, definitions, and analytics questions.",
            "next_action": "ask_user_for_supported_commercial_banking_request",
            "requires_clarification": False,
            "clarification_options": [],
            "sql_result": None,
            "execution_result": None,
            "semantic_result": None,
            "metadata_context": self._compact_metadata_context(state.get("intent_result", {}).get("retrieval_context") or []),
            "assumptions": [],
            "warnings": [],
            "graph_trace": state.get("graph_trace", []) + ["unsupported"],
        }

    def _metadata_answer(self, intent: str, context_items: list[dict[str, Any]]) -> str:
        if not context_items:
            return "I could not find governed commercial banking metadata for that request."

        top = context_items[0]
        document_type = top["document_type"]
        source_id = top["source_id"]

        if document_type == "metric":
            metric = catalog_service.get_metric(source_id)
            if metric:
                return (
                    f"{metric['metric_name']} is a certified metric: {metric['description']} "
                    f"It is calculated as `{metric['calculation_sql']}` from `{metric['base_table']}`."
                )

        if document_type == "dimension":
            dimension = catalog_service.get_dimension(source_id)
            if dimension:
                return (
                    f"{dimension['dimension_name']} is a certified dimension: {dimension['description']} "
                    f"It maps to `{dimension['table_name']}.{dimension['column_name']}`."
                )

        if document_type == "table":
            table = catalog_service.get_table(source_id)
            if table:
                return (
                    f"{table['business_name']} is a certified {table['table_type'].lower()} table named "
                    f"`{table['table_name']}`. Grain: {table['grain']} Refresh: {table['refresh_frequency']}."
                )

        if document_type == "lineage":
            lineage = self._get_lineage(source_id)
            if lineage:
                return (
                    f"`{lineage['target_table']}.{lineage['target_column']}` comes from "
                    f"{lineage['source_system']} / {lineage['source_object']}. "
                    f"{lineage['transformation']}"
                )

        if intent == IntentType.TABLE_DISCOVERY_QUESTION:
            labels = [
                f"{item['business_name']} (`{item.get('table_name') or item['source_id']}`)"
                for item in context_items[:3]
            ]
            return "The most relevant governed assets are: " + "; ".join(labels) + "."

        return f"The most relevant governed metadata match is {top['business_name']}."

    def _get_lineage(self, lineage_id: str) -> dict[str, Any] | None:
        for lineage in catalog_service.list_lineage():
            if lineage["lineage_id"] == lineage_id:
                return lineage
        return None

    def _compact_metadata_context(self, context_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "document_type": item["document_type"],
                "source_id": item["source_id"],
                "business_name": item["business_name"],
                "table_name": item.get("table_name"),
                "column_name": item.get("column_name"),
                "score": item.get("score"),
            }
            for item in context_items[:6]
        ]

    def _response(self, state: OrchestrationState) -> dict[str, Any]:
        intent_result = state.get("intent_result") or {}
        sql_result = state.get("sql_result") or {}
        execution_result = state.get("execution_result") or {}
        answer_result = state.get("answer_result") or {}
        chart_result = state.get("chart_result") or {}
        return {
            "message": state["message"],
            "conversation_id": state.get("conversation_id"),
            "status": state.get("status", OrchestrationStatus.INVALID),
            "intent": intent_result.get("intent"),
            "route": state.get("route"),
            "answer": state.get("answer", ""),
            "next_action": state.get("next_action", "review_request"),
            "requires_clarification": state.get("requires_clarification", False),
            "clarification_options": state.get("clarification_options", []),
            "sql_visible": sql_result.get("sql_visible", False),
            "generated_sql": sql_result.get("generated_sql"),
            "sql_summary": sql_result.get("sql_summary"),
            "sql_validation": sql_result.get("validation"),
            "result_table": execution_result.get("result_table"),
            "execution_ms": execution_result.get("execution_ms"),
            "answer_summary": execution_result.get("answer_summary"),
            "key_points": answer_result.get("key_points", []),
            "result_overview": answer_result.get("result_overview", {}),
            "chart_spec": chart_result.get("chart_spec"),
            "semantic_result": state.get("semantic_result"),
            "metadata_context": state.get("metadata_context", []),
            "assumptions": state.get("assumptions", []),
            "warnings": state.get("warnings", []),
            "graph_trace": state.get("graph_trace", []),
        }


governed_assistant_graph = GovernedAssistantGraph()
