from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, StateGraph

from backend.app.catalog.service import catalog_service
from backend.app.answer.service import governed_answer_generator
from backend.app.chart.service import governed_chart_generator
from backend.app.conversation.service import conversation_store
from backend.app.execution.service import QueryExecutionStatus, governed_query_executor
from backend.app.llm.client import llm_client
from backend.app.orchestration.graph import OrchestrationStatus
from backend.app.retrieval.metadata import metadata_retriever
from backend.app.semantic.resolver import ResolutionStatus, semantic_resolver
from backend.app.sql.service import SqlServiceStatus, governed_sql_service


class LlmAssistantState(TypedDict, total=False):
    message: str
    conversation_id: Optional[str]
    selected_metric_id: Optional[str]
    selected_dimension_ids: List[str]
    user_role: str
    limit: int
    execute_sql: bool
    memory: Dict[str, Any]
    metadata_context: Dict[str, Any]
    llm_decision: Dict[str, Any]
    sql_result: Optional[Dict[str, Any]]
    execution_result: Optional[Dict[str, Any]]
    answer_result: Optional[Dict[str, Any]]
    chart_result: Optional[Dict[str, Any]]
    final_answer: str
    status: str
    next_action: str
    requires_clarification: bool
    clarification_options: List[Dict[str, Any]]
    source_citations: List[Dict[str, Any]]
    graph_trace: List[str]
    warnings: List[str]


class LlmGovernedAssistantGraph:
    """LLM-first metadata-grounded assistant with deterministic governance gates."""

    def __init__(self) -> None:
        graph = StateGraph(LlmAssistantState)
        graph.add_node("memory", self._memory)
        graph.add_node("retrieve", self._retrieve)
        graph.add_node("interpret", self._interpret)
        graph.add_node("respond", self._respond)
        graph.set_entry_point("memory")
        graph.add_edge("memory", "retrieve")
        graph.add_edge("retrieve", "interpret")
        graph.add_edge("interpret", "respond")
        graph.add_edge("respond", END)
        self._graph = graph.compile()

    def invoke(
        self,
        message: str,
        conversation_id: str | None = None,
        selected_metric_id: str | None = None,
        selected_dimension_ids: list[str] | None = None,
        user_role: str = "business_user",
        limit: int = 100,
        execute_sql: bool = True,
        **_: Any,
    ) -> dict[str, Any]:
        state: LlmAssistantState = {
            "message": message,
            "conversation_id": conversation_id,
            "selected_metric_id": selected_metric_id,
            "selected_dimension_ids": selected_dimension_ids or [],
            "user_role": user_role,
            "limit": limit,
            "execute_sql": execute_sql,
            "graph_trace": [],
            "warnings": [],
        }
        return self._response(self._graph.invoke(state))

    def _memory(self, state: LlmAssistantState) -> dict[str, Any]:
        memory: dict[str, Any] = {"previous_turns": [], "pending_clarification": None}
        conversation_id = state.get("conversation_id")
        if conversation_id:
            conversation = conversation_store.get_conversation(conversation_id)
            if conversation:
                turns = conversation.get("turns") or []
                memory["previous_turns"] = turns[-4:]
                if turns:
                    last_response = turns[-1].get("response") or {}
                    if last_response.get("requires_clarification"):
                        memory["pending_clarification"] = {
                            "message": last_response.get("message"),
                            "options": last_response.get("clarification_options") or [],
                        }
        return {"memory": memory, "graph_trace": state.get("graph_trace", []) + ["memory"]}

    def _retrieve(self, state: LlmAssistantState) -> dict[str, Any]:
        context = metadata_retriever.retrieve(state["message"], limit=12)
        return {
            "metadata_context": context,
            "source_citations": context["citations"],
            "graph_trace": state.get("graph_trace", []) + ["retrieve_metadata"],
        }

    def _interpret(self, state: LlmAssistantState) -> dict[str, Any]:
        decision = llm_client.invoke_json(
            task_name="intent_semantic_resolution",
            system_prompt=self._resolver_prompt(),
            input_payload={
                "user_message": state["message"],
                "conversation_memory": state.get("memory") or {},
                "metadata_context": state.get("metadata_context") or {},
                "selected_metric_id": state.get("selected_metric_id"),
                "selected_dimension_ids": state.get("selected_dimension_ids") or [],
            },
            fallback=lambda: self._local_resolution(state),
        )

        if decision.get("action") == "information":
            answer = self._information_answer(decision=decision, context=state.get("metadata_context") or {})
            return {
                "llm_decision": decision,
                "status": OrchestrationStatus.ANSWERED,
                "final_answer": answer,
                "next_action": "answer_from_governed_metadata",
                "requires_clarification": False,
                "clarification_options": [],
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "llm_answer"],
            }

        if decision.get("action") == "clarify":
            return {
                "llm_decision": decision,
                "status": OrchestrationStatus.NEEDS_CLARIFICATION,
                "final_answer": decision.get("clarification_question") or "Which governed definition should I use?",
                "next_action": "ask_follow_up_clarification",
                "requires_clarification": True,
                "clarification_options": decision.get("clarification_options") or [],
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "clarification"],
            }

        if decision.get("action") == "unsupported":
            return {
                "llm_decision": decision,
                "status": OrchestrationStatus.UNSUPPORTED,
                "final_answer": "I can help with governed commercial banking metadata, definitions, SQL-backed analytics, and charts.",
                "next_action": "ask_user_for_supported_commercial_banking_request",
                "requires_clarification": False,
                "clarification_options": [],
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "unsupported"],
            }

        return self._analytics(state=state, decision=decision)

    def _respond(self, state: LlmAssistantState) -> dict[str, Any]:
        return state

    def _analytics(self, state: LlmAssistantState, decision: dict[str, Any]) -> dict[str, Any]:
        selected_metric_id = decision.get("selected_metric_id") or state.get("selected_metric_id")
        selected_dimension_ids = decision.get("selected_dimension_ids") or state.get("selected_dimension_ids") or []

        sql_result = governed_sql_service.generate_from_message(
            message=state["message"],
            intent="chart_query" if decision.get("chart_requested") else "analytical_query",
            selected_metric_id=selected_metric_id,
            selected_dimension_ids=selected_dimension_ids,
            user_role="technical_user",
            technical_mode=True,
            limit=state.get("limit", 100),
        ).to_dict()

        if sql_result["status"] == SqlServiceStatus.NEEDS_CLARIFICATION:
            semantic_result = sql_result["semantic_result"]
            return {
                "llm_decision": decision,
                "sql_result": sql_result,
                "status": OrchestrationStatus.NEEDS_CLARIFICATION,
                "final_answer": self._clarification_question(semantic_result.get("ambiguities") or []),
                "next_action": "ask_follow_up_clarification",
                "requires_clarification": True,
                "clarification_options": semantic_result.get("ambiguities") or [],
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "sql_plan_blocked"],
            }

        if sql_result["status"] != SqlServiceStatus.GENERATED:
            return {
                "llm_decision": decision,
                "sql_result": sql_result,
                "status": OrchestrationStatus.INVALID,
                "final_answer": "I could not create valid governed SQL from the available metadata.",
                "next_action": "review_sql_validation",
                "requires_clarification": False,
                "clarification_options": [],
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "sql_invalid"],
            }

        if not state.get("execute_sql", True):
            return {
                "llm_decision": decision,
                "sql_result": sql_result,
                "status": OrchestrationStatus.SQL_GENERATED,
                "final_answer": "I generated governed SQL from the retrieved metadata and business definitions.",
                "next_action": "review_sql",
                "requires_clarification": False,
                "clarification_options": [],
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "llm_sql_generate"],
            }

        execution_result = governed_query_executor.execute_from_message(
            message=state["message"],
            intent="chart_query" if decision.get("chart_requested") else "analytical_query",
            selected_metric_id=selected_metric_id,
            selected_dimension_ids=selected_dimension_ids,
            user_role="technical_user",
            technical_mode=True,
            limit=state.get("limit", 100),
        ).to_dict()

        if execution_result["status"] != QueryExecutionStatus.EXECUTED:
            return {
                "llm_decision": decision,
                "sql_result": sql_result,
                "execution_result": execution_result,
                "status": OrchestrationStatus.INVALID,
                "final_answer": "The SQL was generated but could not be safely executed.",
                "next_action": "review_execution_errors",
                "requires_clarification": False,
                "clarification_options": [],
                "warnings": execution_result.get("warnings") or [],
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "llm_sql_generate", "execute_failed"],
            }

        answer_result = governed_answer_generator.answer_from_execution(
            message=state["message"],
            execution_result=execution_result,
        ).to_dict()
        answer_payload = llm_client.invoke_json(
            task_name="answer_generation",
            system_prompt=self._answer_prompt(),
            input_payload={
                "user_message": state["message"],
                "result_table": execution_result.get("result_table"),
                "sql": sql_result.get("generated_sql"),
                "governed_query_plan": sql_result.get("governed_query_plan"),
                "citations": state.get("source_citations") or [],
            },
            fallback=lambda: {
                "answer": answer_result["answer"],
                "key_points": answer_result.get("key_points") or [],
            },
        )
        answer_result["answer"] = answer_payload.get("answer") or answer_result["answer"]
        answer_result["key_points"] = answer_payload.get("key_points") or answer_result.get("key_points") or []

        chart_result = None
        if decision.get("chart_requested"):
            chart_result = governed_chart_generator.chart_from_answer_result(
                message=state["message"],
                answer_result=answer_result,
                chart_type=decision.get("chart_type"),
            ).to_dict()

        return {
            "llm_decision": decision,
            "sql_result": sql_result,
            "execution_result": execution_result,
            "answer_result": answer_result,
            "chart_result": chart_result,
            "status": OrchestrationStatus.ANSWERED,
            "final_answer": answer_result["answer"],
            "next_action": "show_answer_sql_sources_and_chart" if chart_result else "show_answer_sql_and_sources",
            "requires_clarification": False,
            "clarification_options": [],
            "warnings": execution_result.get("warnings") or [],
            "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "llm_sql_generate", "validate", "execute", "llm_answer"],
        }

    def _local_resolution(self, state: LlmAssistantState) -> dict[str, Any]:
        selected_metric_id = state.get("selected_metric_id")
        selected_dimension_ids = state.get("selected_dimension_ids") or []
        pending = (state.get("memory") or {}).get("pending_clarification")

        if pending and not selected_metric_id:
            selected_metric_id, selected_dimension_ids = self._resolve_pending_choice(state["message"], pending)

        semantic_result = semantic_resolver.resolve(
            message=(pending or {}).get("message") or state["message"],
            intent=None,
            selected_metric_id=selected_metric_id,
            selected_dimension_ids=selected_dimension_ids,
        ).to_dict()

        if semantic_result["status"] == ResolutionStatus.INFORMATION_ONLY:
            return {
                "action": "information",
                "intent": semantic_result["intent"],
                "selected_metric_id": None,
                "selected_dimension_ids": [],
                "chart_requested": False,
                "citations": (state.get("metadata_context") or {}).get("citations") or [],
            }

        if semantic_result["status"] == ResolutionStatus.UNSUPPORTED:
            return {"action": "unsupported", "intent": "unsupported"}

        if semantic_result["status"] == ResolutionStatus.NEEDS_CLARIFICATION:
            ambiguities = semantic_result.get("ambiguities") or []
            return {
                "action": "clarify",
                "intent": semantic_result["intent"],
                "clarification_question": self._clarification_question(ambiguities),
                "clarification_options": ambiguities,
                "chart_requested": semantic_result["intent"] == "chart_query",
                "citations": (state.get("metadata_context") or {}).get("citations") or [],
            }

        plan = semantic_result["governed_query_plan"] or {}
        return {
            "action": "analytics",
            "intent": semantic_result["intent"],
            "selected_metric_id": (plan.get("metric") or {}).get("id"),
            "selected_dimension_ids": [dimension["id"] for dimension in plan.get("dimensions") or []],
            "chart_requested": semantic_result["intent"] == "chart_query" or "plot" in state["message"].lower(),
            "chart_type": "line" if "month" in state["message"].lower() else None,
            "citations": (state.get("metadata_context") or {}).get("citations") or [],
        }

    def _resolve_pending_choice(self, message: str, pending: dict[str, Any]) -> tuple[str | None, list[str]]:
        normalized = message.lower()
        metric_id = None
        dimension_ids: list[str] = []
        for ambiguity in pending.get("options") or []:
            best_option_id = None
            best_score = 0
            for option in ambiguity.get("options") or []:
                option_id = option.get("id")
                if not option_id:
                    continue
                label = (option.get("label") or "").lower()
                option_words = self._meaningful_words(f"{label} {option_id}")
                score = sum(1 for word in option_words if word in normalized)
                if label and label in normalized:
                    score += 5
                if option_id.lower() in normalized:
                    score += 5
                if score > best_score:
                    best_score = score
                    best_option_id = option_id
            if best_option_id and best_score > 0:
                if ambiguity.get("kind") == "metric":
                    metric_id = best_option_id
                elif ambiguity.get("kind") == "dimension":
                    dimension_ids.append(best_option_id)
        return metric_id, dimension_ids

    def _meaningful_words(self, text: str) -> set[str]:
        ignored = {"average", "balance", "metric", "dimension", "by", "the", "and", "of"}
        return {word for word in text.replace("_", " ").replace(".", " ").split() if len(word) > 2 and word not in ignored}

    def _clarification_question(self, ambiguities: list[dict[str, Any]]) -> str:
        lines = ["I found more than one governed meaning. Please clarify:"]
        for ambiguity in ambiguities:
            lines.append(f"\n{ambiguity.get('question')}")
            for index, option in enumerate(ambiguity.get("options") or [], start=1):
                table = option.get("table")
                column = option.get("column")
                location = f" ({table}.{column})" if table and column else ""
                lines.append(f"{index}. {option.get('label')}{location}")
        return "\n".join(lines)

    def _information_answer(self, decision: dict[str, Any], context: dict[str, Any]) -> str:
        if decision.get("answer"):
            return decision["answer"]
        top = (context.get("search_results") or [{}])[0]
        if not top:
            return "I could not find governed metadata for that question."
        if top.get("document_type") == "metric":
            metric = catalog_service.get_metric(top["source_id"])
            if metric:
                return (
                    f"{metric['metric_name']} is a certified business metric. {metric['description']} "
                    f"Certified calculation: `{metric['calculation_sql']}`. Source table: `{metric['base_table']}`."
                )
        if top.get("document_type") == "dimension":
            dimension = catalog_service.get_dimension(top["source_id"])
            if dimension:
                return (
                    f"{dimension['dimension_name']} is a certified business attribute. {dimension['description']} "
                    f"It maps to `{dimension['table_name']}.{dimension['column_name']}`."
                )
        if top.get("document_type") == "business_term":
            terms = catalog_service.list_business_terms(query=top.get("business_name"))
            if terms:
                term = terms[0]
                mapping = ""
                if term.get("primary_table") and term.get("primary_column"):
                    mapping = f" It maps to `{term['primary_table']}.{term['primary_column']}`."
                return f"{term['term_name']}: {term['definition']}{mapping}"
        if top.get("document_type") == "column":
            table_name = top.get("table_name")
            column_name = top.get("column_name")
            for column in catalog_service.list_columns(table_name=table_name):
                if column["column_name"] == column_name:
                    return (
                        f"{column['business_name']} is a governed business attribute. {column['description']} "
                        f"Physical column: `{table_name}.{column_name}`. Data type: {column['data_type']}."
                    )
        return f"{top.get('business_name')} is documented in the governed metadata catalog. Source: {top.get('source_id')}."

    def _resolver_prompt(self) -> str:
        return (
            "You are a governed commercial banking analytics assistant using AWS Bedrock Titan. "
            "Choose intent and semantic assets only from the provided metadata context. "
            "If table, column, metric, or dimension choice is unclear, ask a follow-up clarification. "
            "Never invent table names or column names. Return JSON with action, intent, selected_metric_id, "
            "selected_dimension_ids, chart_requested, chart_type, clarification_question, clarification_options, and citations."
        )

    def _answer_prompt(self) -> str:
        return (
            "Generate a concise business answer from the SQL result table. Include only facts supported by rows, "
            "and produce JSON with answer and key_points. Do not invent sources."
        )

    def _response(self, state: LlmAssistantState) -> dict[str, Any]:
        sql_result = state.get("sql_result") or {}
        execution_result = state.get("execution_result") or {}
        answer_result = state.get("answer_result") or {}
        chart_result = state.get("chart_result") or {}
        decision = state.get("llm_decision") or {}
        return {
            "message": state["message"],
            "conversation_id": state.get("conversation_id"),
            "status": state.get("status", OrchestrationStatus.INVALID),
            "intent": decision.get("intent"),
            "route": "llm_governed_chat_flow",
            "answer": state.get("final_answer", ""),
            "next_action": state.get("next_action", "review_request"),
            "requires_clarification": state.get("requires_clarification", False),
            "clarification_options": state.get("clarification_options", []),
            "sql_visible": bool(sql_result.get("generated_sql")),
            "generated_sql": sql_result.get("generated_sql"),
            "sql_summary": sql_result.get("sql_summary"),
            "sql_validation": sql_result.get("validation"),
            "result_table": execution_result.get("result_table"),
            "execution_ms": execution_result.get("execution_ms"),
            "answer_summary": execution_result.get("answer_summary"),
            "key_points": answer_result.get("key_points", []),
            "result_overview": answer_result.get("result_overview", {}),
            "chart_spec": (chart_result or {}).get("chart_spec"),
            "semantic_result": sql_result.get("semantic_result"),
            "metadata_context": (state.get("metadata_context") or {}).get("search_results", [])[:6],
            "source_citations": state.get("source_citations") or decision.get("citations") or [],
            "llm_trace": {
                "provider": decision.get("llm_provider"),
                "task": decision.get("llm_task"),
                "error": decision.get("llm_error"),
            },
            "assumptions": sql_result.get("assumptions") or [],
            "warnings": state.get("warnings") or [],
            "graph_trace": state.get("graph_trace", []),
        }


llm_governed_assistant_graph = LlmGovernedAssistantGraph()
