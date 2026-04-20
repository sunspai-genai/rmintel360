from __future__ import annotations

import re
from collections import deque
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, StateGraph

from backend.app.catalog.service import catalog_service
from backend.app.answer.service import governed_answer_generator
from backend.app.cache.service import assistant_response_cache
from backend.app.chart.service import governed_chart_generator
from backend.app.conversation.service import conversation_store
from backend.app.execution.service import QueryExecutionStatus, governed_query_executor
from backend.app.llm.client import llm_client
from backend.app.orchestration.graph import OrchestrationStatus
from backend.app.retrieval.metadata import metadata_retriever
from backend.app.semantic.resolver import ResolutionStatus, semantic_resolver
from backend.app.sql.service import SqlServiceStatus, governed_sql_service


INFORMATION_INTENTS = {
    "definition_question",
    "metadata_question",
    "lineage_question",
    "table_discovery_question",
}
ANALYTICAL_INTENTS = {"analytical_query", "chart_query"}
VALID_INTENTS = INFORMATION_INTENTS | ANALYTICAL_INTENTS | {"clarification_response", "unsupported"}
RESPONSE_MODE_BY_INTENT = {
    "definition_question": "definition_only",
    "metadata_question": "metadata_only",
    "lineage_question": "lineage_only",
    "table_discovery_question": "metadata_only",
    "analytical_query": "sql_answer",
    "chart_query": "chart_answer",
    "clarification_response": "clarification",
    "unsupported": "unsupported",
}
SQL_ALLOWED_INTENTS = {"analytical_query", "chart_query"}
CHART_ALLOWED_INTENTS = {"chart_query"}


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
    response_mode: str
    pending_task: Dict[str, Any]
    cached_response: Dict[str, Any]
    resolved_cache_key: str
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
        response = self._response(self._graph.invoke(state))
        resolved_cache_key = response.pop("_resolved_cache_key", None)
        if resolved_cache_key and self._is_cacheable_final_response(response):
            assistant_response_cache.set(resolved_cache_key, response)
            response.setdefault("llm_trace", {})["cache_status"] = "stored"
        return response

    def _memory(self, state: LlmAssistantState) -> dict[str, Any]:
        memory: dict[str, Any] = {"previous_turns": [], "pending_clarification": None, "pending_task": None}
        conversation_id = state.get("conversation_id")
        if conversation_id:
            conversation = conversation_store.get_conversation(conversation_id)
            if conversation:
                turns = conversation.get("turns") or []
                memory["previous_turns"] = turns[-4:]
                if turns:
                    last_response = turns[-1].get("response") or {}
                    if last_response.get("requires_clarification"):
                        pending_task = last_response.get("pending_task") or self._pending_task_from_response(last_response)
                        memory["pending_task"] = pending_task
                        memory["pending_clarification"] = {
                            "message": (pending_task or {}).get("original_message") or last_response.get("message"),
                            "options": (pending_task or {}).get("clarification_options")
                            or last_response.get("clarification_options")
                            or [],
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
        raw_decision = self._pending_choice_resolution(state)
        if raw_decision is None:
            raw_decision = llm_client.invoke_json(
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
        decision = self._normalize_decision(raw_decision)
        policy = self._policy_for_decision(decision)
        decision.update(policy)

        if policy["requires_clarification"]:
            return {
                "llm_decision": decision,
                "status": OrchestrationStatus.NEEDS_CLARIFICATION,
                "final_answer": decision.get("clarification_question")
                or "Should I answer this from metadata, calculate it from data, or create a chart?",
                "next_action": "ask_follow_up_clarification",
                "requires_clarification": True,
                "clarification_options": decision.get("clarification_options") or [],
                "pending_task": decision.get("pending_task") or {},
                "response_mode": "clarification",
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "intent_policy_clarification"],
            }

        if decision.get("action") == "information":
            answer = self._information_answer(
                message=state["message"],
                decision=decision,
                context=state.get("metadata_context") or {},
            )
            return {
                "llm_decision": decision,
                "status": OrchestrationStatus.ANSWERED,
                "final_answer": answer,
                "next_action": "answer_from_governed_metadata",
                "requires_clarification": False,
                "clarification_options": [],
                "response_mode": decision["response_mode"],
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "intent_policy_information", "llm_answer"],
            }

        if decision.get("action") == "clarify":
            return {
                "llm_decision": decision,
                "status": OrchestrationStatus.NEEDS_CLARIFICATION,
                "final_answer": decision.get("clarification_question") or "Which governed definition should I use?",
                "next_action": "ask_follow_up_clarification",
                "requires_clarification": True,
                "clarification_options": decision.get("clarification_options") or [],
                "pending_task": decision.get("pending_task") or {},
                "response_mode": "clarification",
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
                "response_mode": "unsupported",
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "unsupported"],
            }

        if not decision["allow_sql"]:
            return {
                "llm_decision": decision,
                "status": OrchestrationStatus.NEEDS_CLARIFICATION,
                "final_answer": "Do you want the business definition, a SQL-backed calculation, or a chart?",
                "next_action": "clarify_response_mode_before_sql",
                "requires_clarification": True,
                "clarification_options": self._response_mode_options(),
                "pending_task": decision.get("pending_task") or {},
                "response_mode": "clarification",
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "intent_policy_blocked_sql"],
            }

        return self._analytics(state=state, decision=decision)

    def _respond(self, state: LlmAssistantState) -> dict[str, Any]:
        return state

    def _analytics(self, state: LlmAssistantState, decision: dict[str, Any]) -> dict[str, Any]:
        preflight = self._preflight_clarification(state=state, decision=decision)
        if preflight:
            return preflight

        analytics_message = decision.get("analytics_message") or state["message"]
        selected_metric_id = decision.get("selected_metric_id") or state.get("selected_metric_id")
        selected_dimension_ids = self._selected_dimension_ids(state=state, decision=decision)

        sql_result = governed_sql_service.generate_from_message(
            message=analytics_message,
            intent="chart_query" if decision.get("chart_requested") else "analytical_query",
            selected_metric_id=selected_metric_id,
            selected_dimension_ids=selected_dimension_ids,
            user_role="technical_user",
            technical_mode=True,
            limit=state.get("limit", 100),
        ).to_dict()

        if sql_result["status"] == SqlServiceStatus.NEEDS_CLARIFICATION:
            semantic_result = sql_result["semantic_result"]
            pending_task = self._pending_task_from_semantic(
                semantic_result=semantic_result,
                original_message=state["message"],
                chart_requested=bool(decision.get("chart_requested")),
                chart_type=decision.get("chart_type"),
            )
            return {
                "llm_decision": decision,
                "sql_result": sql_result,
                "status": OrchestrationStatus.NEEDS_CLARIFICATION,
                "final_answer": self._pending_task_clarification_question(pending_task),
                "next_action": "ask_follow_up_clarification",
                "requires_clarification": True,
                "clarification_options": semantic_result.get("ambiguities") or [],
                "pending_task": pending_task,
                "response_mode": "clarification",
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
                "response_mode": "sql_answer",
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "sql_invalid"],
            }

        resolved_cache_key = self._resolved_cache_key(
            state=state,
            decision=decision,
            sql_result=sql_result,
            analytics_message=analytics_message,
        )
        if resolved_cache_key and state.get("execute_sql", True):
            cached = assistant_response_cache.get(resolved_cache_key)
            if cached:
                cached["message"] = state["message"]
                cached["conversation_id"] = state.get("conversation_id")
                cached["graph_trace"] = state.get("graph_trace", []) + [
                    "llm_interpret",
                    "llm_semantic_plan",
                    "llm_sql_generate",
                    "resolved_cache_hit",
                ]
                llm_trace = dict(cached.get("llm_trace") or {})
                llm_trace["cache_status"] = "hit"
                llm_trace["cache_backend"] = (cached.get("cache") or {}).get("backend")
                cached["llm_trace"] = llm_trace
                cached.setdefault("pending_task", {})
                return {"cached_response": cached}

        if not state.get("execute_sql", True):
            return {
                "llm_decision": decision,
                "sql_result": sql_result,
                "status": OrchestrationStatus.SQL_GENERATED,
                "final_answer": "I generated governed SQL from the retrieved metadata and business definitions.",
                "next_action": "review_sql",
                "requires_clarification": False,
                "clarification_options": [],
                "response_mode": decision["response_mode"],
                "resolved_cache_key": resolved_cache_key,
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "llm_sql_generate"],
            }

        execution_result = governed_query_executor.execute_sql_result(
            message=analytics_message,
            sql_result=sql_result,
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
                "response_mode": decision["response_mode"],
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "llm_sql_generate", "execute_failed"],
            }

        answer_result = governed_answer_generator.answer_from_execution(
            message=analytics_message,
            execution_result=execution_result,
        ).to_dict()

        chart_result = None
        if decision.get("chart_requested") and decision.get("allow_chart"):
            chart_result = governed_chart_generator.chart_from_answer_result(
                message=analytics_message,
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
            "response_mode": "chart_answer" if chart_result else "sql_answer",
            "resolved_cache_key": resolved_cache_key,
            "graph_trace": state.get("graph_trace", [])
            + ["llm_interpret", "llm_semantic_plan", "llm_sql_generate", "validate", "execute", "llm_answer"]
            + (["llm_chart_plan", "python_plotly_build"] if chart_result else []),
        }

    def _preflight_clarification(self, state: LlmAssistantState, decision: dict[str, Any]) -> dict[str, Any] | None:
        if state.get("selected_metric_id") or state.get("selected_dimension_ids"):
            return None
        if (state.get("memory") or {}).get("pending_clarification"):
            return None

        semantic_result = semantic_resolver.resolve(
            message=state["message"],
            intent="chart_query" if decision.get("chart_requested") else "analytical_query",
        ).to_dict()
        ambiguities = semantic_result.get("ambiguities") or []
        if semantic_result["status"] != ResolutionStatus.NEEDS_CLARIFICATION or not ambiguities:
            return None
        pending_task = self._pending_task_from_semantic(
            semantic_result=semantic_result,
            original_message=state["message"],
            chart_requested=bool(decision.get("chart_requested")),
            chart_type=decision.get("chart_type"),
        )

        return {
            "llm_decision": decision,
            "sql_result": {
                "semantic_result": semantic_result,
                "generated_sql": None,
                "validation": None,
                "assumptions": semantic_result.get("assumptions") or [],
            },
            "status": OrchestrationStatus.NEEDS_CLARIFICATION,
            "final_answer": self._pending_task_clarification_question(pending_task),
            "next_action": "ask_follow_up_clarification",
            "requires_clarification": True,
            "clarification_options": ambiguities,
            "pending_task": pending_task,
            "response_mode": "clarification",
            "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "preflight_clarification"],
        }

    def _cache_key(
        self,
        *,
        message: str,
        conversation_id: str | None,
        selected_metric_id: str | None,
        selected_dimension_ids: list[str],
        user_role: str,
        limit: int,
        execute_sql: bool,
    ) -> str | None:
        if not assistant_response_cache.is_cacheable_message(message):
            return None
        if self._conversation_has_pending_clarification(conversation_id):
            return None
        return assistant_response_cache.build_key(
            message=message,
            conversation_id=conversation_id,
            user_role=user_role,
            limit=limit,
            execute_sql=execute_sql,
            selected_metric_id=selected_metric_id,
            selected_dimension_ids=selected_dimension_ids,
        )

    def _conversation_has_pending_clarification(self, conversation_id: str | None) -> bool:
        if not conversation_id:
            return False
        conversation = conversation_store.get_conversation(conversation_id)
        if not conversation:
            return False
        turns = conversation.get("turns") or []
        if not turns:
            return False
        last_response = turns[-1].get("response") or {}
        return bool(last_response.get("requires_clarification"))

    def _resolved_cache_key(
        self,
        *,
        state: LlmAssistantState,
        decision: dict[str, Any],
        sql_result: dict[str, Any],
        analytics_message: str,
    ) -> str | None:
        if not assistant_response_cache.is_cacheable_message(analytics_message):
            return None
        semantic_result = sql_result.get("semantic_result") or {}
        if semantic_result.get("status") != ResolutionStatus.RESOLVED:
            return None
        plan = semantic_result.get("governed_query_plan") or {}
        metric = plan.get("metric") or {}
        dimensions = plan.get("dimensions") or []
        return assistant_response_cache.build_resolved_plan_key(
            message=analytics_message,
            conversation_id=state.get("conversation_id"),
            user_role=state.get("user_role", "business_user"),
            limit=state.get("limit", 100),
            execute_sql=state.get("execute_sql", True),
            intent=semantic_result.get("intent") or decision.get("intent"),
            response_mode=decision.get("response_mode"),
            metric_id=metric.get("id"),
            dimension_ids=[dimension["id"] for dimension in dimensions if dimension.get("id")],
            filters=plan.get("filters") or [],
            chart_requested=bool(decision.get("chart_requested")),
            chart_type=decision.get("chart_type"),
            generated_sql=sql_result.get("generated_sql"),
        )

    def _is_cacheable_final_response(self, response: dict[str, Any]) -> bool:
        return (
            response.get("status") in {OrchestrationStatus.ANSWERED, OrchestrationStatus.SQL_GENERATED}
            and not response.get("requires_clarification")
            and bool(response.get("generated_sql"))
        )

    def _selected_dimension_ids(self, state: LlmAssistantState, decision: dict[str, Any]) -> list[str]:
        user_selected = state.get("selected_dimension_ids") or []
        if user_selected:
            return user_selected

        exact_metadata_dimensions = self._unambiguous_metadata_dimension_ids(state)
        if exact_metadata_dimensions:
            return exact_metadata_dimensions

        return [dimension_id for dimension_id in decision.get("selected_dimension_ids") or [] if isinstance(dimension_id, str)]

    def _unambiguous_metadata_dimension_ids(self, state: LlmAssistantState) -> list[str]:
        dimension_ids: list[str] = []
        chosen_phrases: list[str] = []
        for group in (state.get("metadata_context") or {}).get("candidate_groups") or []:
            if group.get("target_type") != "dimension":
                continue
            candidates = group.get("candidates") or []
            if len(candidates) != 1:
                continue
            phrase = str(group.get("phrase") or "").lower()
            if phrase and any(phrase in chosen_phrase and phrase != chosen_phrase for chosen_phrase in chosen_phrases):
                continue
            dimension_id = candidates[0].get("target_id")
            if dimension_id and dimension_id not in dimension_ids:
                dimension_ids.append(dimension_id)
                if phrase:
                    chosen_phrases.append(phrase)
        return dimension_ids

    def _normalize_decision(self, decision: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(decision or {})
        intent = normalized.get("intent")
        if intent not in VALID_INTENTS:
            intent = "clarification_response"
        normalized["intent"] = intent

        response_mode = normalized.get("response_mode") or RESPONSE_MODE_BY_INTENT[intent]
        if intent in INFORMATION_INTENTS or intent in {"chart_query", "unsupported", "clarification_response"}:
            response_mode = RESPONSE_MODE_BY_INTENT[intent]
        if response_mode not in {
            "definition_only",
            "metadata_only",
            "lineage_only",
            "sql_answer",
            "chart_answer",
            "clarification",
            "unsupported",
        }:
            response_mode = RESPONSE_MODE_BY_INTENT[intent]
        normalized["response_mode"] = response_mode

        if intent in INFORMATION_INTENTS:
            normalized["action"] = "information"
        elif intent == "unsupported":
            normalized["action"] = "unsupported"
        elif response_mode == "clarification" or normalized.get("action") == "clarify":
            normalized["action"] = "clarify"
        else:
            normalized["action"] = "analytics"

        normalized["allow_sql"] = bool(normalized.get("allow_sql")) and intent in SQL_ALLOWED_INTENTS
        normalized["allow_chart"] = bool(normalized.get("allow_chart")) and intent in CHART_ALLOWED_INTENTS
        if intent == "analytical_query":
            normalized["chart_requested"] = False
            normalized["allow_chart"] = False
        if intent == "chart_query":
            normalized["chart_requested"] = True
            normalized["allow_sql"] = True
            normalized["allow_chart"] = True
            normalized["response_mode"] = "chart_answer"
        return normalized

    def _policy_for_decision(self, decision: dict[str, Any]) -> dict[str, Any]:
        intent = decision["intent"]
        confidence = float(decision.get("confidence") or 0.0)
        inconsistent = False
        if intent in INFORMATION_INTENTS and (decision.get("allow_sql") or decision.get("allow_chart")):
            inconsistent = True
        if intent == "analytical_query" and decision.get("allow_chart"):
            inconsistent = True
        if intent == "chart_query" and not decision.get("allow_sql"):
            inconsistent = True

        requires_clarification = False
        if intent == "clarification_response":
            requires_clarification = True
        elif confidence and confidence < 0.60:
            requires_clarification = True
        elif inconsistent:
            requires_clarification = True

        return {
            "allow_sql": intent in SQL_ALLOWED_INTENTS and bool(decision.get("allow_sql")),
            "allow_chart": intent in CHART_ALLOWED_INTENTS and bool(decision.get("allow_chart")),
            "requires_clarification": requires_clarification,
            "policy_reason": self._policy_reason(intent=intent, confidence=confidence, inconsistent=inconsistent),
        }

    def _policy_reason(self, intent: str, confidence: float, inconsistent: bool) -> str:
        if inconsistent:
            return "The LLM decision mixed informational and analytical permissions, so SQL/chart generation was blocked."
        if confidence and confidence < 0.60:
            return "The LLM intent confidence was below the threshold for governed execution."
        if intent in INFORMATION_INTENTS:
            return "Information requests are answered from governed metadata only."
        if intent == "analytical_query":
            return "Analytical requests may generate SQL but not charts unless the user asks for a chart."
        if intent == "chart_query":
            return "Chart requests may generate SQL and a chart after validation."
        return "The request needs clarification or is outside supported scope."

    def _response_mode_options(self) -> list[dict[str, Any]]:
        return [
            {"id": "definition_only", "label": "Business definition only"},
            {"id": "sql_answer", "label": "Calculate from data"},
            {"id": "chart_answer", "label": "Plot a chart"},
        ]

    def _local_resolution(self, state: LlmAssistantState) -> dict[str, Any]:
        selected_metric_id = state.get("selected_metric_id")
        selected_dimension_ids = state.get("selected_dimension_ids") or []
        pending = (state.get("memory") or {}).get("pending_clarification")

        if pending and not selected_metric_id:
            pending_choice = self._resolve_pending_choice(state["message"], pending)
            selected_metric_id = pending_choice["metric_id"]
            selected_dimension_ids = pending_choice["dimension_ids"]

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
                "response_mode": RESPONSE_MODE_BY_INTENT.get(semantic_result["intent"], "definition_only"),
                "allow_sql": False,
                "allow_chart": False,
                "confidence": semantic_result.get("confidence", 0.90),
                "selected_metric_id": None,
                "selected_dimension_ids": [],
                "chart_requested": False,
                "citations": (state.get("metadata_context") or {}).get("citations") or [],
            }

        if semantic_result["status"] == ResolutionStatus.UNSUPPORTED:
            return {
                "action": "unsupported",
                "intent": "unsupported",
                "response_mode": "unsupported",
                "allow_sql": False,
                "allow_chart": False,
                "confidence": semantic_result.get("confidence", 0.90),
            }

        if semantic_result["status"] == ResolutionStatus.NEEDS_CLARIFICATION:
            ambiguities = semantic_result.get("ambiguities") or []
            pending_task = self._pending_task_from_semantic(
                semantic_result=semantic_result,
                original_message=(pending or {}).get("message") or state["message"],
                chart_requested=semantic_result["intent"] == "chart_query",
                chart_type=None,
            )
            return {
                "action": "clarify",
                "intent": semantic_result["intent"],
                "response_mode": "clarification",
                "allow_sql": False,
                "allow_chart": False,
                "confidence": semantic_result.get("confidence", 0.72),
                "clarification_question": self._pending_task_clarification_question(pending_task),
                "clarification_options": ambiguities,
                "pending_task": pending_task,
                "chart_requested": semantic_result["intent"] == "chart_query",
                "citations": (state.get("metadata_context") or {}).get("citations") or [],
            }

        plan = semantic_result["governed_query_plan"] or {}
        return {
            "action": "analytics",
            "intent": semantic_result["intent"],
            "response_mode": "chart_answer" if semantic_result["intent"] == "chart_query" else "sql_answer",
            "allow_sql": True,
            "allow_chart": semantic_result["intent"] == "chart_query",
            "confidence": semantic_result.get("confidence", 0.91),
            "selected_metric_id": (plan.get("metric") or {}).get("id"),
            "selected_dimension_ids": [dimension["id"] for dimension in plan.get("dimensions") or []],
            "chart_requested": semantic_result["intent"] == "chart_query" or "plot" in state["message"].lower(),
            "chart_type": "line" if "month" in state["message"].lower() else None,
            "citations": (state.get("metadata_context") or {}).get("citations") or [],
        }

    def _pending_choice_resolution(self, state: LlmAssistantState) -> dict[str, Any] | None:
        if state.get("selected_metric_id") or state.get("selected_dimension_ids"):
            return None
        pending_task = (state.get("memory") or {}).get("pending_task")
        if pending_task and not self._is_clear_topic_change(state["message"]):
            return self._resolve_pending_task_followup(state=state, pending_task=pending_task)
        pending = (state.get("memory") or {}).get("pending_clarification")
        if not pending:
            return None
        pending_choice = self._resolve_pending_choice(state["message"], pending)
        if not pending_choice["matched"]:
            return None

        return self._local_resolution(
            {
                **state,
                "selected_metric_id": pending_choice["metric_id"],
                "selected_dimension_ids": pending_choice["dimension_ids"],
            }
        )

    def _resolve_pending_task_followup(self, state: LlmAssistantState, pending_task: dict[str, Any]) -> dict[str, Any]:
        original_message = pending_task.get("original_message") or state["message"]
        intent = pending_task.get("intent") or "analytical_query"
        selected_metric_id = pending_task.get("resolved_metric_id")
        selected_dimension_ids = list(pending_task.get("resolved_dimension_ids") or [])

        selection = self._resolve_followup_selection(
            message=state["message"],
            clarification_options=pending_task.get("clarification_options") or [],
        )
        if selection.get("metric_id"):
            selected_metric_id = selection["metric_id"]
        selected_dimension_ids = self._merge_pending_dimension_selection(
            current_dimension_ids=selected_dimension_ids,
            selected_dimension_ids=selection.get("dimension_ids") or [],
            pending_task=pending_task,
        )

        semantic_result = semantic_resolver.resolve(
            message=original_message,
            intent=intent,
            selected_metric_id=selected_metric_id,
            selected_dimension_ids=selected_dimension_ids,
        ).to_dict()

        if semantic_result["status"] == ResolutionStatus.RESOLVED:
            plan = semantic_result["governed_query_plan"] or {}
            return {
                "action": "analytics",
                "intent": semantic_result["intent"],
                "response_mode": "chart_answer" if pending_task.get("chart_requested") else "sql_answer",
                "allow_sql": True,
                "allow_chart": bool(pending_task.get("chart_requested")),
                "confidence": semantic_result.get("confidence", 0.91),
                "selected_metric_id": (plan.get("metric") or {}).get("id"),
                "selected_dimension_ids": [dimension["id"] for dimension in plan.get("dimensions") or []],
                "analytics_message": original_message,
                "chart_requested": bool(pending_task.get("chart_requested")),
                "chart_type": pending_task.get("chart_type"),
                "citations": (state.get("metadata_context") or {}).get("citations") or [],
            }

        pending_task = self._pending_task_from_semantic(
            semantic_result=semantic_result,
            original_message=original_message,
            chart_requested=bool(pending_task.get("chart_requested")),
            chart_type=pending_task.get("chart_type"),
        )
        return {
            "action": "clarify",
            "intent": semantic_result["intent"],
            "response_mode": "clarification",
            "allow_sql": False,
            "allow_chart": False,
            "confidence": semantic_result.get("confidence", 0.72),
            "clarification_question": self._pending_task_clarification_question(
                pending_task,
                selection=selection,
            ),
            "clarification_options": semantic_result.get("ambiguities") or [],
            "pending_task": pending_task,
            "chart_requested": bool(pending_task.get("chart_requested")),
            "chart_type": pending_task.get("chart_type"),
            "citations": (state.get("metadata_context") or {}).get("citations") or [],
        }

    def _merge_pending_dimension_selection(
        self,
        *,
        current_dimension_ids: list[str],
        selected_dimension_ids: list[str],
        pending_task: dict[str, Any],
    ) -> list[str]:
        if not selected_dimension_ids:
            return current_dimension_ids

        has_open_dimension_choice = any(
            ambiguity.get("kind") == "dimension"
            for ambiguity in pending_task.get("clarification_options") or []
        )
        if not has_open_dimension_choice:
            return self._dedupe_strings(selected_dimension_ids)

        merged = list(current_dimension_ids)
        for dimension_id in selected_dimension_ids:
            if dimension_id not in merged:
                merged.append(dimension_id)
        return merged

    def _dedupe_strings(self, values: list[str]) -> list[str]:
        deduped = []
        seen = set()
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            deduped.append(value)
        return deduped

    def _resolve_followup_selection(
        self,
        message: str,
        clarification_options: list[dict[str, Any]],
    ) -> dict[str, Any]:
        pending_choice = self._resolve_pending_choice(message, {"options": clarification_options})
        metric_id = pending_choice["metric_id"]
        dimension_ids = list(pending_choice["dimension_ids"])

        if not metric_id:
            metric_id = self._catalog_selection_id(message=message, target_type="metric")
        dimension_id = self._catalog_selection_id(message=message, target_type="dimension")
        if dimension_id and dimension_id not in dimension_ids:
            dimension_ids.append(dimension_id)

        matched = bool(metric_id or dimension_ids or pending_choice["matched"])
        return {"metric_id": metric_id, "dimension_ids": dimension_ids, "matched": matched}

    def _catalog_selection_id(self, message: str, target_type: str) -> str | None:
        normalized = self._normalized_message(message)
        exact_candidates = catalog_service.search_governed_candidates(
            phrase=normalized,
            target_type=target_type,
            min_confidence=0.0,
            exact_match=True,
        )
        if len(exact_candidates) == 1:
            return exact_candidates[0]["target_id"]

        if target_type == "metric":
            for metric in catalog_service.list_metrics(certified=True):
                if self._normalized_message(metric["metric_name"]) == normalized or metric["metric_id"].lower() == message.lower().strip():
                    return metric["metric_id"]
            search_results = catalog_service.search_metadata(message, document_type="metric", limit=3, min_score=0.20)
            if search_results and search_results[0]["business_name"].lower() in message.lower():
                return search_results[0]["source_id"]

        if target_type == "dimension":
            for dimension in catalog_service.list_dimensions(certified=True):
                if (
                    self._normalized_message(dimension["dimension_name"]) == normalized
                    or dimension["dimension_id"].lower() == message.lower().strip()
                ):
                    return dimension["dimension_id"]
            search_results = catalog_service.search_metadata(message, document_type="dimension", limit=3, min_score=0.20)
            if search_results and search_results[0]["business_name"].lower() in message.lower():
                return search_results[0]["source_id"]

        return None

    def _is_clear_topic_change(self, message: str) -> bool:
        normalized = self._normalized_message(message)
        if not normalized:
            return False
        return any(
            phrase in normalized
            for phrase in [
                "what is",
                "what does",
                "define",
                "definition",
                "meaning",
                "columns",
                "schema",
                "which table",
                "what table",
                "grain",
                "join",
                "certified metrics",
                "restricted",
                "pii",
            ]
        )

    def _pending_task_from_response(self, response: dict[str, Any]) -> dict[str, Any]:
        semantic_result = response.get("semantic_result") or {}
        return self._pending_task_from_semantic(
            semantic_result=semantic_result,
            original_message=response.get("message") or semantic_result.get("message") or "",
            chart_requested=response.get("intent") == "chart_query",
            chart_type=((response.get("chart_spec") or {}).get("chart_type")),
        )

    def _pending_task_from_semantic(
        self,
        semantic_result: dict[str, Any],
        original_message: str,
        chart_requested: bool,
        chart_type: str | None,
    ) -> dict[str, Any]:
        resolved = semantic_result.get("resolved") or {}
        metric = resolved.get("metric") or {}
        dimensions = resolved.get("dimensions") or []
        ambiguities = semantic_result.get("ambiguities") or []
        missing_slots: list[str] = []
        blocked_reasons: list[str] = []

        if not metric:
            missing_slots.append("metric")
        for ambiguity in ambiguities:
            kind = ambiguity.get("kind")
            if kind == "metric" and "metric" not in missing_slots:
                missing_slots.append("metric")
            elif kind == "dimension" and "dimension" not in missing_slots:
                missing_slots.append("dimension")
            elif kind == "restricted_column":
                if "restricted_replacement" not in missing_slots:
                    missing_slots.append("restricted_replacement")
                blocked_reasons.append(ambiguity.get("question") or "Requested restricted column is blocked.")
            elif kind == "join_path":
                if "join_path" not in missing_slots:
                    missing_slots.append("join_path")
                blocked_reasons.append(ambiguity.get("question") or "No certified join path was found.")

        return {
            "task_type": "analytical",
            "original_message": original_message,
            "intent": semantic_result.get("intent") or "analytical_query",
            "chart_requested": chart_requested,
            "chart_type": chart_type,
            "resolved_metric_id": metric.get("id"),
            "resolved_metric_label": metric.get("business_name"),
            "resolved_dimension_ids": [dimension["id"] for dimension in dimensions if dimension.get("id")],
            "resolved_dimension_labels": [dimension["business_name"] for dimension in dimensions if dimension.get("business_name")],
            "missing_slots": missing_slots,
            "blocked_reasons": blocked_reasons,
            "clarification_options": ambiguities,
            "status": "awaiting_clarification",
        }

    def _pending_task_clarification_question(
        self,
        pending_task: dict[str, Any],
        selection: dict[str, Any] | None = None,
    ) -> str:
        lines: list[str] = []
        understood = []
        if pending_task.get("resolved_metric_label"):
            understood.append(f"Metric: {pending_task['resolved_metric_label']}")
        for label in pending_task.get("resolved_dimension_labels") or []:
            understood.append(f"Grouping: {label}")

        selection = selection or {}
        selected_dimension_ids = set(selection.get("dimension_ids") or [])
        if selected_dimension_ids and not selection.get("metric_id") and "metric" in (pending_task.get("missing_slots") or []):
            selected_labels = [
                label
                for dimension_id, label in zip(
                    pending_task.get("resolved_dimension_ids") or [],
                    pending_task.get("resolved_dimension_labels") or [],
                )
                if dimension_id in selected_dimension_ids
            ]
            if selected_labels:
                lines.append(f"I already captured {', '.join(selected_labels)} as the grouping.")

        if understood:
            lines.append("I understood:")
            lines.extend(f"- {item}" for item in understood)

        missing_labels = [self._slot_label(slot) for slot in pending_task.get("missing_slots") or []]
        if missing_labels:
            if lines:
                lines.append("")
            lines.append("I still need:")
            lines.extend(f"- {label}" for label in missing_labels)

        blocked_reasons = pending_task.get("blocked_reasons") or []
        if blocked_reasons:
            if lines:
                lines.append("")
            lines.append("Governance block:")
            lines.extend(f"- {reason}" for reason in blocked_reasons)

        clarification_text = self._clarification_question(pending_task.get("clarification_options") or [])
        if clarification_text:
            if lines:
                lines.append("")
            lines.append(clarification_text)
        return "\n".join(lines) if lines else clarification_text

    def _slot_label(self, slot: str) -> str:
        labels = {
            "metric": "a certified governed metric",
            "dimension": "a certified governed grouping",
            "time_period": "a time period",
            "chart_type": "a chart type",
            "restricted_replacement": "a governed non-restricted replacement",
            "join_path": "a certified join path",
        }
        return labels.get(slot, slot.replace("_", " "))

    def _resolve_pending_choice(self, message: str, pending: dict[str, Any]) -> dict[str, Any]:
        normalized = message.lower().strip()
        metric_id = None
        dimension_ids: list[str] = []
        matched = False
        ambiguities = pending.get("options") or []
        allow_numbered_choice = len(ambiguities) == 1
        for ambiguity in ambiguities:
            options = ambiguity.get("options") or []
            numbered_option = self._numbered_option(normalized=normalized, options=options) if allow_numbered_choice else None
            if numbered_option:
                option_id = numbered_option.get("id")
                if ambiguity.get("kind") == "metric":
                    metric_id = option_id
                elif ambiguity.get("kind") == "dimension":
                    dimension_ids.append(option_id)
                matched = True
                continue

            best_option_id = None
            best_score = 0
            for option in options:
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
                matched = True
        return {"metric_id": metric_id, "dimension_ids": dimension_ids, "matched": matched}

    def _numbered_option(self, normalized: str, options: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not normalized.isdigit():
            return None
        index = int(normalized) - 1
        if index < 0 or index >= len(options):
            return None
        return options[index]

    def _meaningful_words(self, text: str) -> set[str]:
        ignored = {"average", "balance", "metric", "dimension", "by", "the", "and", "of"}
        return {word for word in text.replace("_", " ").replace(".", " ").split() if len(word) > 2 and word not in ignored}

    def _clarification_question(self, ambiguities: list[dict[str, Any]]) -> str:
        if ambiguities and all(ambiguity.get("kind") == "restricted_column" for ambiguity in ambiguities):
            lines = ["I cannot use the requested restricted attribute in generated SQL."]
        else:
            lines = ["I found more than one governed meaning. Please clarify:"]
        for ambiguity in ambiguities:
            lines.append(f"\n{ambiguity.get('question')}")
            for index, option in enumerate(ambiguity.get("options") or [], start=1):
                table = option.get("table")
                column = option.get("column")
                location = f" ({table}.{column})" if table and column else ""
                lines.append(f"{index}. {option.get('label')}{location}")
        return "\n".join(lines)

    def _information_answer(self, message: str, decision: dict[str, Any], context: dict[str, Any]) -> str:
        structured_answer = self._structured_metadata_answer(message)
        if structured_answer:
            return structured_answer

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
        if top.get("document_type") == "lineage":
            lineage_answer = self._lineage_answer_from_search_result(top)
            if lineage_answer:
                return lineage_answer
        return f"{top.get('business_name')} is documented in the governed metadata catalog. Source: {top.get('source_id')}."

    def _structured_metadata_answer(self, message: str) -> str | None:
        normalized = self._normalized_message(message)

        if self._asks_for_certified_metrics(normalized):
            return self._certified_metrics_answer(normalized)

        if self._asks_for_restricted_columns(normalized):
            table_name = self._table_from_message(message) or ("dim_customer" if "customer" in normalized else None)
            if table_name:
                return self._restricted_columns_answer(table_name)

        if self._asks_for_join_path(normalized):
            table_names = self._tables_in_message(message)
            if len(table_names) >= 2:
                return self._join_path_answer(table_names[0], table_names[1])

        if self._asks_for_lineage(normalized):
            return self._lineage_answer_for_message(message)

        if self._asks_where_column_lives(normalized):
            return self._column_location_answer(normalized)

        table_name = self._table_from_message(message)
        if table_name and self._asks_for_columns(normalized):
            return self._columns_answer(table_name)

        if table_name and "grain" in normalized:
            return self._grain_answer(table_name)

        return None

    def _asks_for_certified_metrics(self, normalized: str) -> bool:
        return "certified" in normalized and ("metric" in normalized or "metrics" in normalized)

    def _asks_for_restricted_columns(self, normalized: str) -> bool:
        return ("restricted" in normalized or "pii" in normalized) and (
            "column" in normalized or "columns" in normalized or "field" in normalized or "fields" in normalized
        )

    def _asks_for_join_path(self, normalized: str) -> bool:
        return "join" in normalized or "joins" in normalized

    def _asks_for_lineage(self, normalized: str) -> bool:
        lineage_terms = [
            "lineage",
            "come from",
            "comes from",
            "source",
            "sourced",
            "origin",
            "upstream",
            "transformation",
            "transformed",
            "loaded from",
            "data flow",
            "etl",
            "pipeline",
        ]
        return any(term in normalized for term in lineage_terms)

    def _asks_for_columns(self, normalized: str) -> bool:
        return any(term in normalized for term in ["column", "columns", "field", "fields", "schema"])

    def _asks_where_column_lives(self, normalized: str) -> bool:
        return any(term in normalized for term in ["which table", "what table", "where is", "where are"]) and any(
            term in normalized for term in ["column", "field", "attribute", "contain", "contains"]
        )

    def _columns_answer(self, table_name: str) -> str:
        table = catalog_service.get_table(table_name)
        if not table:
            return f"I could not find `{table_name}` in the governed metadata catalog."
        columns = catalog_service.list_columns(table_name=table_name)
        lines = [
            f"`{table_name}` ({table['business_name']}) is a certified {table['table_type'].lower()} table.",
            f"Grain: {table['grain']}",
            "Columns:",
        ]
        for column in columns:
            governance_notes = []
            if column.get("pii_flag"):
                governance_notes.append("PII")
            if column.get("restricted_flag"):
                governance_notes.append("restricted")
            suffix = f" [{', '.join(governance_notes)}]" if governance_notes else ""
            lines.append(
                f"- `{table_name}.{column['column_name']}` ({column['data_type']}): "
                f"{column['business_name']} - {column['description']}{suffix}"
            )
        return "\n".join(lines)

    def _grain_answer(self, table_name: str) -> str:
        table = catalog_service.get_table(table_name)
        if not table:
            return f"I could not find `{table_name}` in the governed metadata catalog."
        return (
            f"`{table_name}` is the governed `{table['business_name']}` table. "
            f"Its grain is: {table['grain']} Refresh frequency: {table['refresh_frequency']}. "
            f"Data owner: {table['data_owner']}."
        )

    def _restricted_columns_answer(self, table_name: str) -> str:
        table = catalog_service.get_table(table_name)
        if not table:
            return f"I could not find `{table_name}` in the governed metadata catalog."
        restricted_columns = [
            column for column in catalog_service.list_columns(table_name=table_name) if column.get("pii_flag") or column.get("restricted_flag")
        ]
        if not restricted_columns:
            return f"`{table_name}` has no columns marked as PII or restricted in the governed metadata catalog."
        lines = [f"Restricted or PII columns in `{table_name}`:"]
        for column in restricted_columns:
            flags = []
            if column.get("pii_flag"):
                flags.append("PII")
            if column.get("restricted_flag"):
                flags.append("restricted")
            lines.append(
                f"- `{table_name}.{column['column_name']}`: {column['business_name']} "
                f"({', '.join(flags)}). {column['description']}"
            )
        return "\n".join(lines)

    def _certified_metrics_answer(self, normalized: str) -> str:
        metrics = catalog_service.list_metrics(certified=True)
        subject_hint = self._subject_hint(normalized)
        if subject_hint:
            metrics = [
                metric
                for metric in metrics
                if subject_hint in self._metadata_blob(
                    metric.get("metric_name"),
                    metric.get("description"),
                    metric.get("base_table"),
                    metric.get("subject_area"),
                )
            ]
        if not metrics:
            return "I could not find certified metrics matching that subject area in the governed metadata catalog."
        lines = ["Certified governed metrics available:"]
        for metric in metrics[:12]:
            lines.append(
                f"- `{metric['metric_id']}`: {metric['metric_name']} "
                f"from `{metric['base_table']}` - {metric['description']}"
            )
        return "\n".join(lines)

    def _column_location_answer(self, normalized: str) -> str | None:
        tokens = self._lookup_tokens(normalized)
        dimensions = catalog_service.list_dimensions(certified=True)
        matching_dimensions = [
            dimension
            for dimension in dimensions
            if all(token in self._metadata_blob(dimension.get("dimension_name"), dimension.get("description"), dimension.get("table_name"), dimension.get("column_name")) for token in tokens)
        ]
        if not matching_dimensions and "channel" in normalized and "deposit" in normalized:
            matching_dimensions = [
                dimension
                for dimension in dimensions
                if dimension.get("dimension_id") == "dimension.transaction_channel"
            ]
        if matching_dimensions:
            lines = ["The governed catalog maps that business attribute to:"]
            for dimension in matching_dimensions[:5]:
                lines.append(
                    f"- {dimension['dimension_name']}: `{dimension['table_name']}.{dimension['column_name']}`. "
                    f"{dimension['description']}"
                )
            return "\n".join(lines)

        matching_columns = [
            column
            for column in catalog_service.list_columns()
            if all(token in self._metadata_blob(column.get("business_name"), column.get("description"), column.get("table_name"), column.get("column_name")) for token in tokens)
        ]
        if matching_columns:
            lines = ["The governed catalog has these matching physical columns:"]
            for column in matching_columns[:5]:
                lines.append(
                    f"- `{column['table_name']}.{column['column_name']}`: {column['business_name']} - {column['description']}"
                )
            return "\n".join(lines)
        return None

    def _join_path_answer(self, from_table: str, to_table: str) -> str:
        path = self._certified_join_path(from_table, to_table)
        if not path:
            return f"I could not find a certified join path from `{from_table}` to `{to_table}` in the metadata catalog."
        lines = [f"Certified join path from `{from_table}` to `{to_table}`:"]
        for index, join in enumerate(path, start=1):
            lines.append(
                f"{index}. `{join['from_table']}.{join['from_column']}` {join['join_type']} JOIN "
                f"`{join['to_table']}.{join['to_column']}` ({join['relationship_type']}) - {join['description']}"
            )
        return "\n".join(lines)

    def _lineage_answer_for_message(self, message: str) -> str | None:
        results = catalog_service.search_metadata(message, document_type="lineage", limit=5, min_score=0.05)
        lineages = []
        seen = set()
        for result in results:
            lineage = self._lineage_from_search_result(result)
            if not lineage or lineage["lineage_id"] in seen:
                continue
            seen.add(lineage["lineage_id"])
            lineages.append(lineage)
        if not lineages:
            return None
        exact_lineages = [lineage for lineage in lineages if self._lineage_matches_message(message, lineage)]
        if exact_lineages:
            lineages = exact_lineages
        return self._format_lineage_answer(lineages)

    def _lineage_answer_from_search_result(self, result: dict[str, Any]) -> str | None:
        lineage = self._lineage_from_search_result(result)
        if not lineage:
            return None
        return self._format_lineage_answer([lineage])

    def _lineage_from_search_result(self, result: dict[str, Any]) -> dict[str, Any] | None:
        lineage = catalog_service.get_lineage(result.get("source_id") or "")
        if lineage:
            return lineage
        table_name = result.get("table_name")
        column_name = result.get("column_name")
        if table_name and column_name:
            matches = catalog_service.list_lineage(asset_name=f"{table_name}.{column_name}")
            return matches[0] if matches else None
        return None

    def _lineage_matches_message(self, message: str, lineage: dict[str, Any]) -> bool:
        normalized = self._normalized_message(message)
        target_column = self._normalized_message(lineage["target_column"].replace("_", " "))
        source_object = self._normalized_message(lineage["source_object"])
        if target_column and target_column in normalized:
            return True
        if source_object and source_object in normalized:
            return True
        return False

    def _format_lineage_answer(self, lineages: list[dict[str, Any]]) -> str:
        lines = ["Governed lineage from the metadata catalog:"]
        for lineage in lineages[:5]:
            lines.extend(
                [
                    f"- Target: `{lineage['target_table']}.{lineage['target_column']}`",
                    f"  Source system: {lineage['source_system']}",
                    f"  Source object: {lineage['source_object']}",
                    f"  Transformation: {lineage['transformation']}",
                    f"  Refresh frequency: {lineage['refresh_frequency']}",
                    f"  Data owner: {lineage['data_owner']}",
                ]
            )
        return "\n".join(lines)

    def _certified_join_path(self, from_table: str, to_table: str) -> list[dict[str, Any]]:
        joins = [join for join in catalog_service.list_join_paths() if join.get("certified_flag")]
        adjacency: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for join in joins:
            adjacency.setdefault(join["from_table"], []).append((join["to_table"], join))
            reversed_join = {
                **join,
                "from_table": join["to_table"],
                "from_column": join["to_column"],
                "to_table": join["from_table"],
                "to_column": join["from_column"],
            }
            adjacency.setdefault(join["to_table"], []).append((join["from_table"], reversed_join))

        queue: deque[tuple[str, list[dict[str, Any]]]] = deque([(from_table, [])])
        visited = {from_table}
        while queue:
            current_table, path = queue.popleft()
            if current_table == to_table:
                return path
            for next_table, join in adjacency.get(current_table, []):
                if next_table in visited:
                    continue
                visited.add(next_table)
                queue.append((next_table, path + [join]))
        return []

    def _table_from_message(self, message: str) -> str | None:
        table_names = self._tables_in_message(message)
        if table_names:
            return table_names[0]
        normalized = self._normalized_message(message)
        if "customer" in normalized and "table" in normalized:
            return "dim_customer"
        if "account" in normalized and "table" in normalized:
            return "dim_account"
        if "loan" in normalized and "table" in normalized:
            return "dim_loan"
        return None

    def _tables_in_message(self, message: str) -> list[str]:
        raw = message.lower()
        normalized = self._normalized_message(message)
        table_positions: list[tuple[int, str]] = []
        for table in catalog_service.list_tables():
            table_name = table["table_name"]
            variants = {
                table_name.lower(),
                table_name.lower().replace("_", " "),
                self._normalized_message(table.get("business_name") or ""),
            }
            positions = []
            for variant in variants:
                if not variant:
                    continue
                raw_position = raw.find(variant)
                normalized_position = normalized.find(variant)
                if raw_position >= 0:
                    positions.append(raw_position)
                if normalized_position >= 0:
                    positions.append(normalized_position)
            if positions:
                table_positions.append((min(positions), table_name))
        return [table_name for _, table_name in sorted(table_positions, key=lambda item: item[0])]

    def _lookup_tokens(self, normalized: str) -> list[str]:
        stopwords = {
            "which",
            "what",
            "where",
            "table",
            "column",
            "columns",
            "field",
            "fields",
            "attribute",
            "attributes",
            "contain",
            "contains",
            "available",
            "in",
            "is",
            "are",
            "the",
            "and",
            "does",
            "do",
        }
        return [word for word in normalized.split() if len(word) > 2 and word not in stopwords]

    def _subject_hint(self, normalized: str) -> str | None:
        if "deposit" in normalized:
            return "deposit"
        if "loan" in normalized or "lending" in normalized:
            return "loan"
        if "credit" in normalized or "risk" in normalized:
            return "risk"
        if "profit" in normalized or "relationship" in normalized:
            return "profit"
        return None

    def _metadata_blob(self, *values: Any) -> str:
        return " ".join(str(value or "").lower().replace("_", " ") for value in values)

    def _normalized_message(self, message: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9_]+", " ", message.lower())).strip()

    def _resolver_prompt(self) -> str:
        return (
            "You are a governed commercial banking analytics assistant using AWS Bedrock Titan. "
            "First classify the user request into exactly one intent: definition_question, metadata_question, "
            "lineage_question, table_discovery_question, analytical_query, chart_query, clarification_response, "
            "or unsupported. Metric names may contain words such as average, total, rate, or balance; those words "
            "do not mean the user wants calculation. If the user asks what something means, define, explain, or asks "
            "for business meaning, classify as definition_question and set allow_sql=false and allow_chart=false. "
            "If the user asks where data lives or asks for fields/schema, classify as metadata_question or "
            "table_discovery_question and set allow_sql=false. If the user asks where a metric comes from, classify "
            "as lineage_question and set allow_sql=false. Only analytical_query may generate SQL without a chart. "
            "Only chart_query may generate SQL plus chart, and only when the user explicitly asks to plot, chart, "
            "graph, visualize, or trend data. Choose semantic assets only from provided metadata context. If table, "
            "column, metric, dimension, or response mode is unclear, ask a follow-up clarification. Never invent table "
            "names or column names. For analytical and chart requests, produce a semantic plan by selecting "
            "selected_metric_id and selected_dimension_ids from the provided governed metadata IDs only. If a "
            "requested attribute is restricted or the intended metric/dimension is ambiguous, block SQL and ask "
            "for clarification. Return only JSON with intent, action, response_mode, allow_sql, allow_chart, "
            "confidence, reason, selected_metric_id, selected_dimension_ids, chart_requested, chart_type, "
            "clarification_question, clarification_options, and citations."
        )

    def _answer_prompt(self) -> str:
        return (
            "Generate a concise business answer from the SQL result table. Include only facts supported by rows, "
            "and produce JSON with answer and key_points. Do not invent sources."
        )

    def _response(self, state: LlmAssistantState) -> dict[str, Any]:
        if state.get("cached_response"):
            return state["cached_response"]
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
            "response_mode": state.get("response_mode") or decision.get("response_mode"),
            "requires_clarification": state.get("requires_clarification", False),
            "clarification_options": state.get("clarification_options", []),
            "pending_task": state.get("pending_task") or {},
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
                "intent_confidence": decision.get("confidence"),
                "response_mode": decision.get("response_mode"),
                "allow_sql": decision.get("allow_sql"),
                "allow_chart": decision.get("allow_chart"),
                "policy_reason": decision.get("policy_reason"),
                "sql_generation": sql_result.get("llm_generation"),
                "chart_generation": ((chart_result or {}).get("chart_spec") or {}).get("chart_plan"),
            },
            "assumptions": sql_result.get("assumptions") or [],
            "warnings": state.get("warnings") or [],
            "graph_trace": state.get("graph_trace", []),
            "_resolved_cache_key": state.get("resolved_cache_key"),
        }


llm_governed_assistant_graph = LlmGovernedAssistantGraph()
