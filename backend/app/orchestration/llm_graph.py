from __future__ import annotations

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
        cache_key = self._cache_key(
            message=message,
            conversation_id=conversation_id,
            selected_metric_id=selected_metric_id,
            selected_dimension_ids=selected_dimension_ids or [],
            user_role=user_role,
            limit=limit,
            execute_sql=execute_sql,
        )
        if cache_key:
            cached = assistant_response_cache.get(cache_key)
            if cached:
                cached["message"] = message
                cached["conversation_id"] = conversation_id
                cached["graph_trace"] = (cached.get("graph_trace") or []) + ["cache_hit"]
                llm_trace = dict(cached.get("llm_trace") or {})
                llm_trace["cache_status"] = "hit"
                llm_trace["cache_backend"] = (cached.get("cache") or {}).get("backend")
                cached["llm_trace"] = llm_trace
                return cached

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
        if cache_key and response.get("status") in {OrchestrationStatus.ANSWERED, OrchestrationStatus.SQL_GENERATED}:
            assistant_response_cache.set(cache_key, response)
            response.setdefault("llm_trace", {})["cache_status"] = "stored"
        return response

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
                "response_mode": "clarification",
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "intent_policy_clarification"],
            }

        if decision.get("action") == "information":
            answer = self._information_answer(decision=decision, context=state.get("metadata_context") or {})
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
                "response_mode": "clarification",
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "intent_policy_blocked_sql"],
            }

        return self._analytics(state=state, decision=decision)

    def _respond(self, state: LlmAssistantState) -> dict[str, Any]:
        return state

    def _analytics(self, state: LlmAssistantState, decision: dict[str, Any]) -> dict[str, Any]:
        selected_metric_id = decision.get("selected_metric_id") or state.get("selected_metric_id")
        selected_dimension_ids = self._selected_dimension_ids(state=state, decision=decision)

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
                "graph_trace": state.get("graph_trace", []) + ["llm_interpret", "llm_sql_generate"],
            }

        execution_result = governed_query_executor.execute_sql_result(
            message=state["message"],
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
            message=state["message"],
            execution_result=execution_result,
        ).to_dict()

        chart_result = None
        if decision.get("chart_requested") and decision.get("allow_chart"):
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
            "response_mode": "chart_answer" if chart_result else "sql_answer",
            "graph_trace": state.get("graph_trace", [])
            + ["llm_interpret", "llm_semantic_plan", "llm_sql_generate", "validate", "execute", "llm_answer"]
            + (["llm_chart_plan", "python_plotly_build"] if chart_result else []),
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
        for group in (state.get("metadata_context") or {}).get("candidate_groups") or []:
            if group.get("target_type") != "dimension":
                continue
            candidates = group.get("candidates") or []
            if len(candidates) != 1:
                continue
            dimension_id = candidates[0].get("target_id")
            if dimension_id and dimension_id not in dimension_ids:
                dimension_ids.append(dimension_id)
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
            return {
                "action": "clarify",
                "intent": semantic_result["intent"],
                "response_mode": "clarification",
                "allow_sql": False,
                "allow_chart": False,
                "confidence": semantic_result.get("confidence", 0.72),
                "clarification_question": self._clarification_question(ambiguities),
                "clarification_options": ambiguities,
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
        }


llm_governed_assistant_graph = LlmGovernedAssistantGraph()
