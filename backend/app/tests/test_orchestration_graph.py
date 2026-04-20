from fastapi.testclient import TestClient

from backend.app.cache.service import assistant_response_cache
from backend.app.main import app
from backend.app.orchestration.graph import OrchestrationStatus, governed_assistant_graph
from backend.app.orchestration.llm_graph import llm_governed_assistant_graph


client = TestClient(app)


def test_information_question_uses_metadata_flow() -> None:
    result = governed_assistant_graph.invoke("What does average collected balance mean?")

    assert result["status"] == OrchestrationStatus.ANSWERED
    assert result["intent"] == "definition_question"
    assert result["route"] == "information_flow"
    assert result["generated_sql"] is None
    assert "certified metric" in result["answer"]
    assert result["graph_trace"] == ["classify:definition_question", "information"]


def test_ambiguous_analytics_request_stops_for_clarification() -> None:
    result = governed_assistant_graph.invoke("Give me average balance by segment")

    assert result["status"] == OrchestrationStatus.NEEDS_CLARIFICATION
    assert result["requires_clarification"] is True
    assert result["generated_sql"] is None
    assert {item["kind"] for item in result["clarification_options"]} == {"metric", "dimension"}
    assert result["graph_trace"] == ["classify:analytical_query", "analytics"]


def test_clarified_analytics_request_generates_governed_sql() -> None:
    result = governed_assistant_graph.invoke(
        "Give me average balance by segment",
        selected_metric_id="metric.average_deposit_ledger_balance",
        selected_dimension_ids=["dimension.customer_segment"],
        user_role="technical_user",
        execute_sql=False,
    )

    assert result["status"] == OrchestrationStatus.SQL_GENERATED
    assert result["requires_clarification"] is False
    assert result["sql_validation"]["is_valid"] is True
    assert "AVG(fdb.ledger_balance)" in result["generated_sql"]
    assert result["next_action"] == "execute_validated_sql_in_next_phase"


def test_clarified_analytics_request_executes_governed_query() -> None:
    result = governed_assistant_graph.invoke(
        "Give me average balance by segment",
        selected_metric_id="metric.average_deposit_ledger_balance",
        selected_dimension_ids=["dimension.customer_segment"],
        user_role="technical_user",
    )

    assert result["status"] == OrchestrationStatus.ANSWERED
    assert result["sql_validation"]["is_valid"] is True
    assert result["result_table"]["row_count"] > 0
    assert result["result_table"]["columns"] == ["customer_segment", "average_deposit_ledger_balance"]
    assert result["key_points"]
    assert "highest" in result["answer"]
    assert result["next_action"] == "show_answer_and_table"
    assert result["graph_trace"] == ["classify:analytical_query", "analytics", "execute", "answer"]


def test_chat_api_contract_for_sql_generation() -> None:
    response = client.post(
        "/chat/message",
        json={
            "message": "Plot loan utilization by month",
            "user_role": "technical_user",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == OrchestrationStatus.ANSWERED
    assert payload["intent"] == "chart_query"
    assert payload["sql_validation"]["is_valid"] is True
    assert "GROUP BY dd.year_month" in payload["generated_sql"]
    assert payload["result_table"]["columns"] == ["year_month", "loan_utilization_rate"]
    assert payload["key_points"]
    assert payload["chart_spec"]["chart_type"] == "line"
    assert payload["route"] == "llm_governed_chat_flow"
    assert "llm_interpret" in payload["graph_trace"]
    assert "execute" in payload["graph_trace"]
    assert payload["source_citations"]


def test_chat_api_definition_question_does_not_generate_sql_or_chart() -> None:
    response = client.post(
        "/chat/message",
        json={
            "message": "What does average collected balance mean?",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == OrchestrationStatus.ANSWERED
    assert payload["intent"] == "definition_question"
    assert payload["response_mode"] == "definition_only"
    assert payload["generated_sql"] is None
    assert payload["result_table"] is None
    assert payload["chart_spec"] is None
    assert payload["source_citations"]


def test_llm_policy_blocks_sql_for_information_intent(monkeypatch) -> None:
    def fake_invoke_json(**_: object) -> dict[str, object]:
        return {
            "intent": "definition_question",
            "action": "analytics",
            "response_mode": "sql_answer",
            "allow_sql": True,
            "allow_chart": True,
            "confidence": 0.95,
            "selected_metric_id": "metric.average_deposit_collected_balance",
            "selected_dimension_ids": ["dimension.customer_segment"],
            "chart_requested": True,
            "chart_type": "bar",
        }

    monkeypatch.setattr("backend.app.orchestration.llm_graph.llm_client.invoke_json", fake_invoke_json)

    result = llm_governed_assistant_graph.invoke(
        "What does average collected balance mean?",
        user_role="technical_user",
    )

    assert result["status"] == OrchestrationStatus.ANSWERED
    assert result["intent"] == "definition_question"
    assert result["response_mode"] == "definition_only"
    assert result["generated_sql"] is None
    assert result["result_table"] is None
    assert result["chart_spec"] is None
    assert result["llm_trace"]["allow_sql"] is False
    assert result["llm_trace"]["allow_chart"] is False


def test_low_confidence_llm_analytics_decision_asks_clarification(monkeypatch) -> None:
    def fake_invoke_json(**_: object) -> dict[str, object]:
        return {
            "intent": "analytical_query",
            "action": "analytics",
            "response_mode": "sql_answer",
            "allow_sql": True,
            "allow_chart": False,
            "confidence": 0.42,
            "selected_metric_id": "metric.average_deposit_collected_balance",
            "selected_dimension_ids": ["dimension.customer_segment"],
            "chart_requested": False,
        }

    monkeypatch.setattr("backend.app.orchestration.llm_graph.llm_client.invoke_json", fake_invoke_json)

    result = llm_governed_assistant_graph.invoke(
        "Average collected balance.",
        user_role="technical_user",
    )

    assert result["status"] == OrchestrationStatus.NEEDS_CLARIFICATION
    assert result["requires_clarification"] is True
    assert result["response_mode"] == "clarification"
    assert result["generated_sql"] is None
    assert result["chart_spec"] is None


def test_llm_chart_prefers_exact_metadata_dimension_over_wrong_llm_dimension(monkeypatch) -> None:
    def fake_invoke_json(**_: object) -> dict[str, object]:
        return {
            "intent": "chart_query",
            "action": "analytics",
            "response_mode": "chart_answer",
            "allow_sql": True,
            "allow_chart": True,
            "confidence": 0.95,
            "selected_metric_id": "metric.average_deposit_ledger_balance",
            "selected_dimension_ids": ["dimension.product_segment"],
            "chart_requested": True,
            "chart_type": "bar",
        }

    monkeypatch.setattr("backend.app.orchestration.llm_graph.llm_client.invoke_json", fake_invoke_json)

    result = llm_governed_assistant_graph.invoke(
        "Create a bar chart of average deposit ledger balance by customer segment",
        user_role="technical_user",
    )

    assert result["status"] == OrchestrationStatus.ANSWERED
    assert result["result_table"]["columns"] == ["customer_segment", "average_deposit_ledger_balance"]
    assert result["chart_spec"]["x_axis"]["column"] == "customer_segment"
    assert result["chart_spec"]["y_axis"]["column"] == "average_deposit_ledger_balance"
    assert "GROUP BY dc.customer_segment" in result["generated_sql"]


def test_chat_followup_preserves_original_dimensions_after_metric_clarification() -> None:
    first_response = client.post(
        "/chat/message",
        json={
            "message": "Show average balance by customer segment",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    )
    first_payload = first_response.json()

    assert first_response.status_code == 200
    assert first_payload["requires_clarification"] is True

    second_response = client.post(
        "/chat/message",
        json={
            "message": "Average Deposit Ledger Balance",
            "conversation_id": first_payload["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    )
    second_payload = second_response.json()

    assert second_response.status_code == 200
    assert second_payload["status"] == OrchestrationStatus.ANSWERED
    assert second_payload["result_table"]["columns"] == ["customer_segment", "average_deposit_ledger_balance"]
    assert "GROUP BY dc.customer_segment" in second_payload["generated_sql"]


def test_chat_followup_accepts_numbered_metric_choice() -> None:
    first_response = client.post(
        "/chat/message",
        json={
            "message": "Show average balance by customer segment",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    )
    first_payload = first_response.json()

    second_response = client.post(
        "/chat/message",
        json={
            "message": "1",
            "conversation_id": first_payload["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    )
    second_payload = second_response.json()

    assert second_response.status_code == 200
    assert second_payload["status"] == OrchestrationStatus.ANSWERED
    assert second_payload["result_table"]["columns"] == ["customer_segment", "average_deposit_ledger_balance"]
    assert "GROUP BY dc.customer_segment" in second_payload["generated_sql"]


def test_chat_restricted_customer_name_request_asks_for_safe_grouping() -> None:
    response = client.post(
        "/chat/message",
        json={
            "message": "List customer names with average deposit balance",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["status"] == OrchestrationStatus.NEEDS_CLARIFICATION
    assert payload["requires_clarification"] is True
    assert payload["generated_sql"] is None
    assert payload["result_table"] is None
    assert "restricted" in payload["answer"]
    assert payload["clarification_options"][0]["kind"] == "restricted_column"


def test_chat_analytical_answer_is_cached_when_llm_answer_varies(monkeypatch) -> None:
    assistant_response_cache.clear()
    calls = {"answer_generation": 0}

    def fake_invoke_json(**kwargs: object) -> dict[str, object]:
        if kwargs.get("task_name") == "answer_generation":
            calls["answer_generation"] += 1
            return {"answer": "non deterministic LLM answer", "key_points": ["unstable"]}
        fallback = kwargs.get("fallback")
        return fallback() if callable(fallback) else {}

    monkeypatch.setattr("backend.app.orchestration.llm_graph.llm_client.invoke_json", fake_invoke_json)

    first = client.post(
        "/chat/message",
        json={"message": "Plot loan utilization by month.", "user_role": "technical_user", "technical_mode": True},
    ).json()
    second = client.post(
        "/chat/message",
        json={
            "message": "Plot loan utilization by month.",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert calls["answer_generation"] == 1
    assert first["answer"] == second["answer"]
    assert second["llm_trace"]["cache_status"] == "hit"
    assert first["result_table"]["columns"] == ["year_month", "loan_utilization_rate"]
    assert first["chart_spec"]["x_axis"]["column"] == "year_month"
    assert "GROUP BY dd.year_month" in first["generated_sql"]


def test_chat_cache_is_scoped_to_conversation() -> None:
    assistant_response_cache.clear()

    first = client.post(
        "/chat/message",
        json={"message": "Plot loan utilization by month.", "user_role": "technical_user", "technical_mode": True},
    ).json()
    same_conversation = client.post(
        "/chat/message",
        json={
            "message": "Plot loan utilization by month.",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    new_conversation = client.post(
        "/chat/message",
        json={"message": "Plot loan utilization by month.", "user_role": "technical_user", "technical_mode": True},
    ).json()

    assert same_conversation["llm_trace"]["cache_status"] == "hit"
    assert new_conversation["conversation_id"] != first["conversation_id"]
    assert new_conversation["llm_trace"].get("cache_status") != "hit"


def test_ambiguous_question_is_not_answered_from_prior_clarified_cache() -> None:
    assistant_response_cache.clear()

    first = client.post(
        "/chat/message",
        json={"message": "can you give balance by segment", "user_role": "technical_user", "technical_mode": True},
    ).json()
    product_answer = client.post(
        "/chat/message",
        json={
            "message": "Average Deposit Ledger Balance and Product Segment",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    second = client.post(
        "/chat/message",
        json={
            "message": "can you give balance by segment",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    customer_answer = client.post(
        "/chat/message",
        json={
            "message": "Average Deposit Ledger Balance and Customer Segment",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    third = client.post(
        "/chat/message",
        json={
            "message": "can you give balance by segment",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert product_answer["status"] == OrchestrationStatus.ANSWERED
    assert product_answer["result_table"]["columns"] == ["product_segment", "average_deposit_ledger_balance"]
    assert customer_answer["status"] == OrchestrationStatus.ANSWERED
    assert customer_answer["result_table"]["columns"] == ["customer_segment", "average_deposit_ledger_balance"]
    assert second["status"] == OrchestrationStatus.NEEDS_CLARIFICATION
    assert second["generated_sql"] is None
    assert third["status"] == OrchestrationStatus.NEEDS_CLARIFICATION
    assert third["generated_sql"] is None


def test_pending_dimension_change_replaces_prior_dimension_choice() -> None:
    assistant_response_cache.clear()

    first = client.post(
        "/chat/message",
        json={"message": "can you give balance by segment", "user_role": "technical_user", "technical_mode": True},
    ).json()
    product_pending = client.post(
        "/chat/message",
        json={
            "message": "Product Segment",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    customer_pending = client.post(
        "/chat/message",
        json={
            "message": "Customer Segment",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    final_answer = client.post(
        "/chat/message",
        json={
            "message": "Average Deposit Ledger Balance",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert product_pending["pending_task"]["resolved_dimension_ids"] == ["dimension.product_segment"]
    assert customer_pending["pending_task"]["resolved_dimension_ids"] == ["dimension.customer_segment"]
    assert final_answer["status"] == OrchestrationStatus.ANSWERED
    assert final_answer["result_table"]["columns"] == ["customer_segment", "average_deposit_ledger_balance"]
    assert "GROUP BY dc.customer_segment" in final_answer["generated_sql"]
    assert "product_segment" not in final_answer["generated_sql"]


def test_resolved_cache_key_does_not_mix_different_dimensions() -> None:
    assistant_response_cache.clear()

    product_first = client.post(
        "/chat/message",
        json={
            "message": "Show average deposit ledger balance by product segment.",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    customer_first = client.post(
        "/chat/message",
        json={
            "message": "Show average deposit ledger balance by customer segment.",
            "conversation_id": product_first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    product_second = client.post(
        "/chat/message",
        json={
            "message": "Show average deposit ledger balance by product segment.",
            "conversation_id": product_first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert product_first["result_table"]["columns"] == ["product_segment", "average_deposit_ledger_balance"]
    assert customer_first["result_table"]["columns"] == ["customer_segment", "average_deposit_ledger_balance"]
    assert product_second["result_table"]["columns"] == ["product_segment", "average_deposit_ledger_balance"]
    assert product_second["llm_trace"]["cache_status"] == "hit"


def test_chat_session_reset_clears_backend_cache() -> None:
    assistant_response_cache.clear()

    first = client.post(
        "/chat/message",
        json={
            "message": "Show average deposit ledger balance by customer segment.",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    second = client.post(
        "/chat/message",
        json={
            "message": "Show average deposit ledger balance by customer segment.",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    reset = client.post("/chat/session/reset").json()
    third = client.post(
        "/chat/message",
        json={
            "message": "Show average deposit ledger balance by customer segment.",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert second["llm_trace"]["cache_status"] == "hit"
    assert reset == {"status": "reset", "cache_cleared": True}
    assert third["llm_trace"].get("cache_status") != "hit"


def test_chat_ambiguous_balance_by_segment_asks_metric_and_dimension() -> None:
    assistant_response_cache.clear()

    payload = client.post(
        "/chat/message",
        json={"message": "can you give balance by segment", "user_role": "technical_user", "technical_mode": True},
    ).json()

    assert payload["status"] == OrchestrationStatus.NEEDS_CLARIFICATION
    assert payload["requires_clarification"] is True
    assert {item["kind"] for item in payload["clarification_options"]} == {"metric", "dimension"}
    assert payload["generated_sql"] is None


def test_llm_selected_dimension_cannot_bypass_ambiguous_segment_clarification(monkeypatch) -> None:
    assistant_response_cache.clear()

    def fake_invoke_json(**kwargs: object) -> dict[str, object]:
        if kwargs.get("task_name") == "intent_semantic_resolution":
            return {
                "intent": "analytical_query",
                "action": "analytics",
                "response_mode": "sql_answer",
                "allow_sql": True,
                "allow_chart": False,
                "confidence": 0.95,
                "selected_metric_id": "metric.average_deposit_ledger_balance",
                "selected_dimension_ids": ["dimension.product_segment"],
                "chart_requested": False,
            }
        fallback = kwargs.get("fallback")
        return fallback() if callable(fallback) else {}

    monkeypatch.setattr("backend.app.orchestration.llm_graph.llm_client.invoke_json", fake_invoke_json)

    payload = client.post(
        "/chat/message",
        json={"message": "can you give balance by segment", "user_role": "technical_user", "technical_mode": True},
    ).json()

    assert payload["status"] == OrchestrationStatus.NEEDS_CLARIFICATION
    assert payload["requires_clarification"] is True
    assert payload["generated_sql"] is None
    assert {item["kind"] for item in payload["clarification_options"]} == {"metric", "dimension"}
    dimension_options = next(item for item in payload["clarification_options"] if item["kind"] == "dimension")["options"]
    assert {option["id"] for option in dimension_options} == {"dimension.customer_segment", "dimension.product_segment"}


def test_chat_missing_metric_preserves_resolved_dimension_and_offers_related_metrics() -> None:
    assistant_response_cache.clear()

    payload = client.post(
        "/chat/message",
        json={
            "message": "show average loan interest rate by product segment",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert payload["status"] == OrchestrationStatus.NEEDS_CLARIFICATION
    assert payload["requires_clarification"] is True
    assert payload["pending_task"]["resolved_dimension_ids"] == ["dimension.product_segment"]
    assert payload["pending_task"]["missing_slots"] == ["metric"]
    assert "Grouping: Product Segment" in payload["answer"]
    metric_options = payload["clarification_options"][0]["options"]
    assert metric_options
    assert "Loan Utilization Rate" in {option["label"] for option in metric_options}
    assert payload["generated_sql"] is None


def test_chat_pending_followup_reuses_original_task_when_user_repeats_dimension() -> None:
    assistant_response_cache.clear()

    first_payload = client.post(
        "/chat/message",
        json={
            "message": "show average loan interest rate by product segment",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    second_payload = client.post(
        "/chat/message",
        json={
            "message": "product segment",
            "conversation_id": first_payload["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert second_payload["status"] == OrchestrationStatus.NEEDS_CLARIFICATION
    assert second_payload["intent"] == "analytical_query"
    assert second_payload["generated_sql"] is None
    assert "already captured Product Segment" in second_payload["answer"]
    assert second_payload["pending_task"]["original_message"] == "show average loan interest rate by product segment"
    assert second_payload["pending_task"]["missing_slots"] == ["metric"]


def test_chat_pending_followup_metric_completes_original_task_with_preserved_dimension() -> None:
    assistant_response_cache.clear()

    first_payload = client.post(
        "/chat/message",
        json={
            "message": "show average loan interest rate by product segment",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    second_payload = client.post(
        "/chat/message",
        json={
            "message": "Loan Utilization Rate",
            "conversation_id": first_payload["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert second_payload["status"] == OrchestrationStatus.ANSWERED
    assert second_payload["requires_clarification"] is False
    assert second_payload["result_table"]["columns"] == ["product_segment", "loan_utilization_rate"]
    assert "GROUP BY dp.product_segment" in second_payload["generated_sql"]


def test_chat_pending_followup_numbered_metric_completes_original_task() -> None:
    assistant_response_cache.clear()

    first_payload = client.post(
        "/chat/message",
        json={
            "message": "show average loan interest rate by product segment",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()
    second_payload = client.post(
        "/chat/message",
        json={
            "message": "2",
            "conversation_id": first_payload["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert second_payload["status"] == OrchestrationStatus.ANSWERED
    assert second_payload["result_table"]["columns"] == ["product_segment", "loan_utilization_rate"]


def test_chat_table_column_location_uses_catalog_metadata() -> None:
    payload = client.post(
        "/chat/message",
        json={
            "message": "Which table and column contain deposit transaction channel?",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert payload["status"] == OrchestrationStatus.ANSWERED
    assert payload["generated_sql"] is None
    assert "fact_deposit_transaction.channel" in payload["answer"]
    assert "fact_deposit_transaction.account_id" not in payload["answer"]


def test_chat_table_columns_question_lists_schema() -> None:
    payload = client.post(
        "/chat/message",
        json={
            "message": "What columns are available in fact_deposit_transaction?",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert payload["status"] == OrchestrationStatus.ANSWERED
    assert payload["generated_sql"] is None
    assert "transaction_date" in payload["answer"]
    assert "channel" in payload["answer"]
    assert "amount" in payload["answer"]


def test_chat_table_grain_question_uses_table_metadata() -> None:
    payload = client.post(
        "/chat/message",
        json={
            "message": "What is the grain of fact_deposit_balance_daily?",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert payload["status"] == OrchestrationStatus.ANSWERED
    assert payload["generated_sql"] is None
    assert "One row per deposit account per business date" in payload["answer"]


def test_chat_join_path_question_uses_certified_join_catalog() -> None:
    payload = client.post(
        "/chat/message",
        json={
            "message": "How does fact_deposit_balance_daily join to dim_customer?",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert payload["status"] == OrchestrationStatus.ANSWERED
    assert payload["generated_sql"] is None
    assert "fact_deposit_balance_daily.account_id" in payload["answer"]
    assert "dim_customer.customer_id" in payload["answer"]


def test_chat_certified_deposit_metrics_lists_catalog_metrics() -> None:
    payload = client.post(
        "/chat/message",
        json={
            "message": "List certified metrics available for deposits.",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert payload["status"] == OrchestrationStatus.ANSWERED
    assert payload["generated_sql"] is None
    assert "Average Deposit Ledger Balance" in payload["answer"]
    assert "Total Deposit Transaction Amount" in payload["answer"]


def test_chat_product_segment_does_not_add_product_type_grouping() -> None:
    assistant_response_cache.clear()

    payload = client.post(
        "/chat/message",
        json={
            "message": "Show average deposit ledger balance by market and product segment.",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    ).json()

    assert payload["status"] == OrchestrationStatus.ANSWERED
    assert payload["result_table"]["columns"] == ["product_segment", "market", "average_deposit_ledger_balance"]
    assert "product_type" not in payload["generated_sql"]


def test_pending_clarification_choice_bypasses_llm_and_uses_prior_context(monkeypatch) -> None:
    first_response = client.post(
        "/chat/message",
        json={
            "message": "Show average balance by customer segment",
            "user_role": "technical_user",
            "technical_mode": True,
        },
    )
    first_payload = first_response.json()

    def fail_if_intent_llm_called(**kwargs: object) -> dict[str, object]:
        if kwargs.get("task_name") == "intent_semantic_resolution":
            raise AssertionError("matched pending clarification choices should not call the intent LLM")
        fallback = kwargs.get("fallback")
        return fallback() if callable(fallback) else {}

    monkeypatch.setattr("backend.app.orchestration.llm_graph.llm_client.invoke_json", fail_if_intent_llm_called)

    second_response = client.post(
        "/chat/message",
        json={
            "message": "Average Deposit Ledger Balance",
            "conversation_id": first_payload["conversation_id"],
            "user_role": "technical_user",
            "technical_mode": True,
        },
    )
    second_payload = second_response.json()

    assert second_response.status_code == 200
    assert second_payload["status"] == OrchestrationStatus.ANSWERED
    assert second_payload["result_table"]["columns"] == ["customer_segment", "average_deposit_ledger_balance"]


def test_unsupported_question_routes_to_unsupported_flow() -> None:
    result = governed_assistant_graph.invoke("What is the weather today?")

    assert result["status"] == OrchestrationStatus.UNSUPPORTED
    assert result["generated_sql"] is None
    assert result["graph_trace"] == ["classify:unsupported", "unsupported"]
