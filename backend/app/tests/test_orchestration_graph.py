from fastapi.testclient import TestClient

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


def test_chat_analytical_answer_is_deterministic_when_llm_answer_varies(monkeypatch) -> None:
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
        json={"message": "Plot loan utilization by month.", "user_role": "technical_user", "technical_mode": True},
    ).json()

    assert calls["answer_generation"] == 0
    assert first["answer"] == second["answer"]
    assert first["result_table"]["columns"] == ["year_month", "loan_utilization_rate"]
    assert first["chart_spec"]["x_axis"]["column"] == "year_month"
    assert "GROUP BY dd.year_month" in first["generated_sql"]


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
