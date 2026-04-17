from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.orchestration.graph import OrchestrationStatus, governed_assistant_graph


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


def test_unsupported_question_routes_to_unsupported_flow() -> None:
    result = governed_assistant_graph.invoke("What is the weather today?")

    assert result["status"] == OrchestrationStatus.UNSUPPORTED
    assert result["generated_sql"] is None
    assert result["graph_trace"] == ["classify:unsupported", "unsupported"]
