from fastapi.testclient import TestClient

from backend.app.execution.service import QueryExecutionStatus, governed_query_executor
from backend.app.main import app


client = TestClient(app)


def test_execution_blocks_ambiguous_request_before_sql_runs() -> None:
    result = governed_query_executor.execute_from_message("Give me average balance by segment").to_dict()

    assert result["status"] == QueryExecutionStatus.NEEDS_CLARIFICATION
    assert result["result_table"] is None
    assert result["sql_result"]["generated_sql"] is None
    assert result["sql_result"]["semantic_result"]["ambiguities"]


def test_execution_returns_tabular_rows_for_clarified_request() -> None:
    result = governed_query_executor.execute_from_message(
        message="Give me average balance by segment",
        selected_metric_id="metric.average_deposit_ledger_balance",
        selected_dimension_ids=["dimension.customer_segment"],
        user_role="technical_user",
    ).to_dict()

    assert result["status"] == QueryExecutionStatus.EXECUTED
    assert result["sql_result"]["validation"]["is_valid"] is True
    assert result["result_table"]["columns"] == ["customer_segment", "average_deposit_ledger_balance"]
    assert result["result_table"]["row_count"] > 0
    assert result["execution_ms"] >= 0
    assert result["answer_summary"].endswith(f"{result['result_table']['row_count']} rows.")


def test_business_user_can_execute_but_sql_text_remains_hidden() -> None:
    result = governed_query_executor.execute_from_message(
        message="Give me average balance by segment",
        selected_metric_id="metric.average_deposit_ledger_balance",
        selected_dimension_ids=["dimension.customer_segment"],
    ).to_dict()

    assert result["status"] == QueryExecutionStatus.EXECUTED
    assert result["sql_result"]["sql_visible"] is False
    assert result["sql_result"]["generated_sql"] is None
    assert result["result_table"]["row_count"] > 0


def test_query_execute_api_contract() -> None:
    response = client.post(
        "/query/execute",
        json={
            "message": "Plot loan utilization by month",
            "user_role": "technical_user",
            "limit": 12,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == QueryExecutionStatus.EXECUTED
    assert payload["sql_result"]["validation"]["is_valid"] is True
    assert payload["result_table"]["columns"] == ["year_month", "loan_utilization_rate"]
    assert payload["result_table"]["row_count"] <= 12

