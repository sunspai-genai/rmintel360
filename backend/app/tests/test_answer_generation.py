from fastapi.testclient import TestClient

from backend.app.answer.service import AnswerStatus, governed_answer_generator
from backend.app.main import app


client = TestClient(app)


def test_answer_generation_blocks_ambiguous_request() -> None:
    result = governed_answer_generator.answer_from_message("Give me average balance by segment").to_dict()

    assert result["status"] == AnswerStatus.NEEDS_CLARIFICATION
    assert result["answer"] == "I need one governed choice before I can answer this analytically."
    assert result["key_points"] == []
    assert result["execution_result"]["result_table"] is None


def test_answer_generation_summarizes_grouped_balance_results() -> None:
    result = governed_answer_generator.answer_from_message(
        message="Give me average balance by segment",
        selected_metric_id="metric.average_deposit_ledger_balance",
        selected_dimension_ids=["dimension.customer_segment"],
        user_role="technical_user",
    ).to_dict()

    assert result["status"] == AnswerStatus.ANSWERED
    assert "Average Deposit Ledger Balance by Customer Segment returned" in result["answer"]
    assert "highest" in result["answer"]
    assert len(result["key_points"]) >= 3
    assert result["result_overview"]["metric"] == "Average Deposit Ledger Balance"
    assert result["result_overview"]["highest"]["formatted_value"].startswith("$")


def test_answer_generation_summarizes_monthly_trend() -> None:
    result = governed_answer_generator.answer_from_message(
        message="Plot loan utilization by month",
        user_role="technical_user",
        limit=12,
    ).to_dict()

    assert result["status"] == AnswerStatus.ANSWERED
    assert result["result_overview"]["metric"] == "Loan Utilization Rate"
    assert result["result_overview"]["highest"]["formatted_value"].endswith("%")
    assert any("From 2025-" in point for point in result["key_points"])


def test_answer_generate_api_contract() -> None:
    response = client.post(
        "/answer/generate",
        json={
            "message": "Give me average balance by segment",
            "selected_metric_id": "metric.average_deposit_ledger_balance",
            "selected_dimension_ids": ["dimension.customer_segment"],
            "user_role": "technical_user",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == AnswerStatus.ANSWERED
    assert payload["execution_result"]["result_table"]["row_count"] > 0
    assert payload["key_points"]

