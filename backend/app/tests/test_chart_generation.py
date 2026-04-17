from fastapi.testclient import TestClient

from backend.app.chart.service import ChartStatus, governed_chart_generator
from backend.app.main import app


client = TestClient(app)


def test_chart_generation_blocks_ambiguous_request() -> None:
    result = governed_chart_generator.chart_from_message("Plot average balance by segment").to_dict()

    assert result["status"] == ChartStatus.NEEDS_CLARIFICATION
    assert result["chart_spec"] is None
    assert result["answer_result"]["execution_result"]["result_table"] is None


def test_chart_generation_creates_line_chart_for_monthly_trend() -> None:
    result = governed_chart_generator.chart_from_message(
        message="Plot loan utilization by month",
        user_role="technical_user",
        limit=12,
    ).to_dict()

    assert result["status"] == ChartStatus.GENERATED
    chart = result["chart_spec"]
    assert chart["chart_type"] == "line"
    assert chart["x_axis"]["column"] == "year_month"
    assert chart["y_axis"]["column"] == "loan_utilization_rate"
    assert chart["plotly_json"]["data"][0]["type"] == "scatter"
    assert chart["data_summary"]["row_count"] == 12


def test_chart_generation_creates_bar_chart_for_grouped_categories() -> None:
    result = governed_chart_generator.chart_from_message(
        message="Show average balance by segment",
        selected_metric_id="metric.average_deposit_ledger_balance",
        selected_dimension_ids=["dimension.customer_segment"],
        user_role="technical_user",
    ).to_dict()

    assert result["status"] == ChartStatus.GENERATED
    chart = result["chart_spec"]
    assert chart["chart_type"] == "bar"
    assert chart["x_axis"]["column"] == "customer_segment"
    assert chart["plotly_json"]["data"][0]["type"] == "bar"


def test_chart_generate_api_contract() -> None:
    response = client.post(
        "/chart/generate",
        json={
            "message": "Plot loan utilization by month",
            "user_role": "technical_user",
            "limit": 12,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == ChartStatus.GENERATED
    assert payload["chart_spec"]["chart_type"] == "line"
    assert payload["answer_result"]["key_points"]

