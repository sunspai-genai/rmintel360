from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.semantic.resolver import ResolutionStatus, semantic_resolver


client = TestClient(app)


def test_average_balance_by_segment_requires_clarification() -> None:
    result = semantic_resolver.resolve("Give me average balance by segment").to_dict()

    assert result["status"] == ResolutionStatus.NEEDS_CLARIFICATION
    assert result["requires_sql"] is False

    ambiguity_kinds = {ambiguity["kind"] for ambiguity in result["ambiguities"]}
    assert ambiguity_kinds == {"metric", "dimension"}

    metric_options = next(item for item in result["ambiguities"] if item["kind"] == "metric")["options"]
    dimension_options = next(item for item in result["ambiguities"] if item["kind"] == "dimension")["options"]

    assert {option["id"] for option in metric_options} == {
        "metric.average_deposit_ledger_balance",
        "metric.average_deposit_collected_balance",
        "metric.average_loan_outstanding_balance",
    }
    assert {option["id"] for option in dimension_options} == {
        "dimension.customer_segment",
        "dimension.product_segment",
    }
    assert all(option["table"] for option in metric_options + dimension_options)


def test_selected_metric_and_dimension_builds_governed_query_plan() -> None:
    result = semantic_resolver.resolve(
        "Give me average balance by segment",
        selected_metric_id="metric.average_deposit_ledger_balance",
        selected_dimension_ids=["dimension.customer_segment"],
    ).to_dict()

    assert result["status"] == ResolutionStatus.RESOLVED
    assert result["requires_sql"] is True

    query_plan = result["governed_query_plan"]
    assert query_plan["metric"]["id"] == "metric.average_deposit_ledger_balance"
    assert [dimension["id"] for dimension in query_plan["dimensions"]] == ["dimension.customer_segment"]
    assert [join["join_path_id"] for join in query_plan["joins"]] == [
        "join.deposit_balance_account",
        "join.account_customer",
    ]
    assert query_plan["filters"] == [{"filter_id": "latest_complete_month", "phrase": "default"}]
    assert result["assumptions"] == ["Used `latest_complete_month` because no time period was specified."]


def test_explicit_customer_segment_resolves_dimension_after_metric_selection() -> None:
    result = semantic_resolver.resolve(
        "Give me average balance by customer segment",
        selected_metric_id="metric.average_deposit_ledger_balance",
    ).to_dict()

    assert result["status"] == ResolutionStatus.RESOLVED
    assert result["governed_query_plan"]["dimensions"][0]["id"] == "dimension.customer_segment"


def test_loan_utilization_by_month_resolves_to_monthly_trend_plan() -> None:
    result = semantic_resolver.resolve("Plot loan utilization by month").to_dict()

    assert result["status"] == ResolutionStatus.RESOLVED
    assert result["intent"] == "chart_query"
    assert result["governed_query_plan"]["metric"]["id"] == "metric.loan_utilization_rate"
    assert [dimension["id"] for dimension in result["governed_query_plan"]["dimensions"]] == ["dimension.year_month"]
    assert [join["join_path_id"] for join in result["governed_query_plan"]["joins"]] == ["join.loan_balance_date"]
    assert result["governed_query_plan"]["filters"] == [
        {"filter_id": "last_12_months", "phrase": "monthly trend default"}
    ]


def test_loan_utilization_by_month_with_punctuation_uses_month_dimension() -> None:
    result = semantic_resolver.resolve("Plot loan utilization by month?").to_dict()

    assert result["status"] == ResolutionStatus.RESOLVED
    assert [dimension["id"] for dimension in result["governed_query_plan"]["dimensions"]] == ["dimension.year_month"]
    assert result["governed_query_plan"]["filters"] == [
        {"filter_id": "last_12_months", "phrase": "monthly trend default"}
    ]


def test_transaction_amount_by_channel_resolves_to_transaction_fact_dimension() -> None:
    result = semantic_resolver.resolve("Show total deposit transaction amount by channel.").to_dict()

    assert result["status"] == ResolutionStatus.RESOLVED
    assert result["governed_query_plan"]["metric"]["id"] == "metric.total_deposit_transaction_amount"
    assert [dimension["id"] for dimension in result["governed_query_plan"]["dimensions"]] == [
        "dimension.transaction_channel"
    ]
    assert result["governed_query_plan"]["joins"] == []


def test_cross_fact_metric_and_dimension_combination_requires_clarification() -> None:
    result = semantic_resolver.resolve(
        "Show total commercial deposits by channel",
        selected_metric_id="metric.total_commercial_deposits",
    ).to_dict()

    assert result["status"] == ResolutionStatus.NEEDS_CLARIFICATION
    assert [ambiguity["kind"] for ambiguity in result["ambiguities"]] == ["join_path"]


def test_restricted_customer_name_request_is_not_silently_dropped() -> None:
    result = semantic_resolver.resolve("List customer names with average deposit balance").to_dict()

    assert result["status"] == ResolutionStatus.NEEDS_CLARIFICATION
    assert result["requires_sql"] is False
    assert result["governed_query_plan"] is None

    ambiguity = result["ambiguities"][0]
    assert ambiguity["kind"] == "restricted_column"
    assert ambiguity["phrase"] == "Customer Name"
    assert {option["id"] for option in ambiguity["options"]} >= {
        "dimension.customer_segment",
        "dimension.industry",
    }


def test_information_question_skips_sql_semantic_resolution() -> None:
    result = semantic_resolver.resolve("What does average collected balance mean?").to_dict()

    assert result["status"] == ResolutionStatus.INFORMATION_ONLY
    assert result["requires_sql"] is False
    assert result["governed_query_plan"] is None


def test_semantic_resolve_api_contract() -> None:
    response = client.post(
        "/semantic/resolve",
        json={
            "message": "Give me average balance by segment",
            "selected_metric_id": "metric.average_deposit_ledger_balance",
            "selected_dimension_ids": ["dimension.customer_segment"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == ResolutionStatus.RESOLVED
    assert payload["governed_query_plan"]["metric"]["id"] == "metric.average_deposit_ledger_balance"
