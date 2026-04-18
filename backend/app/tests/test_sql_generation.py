from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.sql.service import SqlServiceStatus, governed_sql_service


client = TestClient(app)


def test_sql_generation_blocks_ambiguous_semantics() -> None:
    result = governed_sql_service.generate_from_message("Give me average balance by segment").to_dict()

    assert result["status"] == SqlServiceStatus.NEEDS_CLARIFICATION
    assert result["requires_clarification"] is True
    assert result["generated_sql"] is None
    assert result["validation"]["is_valid"] is False
    assert result["semantic_result"]["ambiguities"]


def test_selected_metric_dimension_generates_valid_sql() -> None:
    result = governed_sql_service.generate_from_message(
        message="Give me average balance by segment",
        selected_metric_id="metric.average_deposit_ledger_balance",
        selected_dimension_ids=["dimension.customer_segment"],
        user_role="technical_user",
    ).to_dict()

    assert result["status"] == SqlServiceStatus.GENERATED
    assert result["validation"]["is_valid"] is True
    assert result["sql_visible"] is True

    sql = result["generated_sql"]
    assert "AVG(fdb.ledger_balance) AS average_deposit_ledger_balance" in sql
    assert "INNER JOIN dim_account da ON fdb.account_id = da.account_id" in sql
    assert "INNER JOIN dim_customer dc ON da.customer_id = dc.customer_id" in sql
    assert "GROUP BY dc.customer_segment" in sql
    assert "latest_complete_month" not in sql


def test_chart_query_generates_last_12_months_sql() -> None:
    result = governed_sql_service.generate_from_message(
        message="Plot loan utilization by month",
        user_role="technical_user",
    ).to_dict()

    assert result["status"] == SqlServiceStatus.GENERATED
    assert result["validation"]["is_valid"] is True

    sql = result["generated_sql"]
    assert "SUM(flbm.outstanding_balance) / NULLIF(SUM(flbm.commitment_amount), 0)" in sql
    assert "INNER JOIN dim_date dd ON flbm.as_of_month = dd.calendar_date" in sql
    assert "INTERVAL 11 MONTH" in sql
    assert "GROUP BY dd.year_month" in sql


def test_chart_query_with_punctuation_still_groups_by_month() -> None:
    result = governed_sql_service.generate_from_message(
        message="Plot loan utilization by month?",
        user_role="technical_user",
    ).to_dict()

    assert result["status"] == SqlServiceStatus.GENERATED
    assert result["validation"]["is_valid"] is True

    sql = result["generated_sql"]
    assert "INNER JOIN dim_date dd ON flbm.as_of_month = dd.calendar_date" in sql
    assert "GROUP BY dd.year_month" in sql
    assert "INTERVAL 11 MONTH" in sql
    assert "flbm.as_of_month = (SELECT MAX(as_of_month)" not in sql


def test_transaction_amount_by_channel_generates_same_fact_grouping_sql() -> None:
    result = governed_sql_service.generate_from_message(
        message="Show total deposit transaction amount by channel.",
        user_role="technical_user",
    ).to_dict()

    assert result["status"] == SqlServiceStatus.GENERATED
    assert result["validation"]["is_valid"] is True

    sql = result["generated_sql"]
    assert "fdt.channel AS transaction_channel" in sql
    assert "SUM(fdt.amount) AS total_deposit_transaction_amount" in sql
    assert "FROM fact_deposit_transaction fdt" in sql
    assert "GROUP BY fdt.channel" in sql
    assert "JOIN" not in sql


def test_restricted_customer_name_request_does_not_generate_sql() -> None:
    result = governed_sql_service.generate_from_message(
        message="List customer names with average deposit balance",
        user_role="technical_user",
    ).to_dict()

    assert result["status"] == SqlServiceStatus.NEEDS_CLARIFICATION
    assert result["requires_clarification"] is True
    assert result["generated_sql"] is None
    assert result["semantic_result"]["ambiguities"][0]["kind"] == "restricted_column"


def test_business_user_gets_valid_sql_but_sql_text_is_hidden() -> None:
    result = governed_sql_service.generate_from_message(
        message="Give me average balance by segment",
        selected_metric_id="metric.average_deposit_ledger_balance",
        selected_dimension_ids=["dimension.customer_segment"],
    ).to_dict()

    assert result["status"] == SqlServiceStatus.GENERATED
    assert result["validation"]["is_valid"] is True
    assert result["sql_visible"] is False
    assert result["generated_sql"] is None


def test_sql_generate_api_contract() -> None:
    response = client.post(
        "/sql/generate",
        json={
            "message": "Give me average balance by segment",
            "selected_metric_id": "metric.average_deposit_ledger_balance",
            "selected_dimension_ids": ["dimension.customer_segment"],
            "user_role": "technical_user",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == SqlServiceStatus.GENERATED
    assert payload["validation"]["is_valid"] is True
    assert payload["generated_sql"].startswith("SELECT")
