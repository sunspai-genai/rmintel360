from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def test_chat_response_includes_governance_audit_for_resolved_query() -> None:
    response = client.post(
        "/chat/message",
        json={
            "message": "Plot loan utilization by month",
            "user_role": "technical_user",
            "technical_mode": True,
            "limit": 12,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    audit = payload["audit_report"]
    assert audit["status"] == "available"
    assert audit["audit_summary"]["certified_metric"] is True
    assert audit["audit_summary"]["certified_dimensions"] is True
    assert {table["table_name"] for table in audit["source_tables"]} >= {
        "fact_loan_balance_monthly",
        "dim_date",
    }
    assert audit["approved_joins"][0]["join_path_id"] == "join.loan_balance_date"
    assert any(item["target_column"] == "outstanding_balance" for item in audit["lineage"])
    assert audit["access_controls"]["role_name"] == "technical_user"
    assert audit["access_controls"]["sql_visible"] is True


def test_chat_response_includes_metadata_only_audit_for_clarification() -> None:
    response = client.post(
        "/chat/message",
        json={
            "message": "Give me average balance by segment",
            "user_role": "business_user",
        },
    )

    assert response.status_code == 200
    audit = response.json()["audit_report"]
    assert audit["status"] == "metadata_only"
    assert audit["audit_summary"]["requires_clarification"] is True
    assert audit["lineage"] == []
    assert audit["retrieval_context"]


def test_governance_audit_endpoint_builds_report_from_chat_payload() -> None:
    chat_response = client.post(
        "/chat/message",
        json={
            "message": "Give me average balance by customer segment",
            "selected_metric_id": "metric.average_deposit_ledger_balance",
            "selected_dimension_ids": ["dimension.customer_segment"],
            "user_role": "technical_user",
        },
    ).json()

    response = client.post(
        "/governance/audit",
        json={"chat_response": chat_response, "user_role": "technical_user"},
    )

    assert response.status_code == 200
    audit = response.json()
    assert audit["status"] == "available"
    assert audit["resolved_assets"]["metric"]["id"] == "metric.average_deposit_ledger_balance"
    assert any(item["target_column"] == "customer_segment" for item in audit["lineage"])
    assert audit["sql_validation"]["is_valid"] is True

