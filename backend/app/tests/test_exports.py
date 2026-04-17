from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def _chat_payload() -> dict:
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
    return response.json()


def test_create_html_export_and_view_report() -> None:
    chat_payload = _chat_payload()
    response = client.post(
        "/exports",
        json={
            "chat_response": chat_payload,
            "export_format": "html",
            "user_role": "technical_user",
        },
    )

    assert response.status_code == 200
    export_record = response.json()
    assert export_record["export_format"] == "html"
    assert export_record["view_url"].endswith("/view")

    view = client.get(export_record["view_url"])
    assert view.status_code == 200
    assert "Loan Utilization Rate" in view.text
    assert "Governance Audit" in view.text
    assert "Plotly.newPlot" in view.text


def test_create_csv_export_and_download_result_table() -> None:
    chat_payload = _chat_payload()
    response = client.post(
        "/exports",
        json={
            "chat_response": chat_payload,
            "export_format": "csv",
            "user_role": "technical_user",
        },
    )

    assert response.status_code == 200
    export_record = response.json()
    download = client.get(export_record["download_url"])
    assert download.status_code == 200
    assert "year_month,loan_utilization_rate" in download.text
    assert "2025-04" in download.text


def test_exports_can_be_listed_by_conversation() -> None:
    chat_payload = _chat_payload()
    create = client.post(
        "/exports",
        json={
            "chat_response": chat_payload,
            "export_format": "json",
            "user_role": "technical_user",
        },
    ).json()

    response = client.get(
        "/exports",
        params={"conversation_id": chat_payload["conversation_id"], "limit": 10},
    )

    assert response.status_code == 200
    exports = response.json()["exports"]
    assert any(item["export_id"] == create["export_id"] for item in exports)

