from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def test_chat_response_includes_turn_id_for_feedback() -> None:
    response = client.post(
        "/chat/message",
        json={
            "message": "What does average collected balance mean?",
            "user_role": "business_user",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["conversation_id"]
    assert payload["turn_id"]


def test_feedback_capture_list_and_quality_summary() -> None:
    chat = client.post(
        "/chat/message",
        json={
            "message": "Show average deposit ledger balance by customer segment",
            "selected_metric_id": "metric.average_deposit_ledger_balance",
            "selected_dimension_ids": ["dimension.customer_segment"],
            "user_role": "technical_user",
            "limit": 5,
        },
    ).json()

    positive = client.post(
        "/feedback",
        json={
            "conversation_id": chat["conversation_id"],
            "turn_id": chat["turn_id"],
            "rating": "positive",
            "reason_code": "helpful",
            "user_role": "technical_user",
        },
    )
    assert positive.status_code == 200
    assert positive.json()["feedback"]["turn_id"] == chat["turn_id"]

    negative = client.post(
        "/feedback",
        json={
            "conversation_id": chat["conversation_id"],
            "turn_id": chat["turn_id"],
            "rating": "negative",
            "reason_code": "wrong_metric",
            "comment": "QA signal for quality summary.",
            "user_role": "technical_user",
        },
    )
    assert negative.status_code == 200
    assert negative.json()["feedback"]["reason_code"] == "wrong_metric"

    listed = client.get("/feedback", params={"turn_id": chat["turn_id"]})
    assert listed.status_code == 200
    assert len(listed.json()["feedback"]) == 2

    summary = client.get("/feedback/quality-summary")
    assert summary.status_code == 200
    payload = summary.json()
    assert payload["total_feedback"] >= 2
    assert payload["positive_count"] >= 1
    assert payload["negative_count"] >= 1
    assert any(reason["reason_code"] == "wrong_metric" for reason in payload["top_issue_reasons"])


def test_feedback_rejects_unknown_turns() -> None:
    response = client.post(
        "/feedback",
        json={
            "turn_id": "turn_missing",
            "rating": "negative",
            "reason_code": "other",
            "comment": "Cannot attach to a missing turn.",
        },
    )

    assert response.status_code == 404
    assert "Unknown assistant turn" in response.json()["detail"]


def test_feedback_rejects_mismatched_conversation_id() -> None:
    chat = client.post(
        "/chat/message",
        json={
            "message": "What is customer segment?",
            "user_role": "business_user",
        },
    ).json()

    response = client.post(
        "/feedback",
        json={
            "conversation_id": "conv_wrong",
            "turn_id": chat["turn_id"],
            "rating": "neutral",
            "reason_code": "missing_context",
        },
    )

    assert response.status_code == 400
    assert "does not match" in response.json()["detail"]
