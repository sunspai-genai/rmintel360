from uuid import uuid4

from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def test_chat_message_persists_conversation_and_turn() -> None:
    marker = uuid4().hex[:8]
    response = client.post(
        "/chat/message",
        json={
            "message": f"What does average collected balance mean? {marker}",
            "user_role": "business_user",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    conversation_id = payload["conversation_id"]
    assert conversation_id

    detail = client.get(f"/chat/conversations/{conversation_id}")

    assert detail.status_code == 200
    conversation = detail.json()
    assert conversation["conversation_id"] == conversation_id
    assert conversation["message_count"] == 1
    assert conversation["turns"][0]["request"]["message"].endswith(marker)
    assert conversation["turns"][0]["response"]["conversation_id"] == conversation_id


def test_chat_conversation_can_continue_across_turns() -> None:
    first = client.post(
        "/chat/message",
        json={
            "message": "What does loan utilization mean?",
            "user_role": "technical_user",
        },
    ).json()

    second = client.post(
        "/chat/message",
        json={
            "message": "Plot loan utilization by month",
            "conversation_id": first["conversation_id"],
            "user_role": "technical_user",
            "limit": 12,
        },
    )

    assert second.status_code == 200
    assert second.json()["conversation_id"] == first["conversation_id"]

    detail = client.get(f"/chat/conversations/{first['conversation_id']}").json()
    assert detail["message_count"] == 2
    assert [turn["turn_index"] for turn in detail["turns"]] == [1, 2]
    assert detail["turns"][1]["chart_type"] == "line"


def test_conversation_list_includes_recent_sessions() -> None:
    response = client.get("/chat/conversations", params={"limit": 10})

    assert response.status_code == 200
    payload = response.json()
    assert "conversations" in payload
    assert isinstance(payload["conversations"], list)

