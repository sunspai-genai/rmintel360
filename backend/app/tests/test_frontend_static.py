from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def test_frontend_index_is_served() -> None:
    response = client.get("/")

    assert response.status_code == 200
    assert "CFG Enterprise Data Bot" in response.text
    assert "/assets/app.js" in response.text


def test_frontend_asset_is_served() -> None:
    response = client.get("/assets/app.js")

    assert response.status_code == 200
    assert "sendChat" in response.text
    assert "/chat/message" in response.text


def test_frontend_conversation_memory_is_session_scoped() -> None:
    response = client.get("/assets/app.js")

    assert response.status_code == 200
    assert "sessionStorage.getItem(\"bankingAssistantConversationId\")" in response.text
    assert "sessionStorage.removeItem(\"bankingAssistantConversationId\")" in response.text
    assert "/chat/session/reset" in response.text
    assert "localStorage" not in response.text
