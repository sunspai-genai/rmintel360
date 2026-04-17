from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def test_frontend_index_is_served() -> None:
    response = client.get("/")

    assert response.status_code == 200
    assert "Governed Assistant" in response.text
    assert "/assets/app.js" in response.text


def test_frontend_asset_is_served() -> None:
    response = client.get("/assets/app.js")

    assert response.status_code == 200
    assert "sendChat" in response.text
    assert "/chat/message" in response.text
