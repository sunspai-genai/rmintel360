from fastapi.testclient import TestClient

from backend.app.intent.classifier import IntentType, intent_classifier
from backend.app.main import app


client = TestClient(app)


def test_definition_question_routes_to_information_flow() -> None:
    result = intent_classifier.classify("What does average collected balance mean?").to_dict()

    assert result["intent"] == IntentType.DEFINITION_QUESTION
    assert result["requires_sql"] is False
    assert result["route"] == "information_flow"


def test_table_discovery_question_routes_to_information_flow() -> None:
    result = intent_classifier.classify("Which table has commercial loan balances?").to_dict()

    assert result["intent"] == IntentType.TABLE_DISCOVERY_QUESTION
    assert result["requires_sql"] is False
    assert result["route"] == "information_flow"


def test_lineage_question_routes_to_information_flow() -> None:
    result = intent_classifier.classify("Where does ledger balance come from?").to_dict()

    assert result["intent"] == IntentType.LINEAGE_QUESTION
    assert result["requires_sql"] is False
    assert result["route"] == "information_flow"
    assert result["retrieval_context"][0]["document_type"] == "lineage"


def test_transformation_question_routes_to_lineage_flow() -> None:
    result = intent_classifier.classify("How is relationship profit transformed into the reporting table?").to_dict()

    assert result["intent"] == IntentType.LINEAGE_QUESTION
    assert result["requires_sql"] is False
    assert result["route"] == "information_flow"


def test_analytical_question_requires_sql() -> None:
    result = intent_classifier.classify("Show average balance by segment").to_dict()

    assert result["intent"] == IntentType.ANALYTICAL_QUERY
    assert result["requires_sql"] is True
    assert result["route"] == "governed_analytics_flow"


def test_chart_question_requires_sql_and_chart_flow() -> None:
    result = intent_classifier.classify("Plot loan utilization by month").to_dict()

    assert result["intent"] == IntentType.CHART_QUERY
    assert result["requires_sql"] is True
    assert result["extracted_entities"]["chart_type"] == "auto"


def test_clarification_response_requires_waiting_context() -> None:
    result = intent_classifier.classify(
        "Use deposit ledger balance",
        awaiting_clarification=True,
    ).to_dict()

    assert result["intent"] == IntentType.CLARIFICATION_RESPONSE
    assert result["requires_sql"] is False
    assert result["route"] == "clarification_flow"


def test_unsupported_question_routes_to_unsupported_flow() -> None:
    result = intent_classifier.classify("What is the weather today?").to_dict()

    assert result["intent"] == IntentType.UNSUPPORTED
    assert result["requires_sql"] is False
    assert result["route"] == "unsupported_flow"


def test_intent_api_returns_classification_contract() -> None:
    response = client.post(
        "/intent/classify",
        json={"message": "Show average balance by segment"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == IntentType.ANALYTICAL_QUERY
    assert payload["requires_sql"] is True
    assert payload["route"] == "governed_analytics_flow"
    assert "metric_hints" in payload["extracted_entities"]
