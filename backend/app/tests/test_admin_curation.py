from uuid import uuid4

from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def test_admin_curation_rejects_non_admin_requests() -> None:
    response = client.post(
        "/admin/curation/synonyms",
        json={
            "requested_by": "business_user",
            "synonym_id": f"synonym.blocked_{uuid4().hex[:8]}",
            "phrase": "blocked test phrase",
            "target_type": "dimension",
            "target_id": "dimension.customer_segment",
            "confidence": 0.9,
            "notes": "Should not be accepted.",
        },
    )

    assert response.status_code == 403
    assert "Admin curation" in response.json()["detail"]


def test_admin_curation_rejects_unknown_metric_columns() -> None:
    response = client.post(
        "/admin/curation/metrics",
        json={
            "requested_by": "admin",
            "metric_id": f"metric.invalid_column_{uuid4().hex[:8]}",
            "metric_name": "Invalid Governed Metric",
            "description": "A metric with an ungoverned source column.",
            "calculation_sql": "SUM(not_a_column)",
            "aggregation_type": "sum",
            "base_table": "fact_deposit_balance_daily",
            "required_columns": "not_a_column",
            "default_time_period": "month",
            "certified_flag": True,
            "subject_area": "Deposits",
            "notes": "Validation should reject the unknown column.",
        },
    )

    assert response.status_code == 400
    assert "not governed" in response.json()["detail"]


def test_admin_can_curate_business_term_and_refresh_search_index() -> None:
    suffix = uuid4().hex[:8]
    term_id = f"term.qa_operating_balance_{suffix}"
    term_name = f"QA Operating Balance {suffix}"

    response = client.post(
        "/admin/curation/business-terms",
        json={
            "requested_by": "admin",
            "term_id": term_id,
            "term_name": term_name,
            "term_type": "measure",
            "definition": "A QA-only governed definition for operating deposit balance.",
            "calculation": "Uses ledger_balance from the governed daily deposit balance fact.",
            "primary_table": "fact_deposit_balance_daily",
            "primary_column": "ledger_balance",
            "certified_flag": True,
            "owner": "Commercial Banking Data Governance",
            "subject_area": "Deposits",
            "notes": "Test curation event.",
        },
    )

    assert response.status_code == 200
    event = response.json()["event"]
    assert event["action"] == "create"
    assert event["asset_id"] == term_id
    assert event["after"]["term_name"] == term_name

    glossary = client.get("/glossary", params={"query": term_name})
    assert glossary.status_code == 200
    assert any(term["term_id"] == term_id for term in glossary.json()["terms"])

    search = client.get(
        "/metadata/search",
        params={"query": term_name, "document_type": "business_term", "limit": 3},
    )
    assert search.status_code == 200
    assert any(result["source_id"] == term_id for result in search.json()["results"])


def test_admin_can_curate_synonym_and_list_event_history() -> None:
    suffix = uuid4().hex[:8]
    synonym_id = f"synonym.qa_client_group_{suffix}"
    phrase = f"qa client group {suffix}"

    response = client.post(
        "/admin/curation/synonyms",
        json={
            "requested_by": "admin",
            "synonym_id": synonym_id,
            "phrase": phrase,
            "target_type": "dimension",
            "target_id": "dimension.customer_segment",
            "confidence": 0.97,
            "notes": "Maps QA client group wording to customer segment.",
        },
    )

    assert response.status_code == 200
    event = response.json()["event"]
    assert event["asset_type"] == "synonym"
    assert event["asset_id"] == synonym_id

    candidates = client.get("/metadata/candidates", params={"phrase": phrase})
    assert candidates.status_code == 200
    assert candidates.json()["candidates"][0]["target_id"] == "dimension.customer_segment"

    events = client.get("/admin/curation/events", params={"asset_id": synonym_id})
    assert events.status_code == 200
    payload = events.json()
    assert len(payload["events"]) == 1
    assert payload["events"][0]["asset_id"] == synonym_id
