from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def test_list_governed_tables() -> None:
    response = client.get("/metadata/tables")

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] >= 12
    assert any(table["table_name"] == "fact_deposit_balance_daily" for table in payload["tables"])


def test_get_table_includes_columns_joins_and_lineage() -> None:
    response = client.get("/metadata/tables/fact_deposit_balance_daily")

    assert response.status_code == 200
    payload = response.json()
    assert payload["table_name"] == "fact_deposit_balance_daily"
    assert any(column["column_name"] == "ledger_balance" for column in payload["columns"])
    assert any(join_path["join_path_id"] == "join.deposit_balance_account" for join_path in payload["join_paths"])
    assert any(item["target_column"] == "ledger_balance" for item in payload["lineage"])


def test_average_balance_candidates_are_governed_and_ambiguous() -> None:
    response = client.get("/metadata/candidates", params={"phrase": "average balance"})

    assert response.status_code == 200
    payload = response.json()
    target_ids = {candidate["target_id"] for candidate in payload["candidates"]}

    assert payload["count"] == 3
    assert "metric.average_deposit_ledger_balance" in target_ids
    assert "metric.average_deposit_collected_balance" in target_ids
    assert "metric.average_loan_outstanding_balance" in target_ids


def test_segment_candidates_are_governed_and_focused_by_default() -> None:
    response = client.get("/metadata/candidates", params={"phrase": "segment"})

    assert response.status_code == 200
    payload = response.json()
    target_ids = {candidate["target_id"] for candidate in payload["candidates"]}

    assert payload["count"] == 2
    assert target_ids == {"dimension.customer_segment", "dimension.product_segment"}


def test_lineage_lookup_supports_qualified_asset_names() -> None:
    response = client.get(
        "/lineage",
        params={"asset_name": "fact_deposit_balance_daily.ledger_balance"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["lineage"][0]["source_system"] == "Core Deposit Platform"


def test_metadata_search_retrieves_metrics_and_dimensions() -> None:
    response = client.get(
        "/metadata/search",
        params={"query": "average balance by segment", "limit": 8},
    )

    assert response.status_code == 200
    payload = response.json()
    source_ids = {result["source_id"] for result in payload["results"]}

    assert "metric.average_deposit_ledger_balance" in source_ids
    assert "dimension.customer_segment" in source_ids


def test_metadata_search_boosts_lineage_for_source_questions() -> None:
    response = client.get(
        "/metadata/search",
        params={"query": "where does ledger balance come from", "limit": 3},
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload["results"][0]["document_type"] == "lineage"
    assert payload["results"][0]["source_id"] == "lineage.deposit_ledger_balance"


def test_search_documents_endpoint_lists_indexed_documents() -> None:
    response = client.get("/metadata/search/documents", params={"document_type": "metric"})

    assert response.status_code == 200
    payload = response.json()

    assert payload["count"] >= 10
    assert all("token_vector_json" not in document for document in payload["documents"])
    assert any(document["source_id"] == "metric.loan_utilization_rate" for document in payload["documents"])
