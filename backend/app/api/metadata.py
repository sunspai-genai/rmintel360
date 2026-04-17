from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from backend.app.catalog.service import catalog_service


router = APIRouter(tags=["governed metadata"])


@router.get("/metadata/tables")
def list_tables(
    subject_area: Optional[str] = None,
    table_type: Optional[str] = None,
    certified: Optional[bool] = None,
) -> dict[str, Any]:
    tables = catalog_service.list_tables(
        subject_area=subject_area,
        table_type=table_type,
        certified=certified,
    )
    return {"count": len(tables), "tables": tables}


@router.get("/metadata/tables/{table_name}")
def get_table(table_name: str) -> dict[str, Any]:
    table = catalog_service.get_table(table_name)
    if table is None:
        raise HTTPException(status_code=404, detail=f"Unknown governed table: {table_name}")
    return table


@router.get("/metadata/columns")
def list_columns(
    table_name: Optional[str] = None,
    semantic_type: Optional[str] = None,
    include_restricted: bool = True,
) -> dict[str, Any]:
    columns = catalog_service.list_columns(
        table_name=table_name,
        semantic_type=semantic_type,
        include_restricted=include_restricted,
    )
    return {"count": len(columns), "columns": columns}


@router.get("/metadata/joins")
def list_join_paths(table_name: Optional[str] = None) -> dict[str, Any]:
    join_paths = catalog_service.list_join_paths(table_name=table_name)
    return {"count": len(join_paths), "join_paths": join_paths}


@router.get("/metadata/candidates")
def search_governed_candidates(
    phrase: str = Query(..., min_length=1),
    target_type: Optional[str] = None,
    min_confidence: float = 0.0,
    exact_match: bool = True,
) -> dict[str, Any]:
    candidates = catalog_service.search_governed_candidates(
        phrase=phrase,
        target_type=target_type,
        min_confidence=min_confidence,
        exact_match=exact_match,
    )
    return {
        "phrase": phrase,
        "target_type": target_type,
        "exact_match": exact_match,
        "count": len(candidates),
        "candidates": candidates,
    }


@router.get("/metadata/search")
def search_metadata(
    query: str = Query(..., min_length=1),
    document_type: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    min_score: float = 0.0,
) -> dict[str, Any]:
    results = catalog_service.search_metadata(
        query=query,
        document_type=document_type,
        limit=limit,
        min_score=min_score,
    )
    return {
        "query": query,
        "document_type": document_type,
        "count": len(results),
        "results": results,
    }


@router.get("/metadata/search/documents")
def list_search_documents(document_type: Optional[str] = None) -> dict[str, Any]:
    documents = catalog_service.list_search_documents(document_type=document_type)
    for document in documents:
        document.pop("token_vector_json", None)
    return {"count": len(documents), "documents": documents}


@router.get("/glossary")
def list_business_terms(
    query: Optional[str] = None,
    term_type: Optional[str] = None,
    certified: Optional[bool] = None,
) -> dict[str, Any]:
    terms = catalog_service.list_business_terms(
        query=query,
        term_type=term_type,
        certified=certified,
    )
    return {"count": len(terms), "terms": terms}


@router.get("/metrics")
def list_metrics(
    query: Optional[str] = None,
    subject_area: Optional[str] = None,
    certified: Optional[bool] = None,
) -> dict[str, Any]:
    metrics = catalog_service.list_metrics(
        query=query,
        subject_area=subject_area,
        certified=certified,
    )
    return {"count": len(metrics), "metrics": metrics}


@router.get("/metrics/{metric_id}")
def get_metric(metric_id: str) -> dict[str, Any]:
    metric = catalog_service.get_metric(metric_id)
    if metric is None:
        raise HTTPException(status_code=404, detail=f"Unknown governed metric: {metric_id}")
    return metric


@router.get("/dimensions")
def list_dimensions(
    query: Optional[str] = None,
    subject_area: Optional[str] = None,
    certified: Optional[bool] = None,
) -> dict[str, Any]:
    dimensions = catalog_service.list_dimensions(
        query=query,
        subject_area=subject_area,
        certified=certified,
    )
    return {"count": len(dimensions), "dimensions": dimensions}


@router.get("/dimensions/{dimension_id}")
def get_dimension(dimension_id: str) -> dict[str, Any]:
    dimension = catalog_service.get_dimension(dimension_id)
    if dimension is None:
        raise HTTPException(status_code=404, detail=f"Unknown governed dimension: {dimension_id}")
    return dimension


@router.get("/lineage")
def list_lineage(asset_name: Optional[str] = None) -> dict[str, Any]:
    lineage = catalog_service.list_lineage(asset_name=asset_name)
    return {"count": len(lineage), "lineage": lineage}


@router.get("/access-policies")
def list_access_policies(role_name: Optional[str] = None) -> dict[str, Any]:
    policies = catalog_service.list_access_policies(role_name=role_name)
    return {"count": len(policies), "policies": policies}
