from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.app.curation.service import curation_service
from backend.app.schemas.curation import (
    BusinessTermUpsertRequest,
    CurationEventListResponse,
    CurationEventResponse,
    DimensionUpsertRequest,
    MetricUpsertRequest,
    SynonymUpsertRequest,
)


router = APIRouter(tags=["admin curation"])


@router.post("/admin/curation/business-terms", response_model=CurationEventResponse)
def upsert_business_term(request: BusinessTermUpsertRequest) -> dict:
    return _curate(lambda: curation_service.upsert_business_term(request.model_dump()))


@router.post("/admin/curation/metrics", response_model=CurationEventResponse)
def upsert_metric(request: MetricUpsertRequest) -> dict:
    return _curate(lambda: curation_service.upsert_metric(request.model_dump()))


@router.post("/admin/curation/dimensions", response_model=CurationEventResponse)
def upsert_dimension(request: DimensionUpsertRequest) -> dict:
    return _curate(lambda: curation_service.upsert_dimension(request.model_dump()))


@router.post("/admin/curation/synonyms", response_model=CurationEventResponse)
def upsert_synonym(request: SynonymUpsertRequest) -> dict:
    return _curate(lambda: curation_service.upsert_synonym(request.model_dump()))


@router.get("/admin/curation/events", response_model=CurationEventListResponse)
def list_curation_events(
    asset_type: Optional[str] = None,
    asset_id: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    return {"events": curation_service.list_events(asset_type=asset_type, asset_id=asset_id, limit=limit)}


def _curate(operation) -> dict:
    try:
        return {"event": operation()}
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
