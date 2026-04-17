from __future__ import annotations

from fastapi import APIRouter

from backend.app.schemas.semantic import SemanticResolutionRequest, SemanticResolutionResponse
from backend.app.semantic.resolver import semantic_resolver


router = APIRouter(tags=["semantic resolution"])


@router.post("/semantic/resolve", response_model=SemanticResolutionResponse)
def resolve_semantics(request: SemanticResolutionRequest) -> dict:
    result = semantic_resolver.resolve(
        message=request.message,
        intent=request.intent,
        selected_metric_id=request.selected_metric_id,
        selected_dimension_ids=request.selected_dimension_ids,
        awaiting_clarification=request.awaiting_clarification,
        context=request.context,
    )
    return result.to_dict()

