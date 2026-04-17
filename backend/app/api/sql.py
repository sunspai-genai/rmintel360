from __future__ import annotations

from fastapi import APIRouter

from backend.app.schemas.sql import SqlGenerationRequest, SqlGenerationResponse
from backend.app.sql.service import governed_sql_service


router = APIRouter(tags=["governed sql"])


@router.post("/sql/generate", response_model=SqlGenerationResponse)
def generate_sql(request: SqlGenerationRequest) -> dict:
    result = governed_sql_service.generate_from_message(
        message=request.message,
        intent=request.intent,
        selected_metric_id=request.selected_metric_id,
        selected_dimension_ids=request.selected_dimension_ids,
        awaiting_clarification=request.awaiting_clarification,
        user_role=request.user_role,
        technical_mode=request.technical_mode,
        context=request.context,
        governed_query_plan=request.governed_query_plan,
        limit=request.limit,
    )
    return result.to_dict()

