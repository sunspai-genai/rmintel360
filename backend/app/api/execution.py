from __future__ import annotations

from fastapi import APIRouter

from backend.app.execution.service import governed_query_executor
from backend.app.schemas.execution import QueryExecutionRequest, QueryExecutionResponse


router = APIRouter(tags=["query execution"])


@router.post("/query/execute", response_model=QueryExecutionResponse)
def execute_query(request: QueryExecutionRequest) -> dict:
    result = governed_query_executor.execute_from_message(
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

