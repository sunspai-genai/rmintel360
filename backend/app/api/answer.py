from __future__ import annotations

from fastapi import APIRouter

from backend.app.answer.service import governed_answer_generator
from backend.app.schemas.answer import AnalyticalAnswerRequest, AnalyticalAnswerResponse


router = APIRouter(tags=["analytical answers"])


@router.post("/answer/generate", response_model=AnalyticalAnswerResponse)
def generate_answer(request: AnalyticalAnswerRequest) -> dict:
    result = governed_answer_generator.answer_from_message(
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

