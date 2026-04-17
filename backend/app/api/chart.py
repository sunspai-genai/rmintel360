from __future__ import annotations

from fastapi import APIRouter

from backend.app.chart.service import governed_chart_generator
from backend.app.schemas.chart import ChartGenerationRequest, ChartGenerationResponse


router = APIRouter(tags=["chart generation"])


@router.post("/chart/generate", response_model=ChartGenerationResponse)
def generate_chart(request: ChartGenerationRequest) -> dict:
    result = governed_chart_generator.chart_from_message(
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
        chart_type=request.chart_type,
    )
    return result.to_dict()

