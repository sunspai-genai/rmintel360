from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.app.feedback.service import feedback_service
from backend.app.schemas.feedback import (
    FeedbackCreateRequest,
    FeedbackCreateResponse,
    FeedbackListResponse,
    QualitySummaryResponse,
)


router = APIRouter(tags=["feedback quality monitoring"])


@router.post("/feedback", response_model=FeedbackCreateResponse)
def create_feedback(request: FeedbackCreateRequest) -> dict:
    try:
        return {"feedback": feedback_service.create_feedback(request.model_dump())}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/feedback", response_model=FeedbackListResponse)
def list_feedback(
    conversation_id: Optional[str] = None,
    turn_id: Optional[str] = None,
    rating: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    return {
        "feedback": feedback_service.list_feedback(
            conversation_id=conversation_id,
            turn_id=turn_id,
            rating=rating,
            limit=limit,
        )
    }


@router.get("/feedback/quality-summary", response_model=QualitySummaryResponse)
def quality_summary(limit: int = Query(default=5, ge=1, le=25)) -> dict:
    return feedback_service.quality_summary(limit=limit)
