from __future__ import annotations

from fastapi import APIRouter

from backend.app.intent.classifier import intent_classifier
from backend.app.schemas.intent import IntentClassificationRequest, IntentClassificationResponse


router = APIRouter(tags=["intent classification"])


@router.post("/intent/classify", response_model=IntentClassificationResponse)
def classify_intent(request: IntentClassificationRequest) -> dict:
    result = intent_classifier.classify(
        message=request.message,
        awaiting_clarification=request.awaiting_clarification,
        context=request.context,
    )
    return result.to_dict()

