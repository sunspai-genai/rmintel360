from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field


FeedbackRating = Literal["positive", "negative", "neutral"]
FeedbackReasonCode = Literal[
    "helpful",
    "wrong_metric",
    "wrong_dimension",
    "wrong_sql",
    "unclear_answer",
    "bad_chart",
    "missing_context",
    "other",
]


class FeedbackCreateRequest(BaseModel):
    turn_id: str = Field(..., min_length=1)
    conversation_id: Optional[str] = None
    rating: FeedbackRating
    reason_code: Optional[FeedbackReasonCode] = None
    comment: Optional[str] = Field(default=None, max_length=1000)
    user_role: str = "business_user"


class FeedbackRecord(BaseModel):
    feedback_id: str
    created_at: str
    conversation_id: str
    turn_id: str
    turn_index: Optional[int]
    rating: FeedbackRating
    reason_code: FeedbackReasonCode
    comment: Optional[str]
    user_role: str
    status: Optional[str]
    intent: Optional[str]
    route: Optional[str]


class FeedbackCreateResponse(BaseModel):
    feedback: FeedbackRecord


class FeedbackListResponse(BaseModel):
    feedback: List[FeedbackRecord]


class QualitySummaryResponse(BaseModel):
    total_feedback: int
    positive_count: int
    negative_count: int
    neutral_count: int
    positive_rate: float
    issue_rate: float
    top_issue_reasons: List[Dict[str, object]]
    route_quality: List[Dict[str, object]]
    recent_feedback: List[FeedbackRecord]
