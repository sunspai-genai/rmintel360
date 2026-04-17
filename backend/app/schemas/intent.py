from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class IntentClassificationRequest(BaseModel):
    message: str = Field(..., min_length=1)
    awaiting_clarification: bool = False
    user_role: str = "business_user"
    technical_mode: bool = False
    conversation_id: Optional[str] = None
    context: Optional[Dict[str, Any]] = None


class IntentClassificationResponse(BaseModel):
    message: str
    intent: str
    confidence: float
    requires_sql: bool
    route: str
    rationale: str
    signals: List[str]
    extracted_entities: Dict[str, Any]
    retrieval_context: List[Dict[str, Any]]

