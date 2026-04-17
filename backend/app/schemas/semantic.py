from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SemanticResolutionRequest(BaseModel):
    message: str = Field(..., min_length=1)
    intent: Optional[str] = None
    selected_metric_id: Optional[str] = None
    selected_dimension_ids: List[str] = Field(default_factory=list)
    awaiting_clarification: bool = False
    user_role: str = "business_user"
    technical_mode: bool = False
    conversation_id: Optional[str] = None
    context: Optional[Dict[str, Any]] = None


class SemanticResolutionResponse(BaseModel):
    message: str
    intent: str
    status: str
    requires_sql: bool
    confidence: float
    rationale: str
    ambiguities: List[Dict[str, Any]]
    resolved: Dict[str, Any]
    governed_query_plan: Optional[Dict[str, Any]]
    assumptions: List[str]
    retrieval_context: List[Dict[str, Any]]

