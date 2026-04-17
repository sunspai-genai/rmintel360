from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SqlGenerationRequest(BaseModel):
    message: str = Field(..., min_length=1)
    intent: Optional[str] = None
    selected_metric_id: Optional[str] = None
    selected_dimension_ids: List[str] = Field(default_factory=list)
    awaiting_clarification: bool = False
    user_role: str = "business_user"
    technical_mode: bool = False
    conversation_id: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    governed_query_plan: Optional[Dict[str, Any]] = None
    limit: int = Field(default=100, ge=1, le=500)


class SqlGenerationResponse(BaseModel):
    message: str
    status: str
    semantic_status: str
    requires_clarification: bool
    requires_sql: bool
    sql_visible: bool
    generated_sql: Optional[str]
    sql_summary: Optional[str]
    validation: Dict[str, Any]
    semantic_result: Dict[str, Any]
    governed_query_plan: Optional[Dict[str, Any]]
    assumptions: List[str]
    warnings: List[str]

