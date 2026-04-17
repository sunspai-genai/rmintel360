from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class QueryExecutionRequest(BaseModel):
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


class QueryExecutionResponse(BaseModel):
    message: str
    status: str
    sql_result: Dict[str, Any]
    result_table: Optional[Dict[str, Any]]
    execution_ms: Optional[int]
    answer_summary: Optional[str]
    warnings: List[str]

