from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
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
    execute_sql: bool = True


class ChatResponse(BaseModel):
    message: str
    conversation_id: Optional[str]
    turn_id: Optional[str] = None
    status: str
    intent: Optional[str]
    route: Optional[str]
    answer: str
    next_action: str
    response_mode: Optional[str] = None
    requires_clarification: bool
    clarification_options: List[Dict[str, Any]]
    sql_visible: bool
    generated_sql: Optional[str]
    sql_summary: Optional[str]
    sql_validation: Optional[Dict[str, Any]]
    result_table: Optional[Dict[str, Any]]
    execution_ms: Optional[int]
    answer_summary: Optional[str]
    key_points: List[str]
    result_overview: Dict[str, Any]
    chart_spec: Optional[Dict[str, Any]]
    audit_report: Optional[Dict[str, Any]] = None
    semantic_result: Optional[Dict[str, Any]]
    metadata_context: List[Dict[str, Any]]
    source_citations: List[Dict[str, Any]] = Field(default_factory=list)
    llm_trace: Dict[str, Any] = Field(default_factory=dict)
    assumptions: List[str]
    warnings: List[str]
    graph_trace: List[str]
