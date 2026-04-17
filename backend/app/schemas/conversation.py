from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ConversationSummary(BaseModel):
    conversation_id: str
    title: str
    created_at: str
    updated_at: str
    user_role: str
    message_count: int
    last_status: Optional[str]
    last_intent: Optional[str]
    last_message: Optional[str]


class ConversationTurn(BaseModel):
    turn_id: str
    conversation_id: str
    turn_index: int
    created_at: str
    user_message: str
    status: Optional[str]
    intent: Optional[str]
    route: Optional[str]
    answer: Optional[str]
    requires_clarification: bool
    generated_sql: Optional[str]
    chart_type: Optional[str]
    result_row_count: Optional[int]
    request: Dict[str, Any]
    response: Dict[str, Any]


class ConversationListResponse(BaseModel):
    conversations: List[ConversationSummary]


class ConversationDetailResponse(ConversationSummary):
    turns: List[ConversationTurn] = Field(default_factory=list)

