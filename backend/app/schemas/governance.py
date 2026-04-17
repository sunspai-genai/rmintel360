from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class GovernanceAuditRequest(BaseModel):
    chat_response: Dict[str, Any] = Field(..., description="Response payload returned by /chat/message.")
    user_role: str = "business_user"


class GovernanceAuditResponse(BaseModel):
    status: str
    audit_summary: Dict[str, Any]
    resolved_assets: Dict[str, Any]
    source_tables: list[Dict[str, Any]]
    approved_joins: list[Dict[str, Any]]
    lineage: list[Dict[str, Any]]
    access_controls: Dict[str, Any]
    sql_validation: Optional[Dict[str, Any]]
    retrieval_context: list[Dict[str, Any]]
    assumptions: list[str]
    warnings: list[str]
    graph_trace: list[str]
