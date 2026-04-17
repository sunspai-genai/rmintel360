from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class ExportCreateRequest(BaseModel):
    chat_response: Dict[str, Any] = Field(..., description="Response payload returned by /chat/message.")
    export_format: Literal["json", "csv", "html"] = "html"
    user_role: str = "business_user"
    title: Optional[str] = None


class ExportCreateResponse(BaseModel):
    export_id: str
    created_at: str
    title: str
    export_format: str
    content_type: str
    filename: str
    conversation_id: Optional[str]
    row_count: Optional[int]
    chart_type: Optional[str]
    download_url: str
    view_url: str


class ExportRecord(BaseModel):
    export_id: str
    created_at: str
    title: str
    export_format: str
    content_type: str
    filename: str
    conversation_id: Optional[str]
    user_role: str
    source_message: Optional[str]
    row_count: Optional[int]
    chart_type: Optional[str]
    download_url: str
    view_url: str


class ExportListResponse(BaseModel):
    exports: List[ExportRecord]

