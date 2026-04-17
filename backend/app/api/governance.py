from __future__ import annotations

from fastapi import APIRouter

from backend.app.governance.audit import governance_audit_service
from backend.app.schemas.governance import GovernanceAuditRequest, GovernanceAuditResponse


router = APIRouter(tags=["governance audit"])


@router.post("/governance/audit", response_model=GovernanceAuditResponse)
def build_governance_audit(request: GovernanceAuditRequest) -> dict:
    return governance_audit_service.build_from_chat_response(
        chat_response=request.chat_response,
        user_role=request.user_role,
    )

