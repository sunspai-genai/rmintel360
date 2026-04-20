from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from backend.app.cache.service import assistant_response_cache
from backend.app.conversation.service import conversation_store
from backend.app.governance.audit import governance_audit_service
from backend.app.orchestration.llm_graph import llm_governed_assistant_graph
from backend.app.schemas.conversation import ConversationDetailResponse, ConversationListResponse
from backend.app.schemas.chat import ChatRequest, ChatResponse


router = APIRouter(tags=["assistant orchestration"])


@router.post("/chat/message", response_model=ChatResponse)
def chat_message(request: ChatRequest) -> dict:
    request_payload = request.model_dump()
    conversation_id = conversation_store.ensure_conversation(
        conversation_id=request.conversation_id,
        first_message=request.message,
        user_role=request.user_role,
    )
    response = llm_governed_assistant_graph.invoke(
        message=request.message,
        intent=request.intent,
        selected_metric_id=request.selected_metric_id,
        selected_dimension_ids=request.selected_dimension_ids,
        awaiting_clarification=request.awaiting_clarification,
        user_role=request.user_role,
        technical_mode=request.technical_mode,
        conversation_id=conversation_id,
        limit=request.limit,
        execute_sql=request.execute_sql,
    )
    request_payload["conversation_id"] = conversation_id
    response["conversation_id"] = conversation_id
    response["audit_report"] = governance_audit_service.build_from_chat_response(
        chat_response=response,
        user_role=request.user_role,
    )
    turn = conversation_store.record_turn(
        conversation_id=conversation_id,
        request_payload=request_payload,
        response_payload=response,
    )
    response["turn_id"] = turn["turn_id"]
    return response


@router.post("/chat/session/reset")
def reset_chat_session() -> dict:
    assistant_response_cache.clear()
    return {"status": "reset", "cache_cleared": True}


@router.get("/chat/conversations", response_model=ConversationListResponse)
def list_conversations(limit: int = Query(default=25, ge=1, le=100)) -> dict:
    return {"conversations": conversation_store.list_conversations(limit=limit)}


@router.get("/chat/conversations/{conversation_id}", response_model=ConversationDetailResponse)
def get_conversation(conversation_id: str) -> dict:
    conversation = conversation_store.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found.")
    return conversation
