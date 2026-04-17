from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Response

from backend.app.exporting.service import export_service
from backend.app.schemas.export import ExportCreateRequest, ExportCreateResponse, ExportListResponse


router = APIRouter(tags=["exports"])


@router.post("/exports", response_model=ExportCreateResponse)
def create_export(request: ExportCreateRequest) -> dict:
    try:
        return export_service.create_export(
            chat_response=request.chat_response,
            export_format=request.export_format,
            user_role=request.user_role,
            title=request.title,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/exports", response_model=ExportListResponse)
def list_exports(
    conversation_id: Optional[str] = None,
    limit: int = Query(default=25, ge=1, le=100),
) -> dict:
    return {"exports": export_service.list_exports(conversation_id=conversation_id, limit=limit)}


@router.get("/exports/{export_id}")
def get_export(export_id: str) -> dict:
    record = export_service.get_export(export_id)
    if not record:
        raise HTTPException(status_code=404, detail="Export not found.")
    return {
        key: value
        for key, value in record.items()
        if key not in {"content"}
    }


@router.get("/exports/{export_id}/download")
def download_export(export_id: str) -> Response:
    record = _export_or_404(export_id)
    headers = {"Content-Disposition": f'attachment; filename="{record["filename"]}"'}
    return Response(content=record["content"], media_type=record["content_type"], headers=headers)


@router.get("/exports/{export_id}/view")
def view_export(export_id: str) -> Response:
    record = _export_or_404(export_id)
    return Response(content=record["content"], media_type=record["content_type"])


def _export_or_404(export_id: str) -> dict:
    record = export_service.get_export(export_id)
    if not record:
        raise HTTPException(status_code=404, detail="Export not found.")
    return record
