from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.app.api.answer import router as answer_router
from backend.app.api.chat import router as chat_router
from backend.app.api.chart import router as chart_router
from backend.app.api.curation import router as curation_router
from backend.app.api.execution import router as execution_router
from backend.app.api.export import router as export_router
from backend.app.api.feedback import router as feedback_router
from backend.app.api.governance import router as governance_router
from backend.app.api.intent import router as intent_router
from backend.app.api.metadata import router as metadata_router
from backend.app.api.semantic import router as semantic_router
from backend.app.api.sql import router as sql_router
from backend.app.core.config import get_settings
from backend.app.db.connection import database_exists, get_duckdb_path, table_counts


settings = get_settings()
PROJECT_ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DIR = PROJECT_ROOT / "frontend"

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Backend API for the governed commercial banking analytics assistant MVP.",
)
app.include_router(metadata_router)
app.include_router(intent_router)
app.include_router(semantic_router)
app.include_router(sql_router)
app.include_router(chat_router)
app.include_router(execution_router)
app.include_router(answer_router)
app.include_router(chart_router)
app.include_router(governance_router)
app.include_router(export_router)
app.include_router(curation_router)
app.include_router(feedback_router)
app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")


@app.get("/")
def frontend() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "app_name": settings.app_name,
        "version": settings.app_version,
        "database_path": str(get_duckdb_path()),
        "database_exists": database_exists(),
    }


@app.get("/data/profile")
def data_profile() -> dict[str, object]:
    return {
        "database_path": str(get_duckdb_path()),
        "database_exists": database_exists(),
        "tables": table_counts(),
    }
