"""
backend/routes/documents.py — Document upload/listing endpoints (Task 33)

Routes:
  POST  /api/applications/{id}/documents  — multipart upload
  GET   /api/applications/{id}/documents  — list uploaded documents

Files are saved to:
    {DOCUMENTS_DIR}/{applicant_id}/{document_type}_{original_filename}

DOCUMENTS_DIR defaults to ./documents (relative to CWD).
The applicant_id is resolved from application_summary before saving.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import asyncpg
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from src.event_store import EventStore

logger = logging.getLogger(__name__)
router = APIRouter()

# Injected by backend/main.py lifespan
_store: EventStore | None = None
_pool: asyncpg.Pool | None = None

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ALLOWED_DOCUMENT_TYPES = {
    "income_statement",
    "balance_sheet",
    "application_proposal",
    "financial_summary",
}

_ALLOWED_EXTENSIONS = {".pdf", ".xlsx", ".csv"}

_DOCUMENTS_DIR = Path(os.environ.get("DOCUMENTS_DIR", "documents"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_pool() -> asyncpg.Pool:
    if _pool is None:
        raise HTTPException(status_code=503, detail="Database pool not ready")
    return _pool


async def _resolve_applicant_id(pool: asyncpg.Pool, application_id: str) -> str:
    """Look up applicant_id from application_summary projection."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT applicant_id FROM application_summary WHERE application_id = $1",
            application_id,
        )
    if row is None:
        raise HTTPException(status_code=404, detail=f"Application {application_id!r} not found")
    applicant_id = row["applicant_id"]
    if not applicant_id:
        raise HTTPException(status_code=422, detail="applicant_id not yet recorded in projection")
    return applicant_id


# ---------------------------------------------------------------------------
# POST /api/applications/{id}/documents
# ---------------------------------------------------------------------------

@router.post("/applications/{application_id}/documents", status_code=201)
async def upload_document(
    application_id: str,
    file: UploadFile = File(...),
    document_type: str = Form(...),
):
    """
    Upload a document for a loan application.

    - document_type must be one of: income_statement, balance_sheet,
      application_proposal, financial_summary
    - File extension must be .pdf, .xlsx, or .csv
    - Saved to {DOCUMENTS_DIR}/{applicant_id}/{document_type}_{filename}
    """
    # Validate document_type
    if document_type not in _ALLOWED_DOCUMENT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid document_type {document_type!r}. Must be one of: {sorted(_ALLOWED_DOCUMENT_TYPES)}",
        )

    # Validate file extension
    original_filename = file.filename or "upload"
    ext = Path(original_filename).suffix.lower()
    if ext not in _ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File extension {ext!r} not allowed. Accepted: {sorted(_ALLOWED_EXTENSIONS)}",
        )

    pool = _require_pool()
    applicant_id = await _resolve_applicant_id(pool, application_id)

    # Build destination path
    dest_dir = _DOCUMENTS_DIR / applicant_id
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest_filename = f"{document_type}_{original_filename}"
    dest_path = dest_dir / dest_filename

    # Write file
    contents = await file.read()
    dest_path.write_bytes(contents)

    size_bytes = len(contents)
    logger.info(
        "Saved document for application %s: %s (%d bytes)",
        application_id,
        dest_path,
        size_bytes,
    )

    return {
        "application_id": application_id,
        "applicant_id": applicant_id,
        "document_type": document_type,
        "filename": dest_filename,
        "size_bytes": size_bytes,
    }


# ---------------------------------------------------------------------------
# GET /api/applications/{id}/documents
# ---------------------------------------------------------------------------

@router.get("/applications/{application_id}/documents")
async def list_documents(application_id: str):
    """List uploaded documents for an application by scanning the applicant directory."""
    pool = _require_pool()
    applicant_id = await _resolve_applicant_id(pool, application_id)

    doc_dir = _DOCUMENTS_DIR / applicant_id
    if not doc_dir.exists():
        return []

    results: list[dict[str, Any]] = []
    for path in sorted(doc_dir.iterdir()):
        if not path.is_file():
            continue
        # Infer document_type from filename prefix (document_type_original_name)
        name = path.name
        doc_type = "unknown"
        for dt in _ALLOWED_DOCUMENT_TYPES:
            if name.startswith(f"{dt}_"):
                doc_type = dt
                break
        results.append(
            {
                "filename": name,
                "document_type": doc_type,
                "size_bytes": path.stat().st_size,
            }
        )

    return results
