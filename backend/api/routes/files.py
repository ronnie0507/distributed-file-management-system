from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from pydantic import BaseModel, Field


router = APIRouter(prefix="/api/files", tags=["files"])


class RestoreRequest(BaseModel):
    version_number: int = Field(ge=1)


class UploadAccepted(BaseModel):
    file_id: str
    filename: str
    size_bytes: int
    content_type: str | None
    upload_state: str = "PENDING"


@router.post("/upload", status_code=202, response_model=UploadAccepted)
async def upload_file(file: UploadFile = File(...)) -> UploadAccepted:
    content = await file.read()
    if not content:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "EMPTY_FILE",
                "message": "A file must contain at least one byte.",
            },
        )

    return UploadAccepted(
        file_id=str(uuid4()),
        filename=file.filename or "unnamed",
        size_bytes=len(content),
        content_type=file.content_type,
    )


@router.get("")
async def list_files(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": [],
        "limit": limit,
        "offset": offset,
        "total": 0,
        "phase": "networking-placeholder",
    }


@router.get("/{file_id}")
async def get_file_metadata(file_id: str) -> dict:
    if len(file_id) != 36:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_FILE_ID",
                "message": "file_id must be a UUID.",
            },
        )
    return {
        "file_id": file_id,
        "upload_state": "PENDING",
        "phase": "networking-placeholder",
    }


@router.delete("/{file_id}", status_code=204)
async def delete_file(file_id: str) -> None:
    if len(file_id) != 36:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_FILE_ID",
                "message": "file_id must be a UUID.",
            },
        )
    return None


@router.get("/{file_id}/download")
async def download_file(file_id: str) -> dict:
    raise HTTPException(
        status_code=501,
        detail={
            "code": "NOT_IMPLEMENTED",
            "message": "File content retrieval starts in Phase 3.",
            "file_id": file_id,
        },
    )


@router.get("/{file_id}/versions")
async def list_versions(file_id: str) -> dict:
    return {
        "file_id": file_id,
        "items": [],
        "phase": "networking-placeholder",
    }


@router.post("/{file_id}/restore", status_code=202)
async def restore_version(file_id: str, request: RestoreRequest) -> dict:
    return {
        "file_id": file_id,
        "version_number": request.version_number,
        "status": "ACCEPTED",
        "accepted_at": datetime.now(timezone.utc).isoformat(),
        "phase": "networking-placeholder",
    }
