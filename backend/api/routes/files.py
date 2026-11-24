from uuid import UUID, uuid4
from urllib.parse import quote
from urllib3.exceptions import HTTPError

from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Query,
    Response,
    UploadFile,
    status,
)
from fastapi.responses import StreamingResponse
from minio.error import S3Error
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

if __package__ == "backend.api.routes":
    from ...api.dependencies import get_current_user, get_file_engine
    from ...database.models import FileMetadata, FileVersion, User
    from ...database.session import get_db
    from ...engines.access import access_engine
    from ...engines.files import FileEngine, FileTooLargeError
    from ...engines.metadata import metadata_engine
else:
    from api.dependencies import get_current_user, get_file_engine
    from database.models import FileMetadata, FileVersion, User
    from database.session import get_db
    from engines.access import access_engine
    from engines.files import FileEngine, FileTooLargeError
    from engines.metadata import metadata_engine


router = APIRouter(prefix="/api/files", tags=["files"])


class FileSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    file_id: UUID
    filename: str
    checksum: str
    file_size: int
    mime_type: str | None
    current_version: int
    upload_state: str


class RestoreRequest(BaseModel):
    version_number: int = Field(ge=1)


@router.post("/upload", status_code=status.HTTP_201_CREATED)
def upload_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    file_engine: FileEngine = Depends(get_file_engine),
) -> FileSummary:
    file_id = uuid4()
    filename = (file.filename or "").strip() or "unnamed"
    try:
        stored = file_engine.store_content(
            file_id=file_id,
            version_number=1,
            source=file.file,
            content_type=file.content_type,
        )
    except FileTooLargeError as exc:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail={"code": "FILE_TOO_LARGE", "message": str(exc)},
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "INVALID_FILE", "message": str(exc)},
        ) from exc
    except (HTTPError, S3Error) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "OBJECT_STORAGE_UNAVAILABLE",
                "message": "File content could not be stored.",
            },
        ) from exc

    try:
        record = metadata_engine.register_file(
            db,
            file_id=file_id,
            owner_id=current_user.user_id,
            filename=filename,
            checksum=stored.checksum,
            file_size=stored.size_bytes,
            mime_type=stored.content_type,
        )
        metadata_engine.transition_upload_state(
            db,
            owner_id=current_user.user_id,
            file_id=file_id,
            new_state="UPLOADED",
        )
        db.commit()
    except SQLAlchemyError as database_error:
        db.rollback()
        try:
            file_engine.delete_content(stored.object_key)
        except (HTTPError, S3Error) as cleanup_error:
            raise ExceptionGroup(
                "Metadata commit failed and uploaded object cleanup also failed",
                [database_error, cleanup_error],
            ) from database_error
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "METADATA_UNAVAILABLE",
                "message": "File metadata could not be committed.",
            },
        ) from database_error
    return record


@router.get("")
def list_files(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    files, total = metadata_engine.list_files(
        db,
        owner_id=current_user.user_id,
        limit=limit,
        offset=offset,
    )
    return {
        "items": [FileSummary.model_validate(file) for file in files],
        "limit": limit,
        "offset": offset,
        "total": total,
    }


@router.get("/{file_id}", response_model=FileSummary)
def get_file_metadata(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> FileMetadata:
    file = db.scalar(
        select(FileMetadata).where(
            FileMetadata.file_id == file_id,
            FileMetadata.deleted_at.is_(None),
        )
    )
    if file is None or not access_engine.can_access_file(
        db,
        user=current_user,
        file_id=file_id,
        required_role="viewer",
    ):
        raise HTTPException(
            status_code=404,
            detail={"code": "FILE_NOT_FOUND", "message": "File not found."},
        )
    return file


@router.delete("/{file_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_file(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Response:
    if not metadata_engine.soft_delete(
        db,
        owner_id=current_user.user_id,
        file_id=file_id,
    ):
        raise HTTPException(
            status_code=404,
            detail={"code": "FILE_NOT_FOUND", "message": "File not found."},
        )
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/{file_id}/download")
def download_file(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    file_engine: FileEngine = Depends(get_file_engine),
) -> Response:
    if not access_engine.can_access_file(
        db, user=current_user, file_id=file_id, required_role="viewer"
    ):
        raise HTTPException(
            status_code=404,
            detail={"code": "FILE_NOT_FOUND", "message": "File not found."},
        )
    file = db.scalar(
        select(FileMetadata).where(
            FileMetadata.file_id == file_id,
            FileMetadata.deleted_at.is_(None),
        )
    )
    if file is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "FILE_NOT_FOUND", "message": "File not found."},
        )
    if file.upload_state != "UPLOADED":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "FILE_NOT_READY",
                "message": "The file is not available for download.",
            },
        )
    object_key = FileEngine.object_key(
        file_id=file.file_id,
        version_number=file.current_version,
        checksum=file.checksum,
    )
    try:
        stored_object = file_engine.open_content(object_key)
    except (HTTPError, S3Error) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "OBJECT_STORAGE_UNAVAILABLE",
                "message": "File content could not be retrieved.",
            },
        ) from exc

    def content_stream():
        try:
            while chunk := stored_object.read(1024 * 1024):
                yield chunk
        finally:
            try:
                stored_object.close()
            finally:
                stored_object.release_conn()

    return StreamingResponse(
        content_stream(),
        media_type=file.mime_type or "application/octet-stream",
        headers={
            "Content-Disposition": (
                f"attachment; filename*=UTF-8''{quote(file.filename)}"
            )
        },
    )


@router.get("/{file_id}/versions")
def list_versions(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    if not access_engine.can_access_file(
        db, user=current_user, file_id=file_id, required_role="viewer"
    ):
        raise HTTPException(
            status_code=404,
            detail={"code": "FILE_NOT_FOUND", "message": "File not found."},
        )
    versions = db.scalars(
        select(FileVersion)
        .where(FileVersion.file_id == file_id)
        .order_by(FileVersion.version_number)
    )
    return {
        "file_id": file_id,
        "items": [
            {
                "version_id": version.version_id,
                "version_number": version.version_number,
                "checksum": version.checksum,
                "file_size": version.file_size,
                "created_at": version.created_at,
            }
            for version in versions
        ],
    }


@router.post("/{file_id}/restore", status_code=status.HTTP_501_NOT_IMPLEMENTED)
def restore_version(
    file_id: UUID,
    request: RestoreRequest,
    _current_user: User = Depends(get_current_user),
) -> None:
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail={
            "code": "RESTORE_WORKFLOW_NOT_READY",
            "message": "Version restoration is not implemented yet.",
        },
    )
