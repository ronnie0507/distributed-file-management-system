from datetime import datetime, timedelta, timezone
import os
from urllib.parse import quote
from uuid import UUID, uuid4
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
    from ...api.dependencies import get_current_user, get_storage_registry
    from ...database.models import (
        FileMetadata,
        FileReplica,
        FileVersion,
        StorageNode,
        User,
    )
    from ...database.session import get_db
    from ...engines.access import access_engine
    from ...engines.distribution import (
        InsufficientHealthyNodesError,
        distribution_engine,
    )
    from ...engines.files import (
        ChecksumMismatchError,
        FileEngine,
        FileTooLargeError,
        MAX_FILE_SIZE_BYTES,
    )
    from ...engines.metadata import metadata_engine
    from ...engines.replication import replication_engine
    from ...engines.storage import StorageRegistry
else:
    from api.dependencies import get_current_user, get_storage_registry
    from database.models import FileMetadata, FileReplica, FileVersion, StorageNode, User
    from database.session import get_db
    from engines.access import access_engine
    from engines.distribution import InsufficientHealthyNodesError, distribution_engine
    from engines.files import ChecksumMismatchError, FileEngine, FileTooLargeError
    from engines.files import MAX_FILE_SIZE_BYTES
    from engines.metadata import metadata_engine
    from engines.replication import replication_engine
    from engines.storage import StorageRegistry


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
    availability_state: str
    replication_factor: int


class RestoreRequest(BaseModel):
    version_number: int = Field(ge=1)


@router.post("/upload", status_code=status.HTTP_201_CREATED)
def upload_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    storage: StorageRegistry = Depends(get_storage_registry),
) -> FileSummary:
    file_id = uuid4()
    filename = (file.filename or "").strip() or "unnamed"
    if file.size is not None and file.size > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail={
                "code": "FILE_TOO_LARGE",
                "message": f"File exceeds the {MAX_FILE_SIZE_BYTES}-byte limit",
            },
        )
    try:
        targets = distribution_engine.select_targets(
            db,
            file_id=file_id,
            replica_count=int(os.getenv("STORAGE_REPLICATION_FACTOR", "3")),
            file_size_bytes=file.size or 0,
        )
    except InsufficientHealthyNodesError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "STORAGE_NODES_UNAVAILABLE",
                "message": "There are not enough healthy storage nodes to accept this file.",
            },
        ) from exc

    try:
        primary_config = storage.for_node(targets[0])
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "STORAGE_CONFIGURATION_ERROR",
                "message": "The selected storage node is not trusted by this service.",
            },
        ) from exc

    try:
        file_engine = FileEngine(
            primary_config.object_store,
            bucket=primary_config.bucket,
        )
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
    except ChecksumMismatchError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "STORAGE_CHECKSUM_MISMATCH",
                "message": "Stored content failed checksum verification.",
            },
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
        record.replication_factor = len(targets)
        metadata_engine.transition_upload_state(
            db,
            owner_id=current_user.user_id,
            file_id=file_id,
            new_state="UPLOADED",
        )
        version = db.scalar(
            select(FileVersion).where(
                FileVersion.file_id == file_id,
                FileVersion.version_number == 1,
            )
        )
        if version is None:
            raise RuntimeError("The initial file version was not persisted")
        db.add(
            FileReplica(
                file_id=file_id,
                version_id=version.version_id,
                node_id=targets[0].node_id,
                object_key=stored.object_key,
                checksum=stored.checksum,
                status="VERIFIED",
                checksum_verified_at=datetime.now(timezone.utc),
            )
        )
        record.availability_state = "DEGRADED"
        replication_engine.enqueue(
            db,
            file_id=file_id,
            version_id=version.version_id,
            target_count=len(targets),
            idempotency_key=f"file:{file_id}:version:1:replicas:{len(targets)}",
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
    return FileSummary.model_validate(record)


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
) -> FileSummary:
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
    return FileSummary.model_validate(file)


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
    storage: StorageRegistry = Depends(get_storage_registry),
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
    version = db.scalar(
        select(FileVersion).where(
            FileVersion.file_id == file.file_id,
            FileVersion.version_number == file.current_version,
        )
    )
    if version is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "FILE_UNAVAILABLE",
                "message": "No current file version is available.",
            },
        )
    heartbeat_cutoff = datetime.now(timezone.utc) - timedelta(
        seconds=int(os.getenv("STORAGE_HEARTBEAT_TTL_SECONDS", "60"))
    )
    replicas = list(
        db.scalars(
            select(FileReplica)
            .join(StorageNode, StorageNode.node_id == FileReplica.node_id)
            .where(
                FileReplica.version_id == version.version_id,
                FileReplica.status == "VERIFIED",
                StorageNode.status == "healthy",
                StorageNode.last_heartbeat_at >= heartbeat_cutoff,
            )
            .order_by(FileReplica.checksum_verified_at, StorageNode.node_name)
        )
    )
    verified_content = None
    storage_failures: list[Exception] = []
    for replica in replicas:
        node = db.get(StorageNode, replica.node_id)
        if node is None:
            continue
        try:
            configured = storage.for_node(node)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "STORAGE_CONFIGURATION_ERROR",
                    "message": "A verified replica is not in the trusted storage configuration.",
                },
            ) from exc
        try:
            file_engine = FileEngine(
                configured.object_store,
                bucket=configured.bucket,
            )
            verified_content = file_engine.open_verified_content(
                replica.object_key,
                version.checksum,
            )
            break
        except ChecksumMismatchError as exc:
            replica.status = "CORRUPT"
            replica.error_message = str(exc)
            storage_failures.append(exc)
            replication_engine.refresh_availability(db, file=file)
            db.commit()
        except (HTTPError, S3Error, OSError) as exc:
            distribution_engine.record_heartbeat(
                db,
                node_id=node.node_id,
                status="offline",
                latency_ms=None,
                used_bytes=None,
            )
            storage_failures.append(exc)
            replication_engine.refresh_availability(db, file=file)
            db.commit()

    if verified_content is None:
        replication_engine.refresh_availability(db, file=file)
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "FILE_UNAVAILABLE",
                "message": "No verified healthy replica is currently readable.",
            },
        ) from (storage_failures[0] if storage_failures else None)

    def content_stream():
        try:
            while chunk := verified_content.read(1024 * 1024):
                yield chunk
        finally:
            verified_content.close()

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
