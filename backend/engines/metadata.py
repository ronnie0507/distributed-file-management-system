import re
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

if __package__ == "backend.engines":
    from ..database.models import FileMetadata, FileVersion
else:
    from database.models import FileMetadata, FileVersion


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_UPLOAD_STATE_TRANSITIONS = {
    "PENDING": {"UPLOADED", "FAILED"},
    "UPLOADED": {"REPLICATING", "FAILED"},
    "REPLICATING": {"AVAILABLE", "FAILED"},
    "AVAILABLE": set(),
    "FAILED": {"PENDING"},
}


class InvalidUploadStateTransition(ValueError):
    pass


class MetadataEngine:
    def register_file(
        self,
        db: Session,
        *,
        owner_id: UUID,
        file_id: UUID | None = None,
        filename: str,
        checksum: str,
        file_size: int,
        mime_type: str | None = None,
    ) -> FileMetadata:
        normalized_filename = filename.strip()
        if not normalized_filename:
            raise ValueError("filename must not be empty")
        if file_size < 0:
            raise ValueError("file_size must not be negative")
        if not _SHA256_PATTERN.fullmatch(checksum):
            raise ValueError("checksum must be a lowercase SHA-256 digest")

        file = FileMetadata(
            file_id=file_id,
            owner_id=owner_id,
            filename=normalized_filename,
            checksum=checksum,
            file_size=file_size,
            mime_type=mime_type,
            current_version=1,
            upload_state="PENDING",
        )
        db.add(file)
        db.flush()
        db.add(
            FileVersion(
                file_id=file.file_id,
                version_number=1,
                checksum=checksum,
                file_size=file_size,
                created_by=owner_id,
            )
        )
        db.flush()
        return file

    def get_file(
        self, db: Session, *, owner_id: UUID, file_id: UUID
    ) -> FileMetadata | None:
        statement = select(FileMetadata).where(
            FileMetadata.file_id == file_id,
            FileMetadata.owner_id == owner_id,
            FileMetadata.deleted_at.is_(None),
        )
        return db.scalar(statement)

    def list_files(
        self,
        db: Session,
        *,
        owner_id: UUID,
        limit: int,
        offset: int,
    ) -> tuple[list[FileMetadata], int]:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        if offset < 0:
            raise ValueError("offset must not be negative")

        criteria = (
            FileMetadata.owner_id == owner_id,
            FileMetadata.deleted_at.is_(None),
        )
        total = db.scalar(
            select(func.count()).select_from(FileMetadata).where(*criteria)
        )
        statement = (
            select(FileMetadata)
            .where(*criteria)
            .order_by(FileMetadata.created_at.desc(), FileMetadata.file_id)
            .limit(limit)
            .offset(offset)
        )
        return list(db.scalars(statement)), total or 0

    def soft_delete(
        self, db: Session, *, owner_id: UUID, file_id: UUID
    ) -> bool:
        file = self.get_file(db, owner_id=owner_id, file_id=file_id)
        if file is None:
            return False
        file.deleted_at = datetime.now(timezone.utc)
        file.updated_at = datetime.now(timezone.utc)
        db.flush()
        return True

    def transition_upload_state(
        self,
        db: Session,
        *,
        owner_id: UUID,
        file_id: UUID,
        new_state: str,
    ) -> FileMetadata | None:
        file = self.get_file(db, owner_id=owner_id, file_id=file_id)
        if file is None:
            return None
        if new_state == file.upload_state:
            return file
        allowed = _UPLOAD_STATE_TRANSITIONS.get(file.upload_state, set())
        if new_state not in allowed:
            raise InvalidUploadStateTransition(
                f"Cannot transition upload from {file.upload_state} to {new_state}"
            )
        file.upload_state = new_state
        file.updated_at = datetime.now(timezone.utc)
        db.flush()
        return file


metadata_engine = MetadataEngine()
