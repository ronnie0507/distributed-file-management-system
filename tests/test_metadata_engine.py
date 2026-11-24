from collections.abc import Generator
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backend.database.base import Base
from backend.database.models import FileVersion, User
from backend.engines.metadata import (
    InvalidUploadStateTransition,
    MetadataEngine,
)


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    with session_factory() as session:
        yield session
    engine.dispose()


@pytest.fixture
def owner_id(db_session: Session) -> UUID:
    user_id = uuid4()
    db_session.add(
        User(
            user_id=user_id,
            username=f"user-{user_id}",
            email=f"{user_id}@example.test",
            password_hash="test-hash",
        )
    )
    db_session.commit()
    return user_id


def test_register_file_creates_initial_version(
    db_session: Session, owner_id: UUID
) -> None:
    service = MetadataEngine()
    file = service.register_file(
        db_session,
        owner_id=owner_id,
        filename=" report.pdf ",
        checksum="a" * 64,
        file_size=123,
        mime_type="application/pdf",
    )

    version = db_session.query(FileVersion).one()
    assert file.filename == "report.pdf"
    assert file.upload_state == "PENDING"
    assert version.file_id == file.file_id
    assert version.version_number == 1
    assert version.created_by == owner_id


@pytest.mark.parametrize(
    ("filename", "checksum", "file_size"),
    [
        (" ", "a" * 64, 1),
        ("file.txt", "A" * 64, 1),
        ("file.txt", "a" * 63, 1),
        ("file.txt", "a" * 64, -1),
    ],
)
def test_register_file_rejects_invalid_metadata(
    db_session: Session,
    owner_id: UUID,
    filename: str,
    checksum: str,
    file_size: int,
) -> None:
    with pytest.raises(ValueError):
        MetadataEngine().register_file(
            db_session,
            owner_id=owner_id,
            filename=filename,
            checksum=checksum,
            file_size=file_size,
        )


def test_file_queries_are_owner_scoped_and_deleted_files_are_hidden(
    db_session: Session, owner_id: UUID
) -> None:
    service = MetadataEngine()
    file = service.register_file(
        db_session,
        owner_id=owner_id,
        filename="report.pdf",
        checksum="b" * 64,
        file_size=20,
    )
    other_owner = uuid4()

    assert service.get_file(
        db_session, owner_id=other_owner, file_id=file.file_id
    ) is None
    items, total = service.list_files(
        db_session, owner_id=owner_id, limit=20, offset=0
    )
    assert total == 1
    assert [item.file_id for item in items] == [file.file_id]

    assert service.soft_delete(
        db_session, owner_id=owner_id, file_id=file.file_id
    )
    assert service.get_file(
        db_session, owner_id=owner_id, file_id=file.file_id
    ) is None


def test_upload_state_transitions_reject_skipping_required_states(
    db_session: Session, owner_id: UUID
) -> None:
    service = MetadataEngine()
    file = service.register_file(
        db_session,
        owner_id=owner_id,
        filename="report.pdf",
        checksum="c" * 64,
        file_size=20,
    )

    with pytest.raises(InvalidUploadStateTransition):
        service.transition_upload_state(
            db_session,
            owner_id=owner_id,
            file_id=file.file_id,
            new_state="AVAILABLE",
        )

    for state in ("UPLOADED", "REPLICATING", "AVAILABLE"):
        service.transition_upload_state(
            db_session,
            owner_id=owner_id,
            file_id=file.file_id,
            new_state=state,
        )
    assert file.upload_state == "AVAILABLE"
