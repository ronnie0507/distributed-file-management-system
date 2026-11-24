from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import jwt
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backend.database.base import Base
from backend.database.models import User
from backend.engines.access import AccessEngine


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    with session_factory() as session:
        yield session
    engine.dispose()


def create_user(db: Session, role: str = "viewer") -> User:
    user_id = uuid4()
    user = User(
        user_id=user_id,
        username=f"user-{user_id}",
        email=f"{user_id}@example.test",
        password_hash="not-used-in-this-test",
        role=role,
    )
    db.add(user)
    db.flush()
    return user


def test_registration_hashes_password_and_assigns_least_privilege(
    db_session: Session,
) -> None:
    engine = AccessEngine(secret_key="a" * 32)

    user = engine.register_user(
        db_session,
        username="  test-user ",
        email=" USER@example.test ",
        password="a-strong-password",
    )

    assert user.username == "test-user"
    assert user.email == "user@example.test"
    assert user.role == "viewer"
    assert user.password_hash != "a-strong-password"
    assert engine.verify_password("a-strong-password", user.password_hash)
    assert not engine.verify_password("wrong-password", user.password_hash)


def test_passwords_must_meet_minimum_length() -> None:
    with pytest.raises(ValueError, match="12 characters"):
        AccessEngine().hash_password("short")


def test_token_round_trip_and_signature_validation(db_session: Session) -> None:
    user = create_user(db_session)
    access = AccessEngine(secret_key="a" * 32)
    other_access = AccessEngine(secret_key="b" * 32)

    token = access.create_access_token(user.user_id)

    assert access.decode_access_token(token) == user.user_id
    with pytest.raises(jwt.InvalidTokenError):
        other_access.decode_access_token(token)


def test_expired_token_is_rejected() -> None:
    access = AccessEngine(
        secret_key="a" * 32,
        token_lifetime=timedelta(seconds=-1),
    )
    token = access.create_access_token(
        uuid4(),
        now=datetime.now(timezone.utc) - timedelta(minutes=2),
    )

    with pytest.raises(jwt.ExpiredSignatureError):
        access.decode_access_token(token)


def test_file_permission_roles_are_enforced(db_session: Session) -> None:
    from backend.database.models import FileMetadata

    owner = create_user(db_session, role="editor")
    viewer = create_user(db_session)
    file = FileMetadata(
        owner_id=owner.user_id,
        filename="shared.txt",
        checksum="a" * 64,
        file_size=4,
        current_version=1,
        upload_state="AVAILABLE",
    )
    db_session.add(file)
    db_session.flush()
    access = AccessEngine(secret_key="a" * 32)

    access.grant_file_permission(
        db_session,
        owner_id=owner.user_id,
        user_id=viewer.user_id,
        file_id=file.file_id,
        role="viewer",
    )

    assert access.can_access_file(
        db_session, user=viewer, file_id=file.file_id, required_role="viewer"
    )
    assert not access.can_access_file(
        db_session, user=viewer, file_id=file.file_id, required_role="editor"
    )
    assert access.can_access_file(
        db_session, user=owner, file_id=file.file_id, required_role="owner"
    )
