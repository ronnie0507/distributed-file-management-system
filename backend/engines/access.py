import os
from datetime import datetime, timedelta, timezone
from uuid import UUID

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerificationError, VerifyMismatchError
from sqlalchemy import select
from sqlalchemy.orm import Session

if __package__ == "backend.engines":
    from ..database.models import FileMetadata, Permission, User
else:
    from database.models import FileMetadata, Permission, User


_TOKEN_ISSUER = "distributed-file-management"
_PASSWORD_HASHER = PasswordHasher()
_FILE_ROLE_LEVEL = {"viewer": 1, "editor": 2, "owner": 3}


class AccessEngine:
    def __init__(
        self,
        *,
        secret_key: str | None = None,
        token_lifetime: timedelta = timedelta(hours=1),
    ) -> None:
        self._secret_key = secret_key
        self._token_lifetime = token_lifetime

    def validate_configuration(self) -> None:
        self._configured_secret()

    def hash_password(self, password: str) -> str:
        if len(password) < 12:
            raise ValueError("password must be at least 12 characters")
        return _PASSWORD_HASHER.hash(password)

    def verify_password(self, password: str, password_hash: str) -> bool:
        try:
            return _PASSWORD_HASHER.verify(password_hash, password)
        except (VerifyMismatchError, VerificationError):
            return False

    def register_user(
        self,
        db: Session,
        *,
        username: str,
        email: str,
        password: str,
    ) -> User:
        normalized_username = username.strip()
        normalized_email = email.strip().lower()
        if not normalized_username:
            raise ValueError("username must not be empty")

        user = User(
            username=normalized_username,
            email=normalized_email,
            password_hash=self.hash_password(password),
            role="viewer",
        )
        db.add(user)
        db.flush()
        return user

    def authenticate_user(
        self, db: Session, *, email: str, password: str
    ) -> User | None:
        user = db.scalar(
            select(User).where(User.email == email.strip().lower())
        )
        if user is None or not self.verify_password(password, user.password_hash):
            return None
        return user

    def create_access_token(
        self, user_id: UUID, *, now: datetime | None = None
    ) -> str:
        secret_key = self._configured_secret()
        issued_at = now or datetime.now(timezone.utc)
        payload = {
            "sub": str(user_id),
            "iss": _TOKEN_ISSUER,
            "iat": issued_at,
            "exp": issued_at + self._token_lifetime,
        }
        return jwt.encode(payload, secret_key, algorithm="HS256")

    def decode_access_token(
        self, token: str
    ) -> UUID:
        secret_key = self._configured_secret()
        claims = jwt.decode(
            token,
            secret_key,
            algorithms=["HS256"],
            issuer=_TOKEN_ISSUER,
            options={"require": ["exp", "iat", "sub", "iss"]},
            leeway=0,
        )
        return UUID(claims["sub"])

    def _configured_secret(self) -> str:
        secret_key = self._secret_key or os.getenv("JWT_SECRET_KEY")
        if not secret_key or len(secret_key) < 32:
            raise RuntimeError("JWT_SECRET_KEY must contain at least 32 characters")
        return secret_key

    def can_access_file(
        self,
        db: Session,
        *,
        user: User,
        file_id: UUID,
        required_role: str = "viewer",
    ) -> bool:
        if required_role not in _FILE_ROLE_LEVEL:
            raise ValueError(f"Unknown file role: {required_role}")

        file_owner_id = db.scalar(
            select(FileMetadata.owner_id).where(
                FileMetadata.file_id == file_id,
                FileMetadata.deleted_at.is_(None),
            )
        )
        if file_owner_id is None:
            return False
        if user.role == "admin" or file_owner_id == user.user_id:
            return True

        permission_role = db.scalar(
            select(Permission.role).where(
                Permission.user_id == user.user_id,
                Permission.file_id == file_id,
            )
        )
        return (
            permission_role in _FILE_ROLE_LEVEL
            and _FILE_ROLE_LEVEL[permission_role] >= _FILE_ROLE_LEVEL[required_role]
        )

    def grant_file_permission(
        self,
        db: Session,
        *,
        owner_id: UUID,
        user_id: UUID,
        file_id: UUID,
        role: str,
    ) -> Permission:
        if role not in {"editor", "viewer"}:
            raise ValueError("permission role must be editor or viewer")
        file = db.scalar(
            select(FileMetadata).where(
                FileMetadata.file_id == file_id,
                FileMetadata.owner_id == owner_id,
                FileMetadata.deleted_at.is_(None),
            )
        )
        if file is None:
            raise LookupError("file not found")

        permission = db.scalar(
            select(Permission).where(
                Permission.user_id == user_id,
                Permission.file_id == file_id,
            )
        )
        if permission is None:
            permission = Permission(
                user_id=user_id,
                file_id=file_id,
                role=role,
            )
            db.add(permission)
        else:
            permission.role = role
        db.flush()
        return permission


access_engine = AccessEngine()
