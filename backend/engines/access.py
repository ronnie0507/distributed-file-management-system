import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from uuid import UUID, uuid4

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerificationError, VerifyMismatchError
from sqlalchemy import select, update
from sqlalchemy.orm import Session

if __package__ == "backend.engines":
    from ..database.models import FileMetadata, Permission, RefreshSession, User
else:
    from database.models import FileMetadata, Permission, RefreshSession, User


_TOKEN_ISSUER = "distributed-file-management"
_TOKEN_AUDIENCE = "dfms-api"
_PASSWORD_HASHER = PasswordHasher()
_FILE_ROLE_LEVEL = {"viewer": 1, "editor": 2, "owner": 3}
_REFRESH_TOKEN_LIFETIME = timedelta(days=30)


@dataclass(frozen=True)
class AccessTokenClaims:
    user_id: UUID
    session_id: UUID | None


class AccessEngine:
    def __init__(
        self,
        *,
        secret_key: str | None = None,
        token_lifetime: timedelta = timedelta(minutes=15),
        refresh_token_lifetime: timedelta = _REFRESH_TOKEN_LIFETIME,
    ) -> None:
        self._secret_key = secret_key
        self._token_lifetime = token_lifetime
        self._refresh_token_lifetime = refresh_token_lifetime

    def validate_configuration(self) -> None:
        self._configured_secret()
        previous = os.getenv("JWT_SECRET_KEY_PREVIOUS")
        if previous and previous == self._configured_secret():
            raise RuntimeError("JWT_SECRET_KEY and JWT_SECRET_KEY_PREVIOUS must differ")
        if (
            os.getenv("APP_ENV", "development").lower() == "production"
            and os.getenv("REFRESH_COOKIE_SECURE", "true").lower() != "true"
        ):
            raise RuntimeError("REFRESH_COOKIE_SECURE must be true in production")

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
        self,
        user_id: UUID,
        *,
        session_id: UUID | None = None,
        now: datetime | None = None,
    ) -> str:
        secret_key = self._configured_secret()
        issued_at = now or datetime.now(timezone.utc)
        payload = {
            "sub": str(user_id),
            "iss": _TOKEN_ISSUER,
            "aud": _TOKEN_AUDIENCE,
            "iat": issued_at,
            "exp": issued_at + self._token_lifetime,
        }
        if session_id is not None:
            payload["sid"] = str(session_id)
        return jwt.encode(
            payload,
            secret_key,
            algorithm="HS256",
            headers={"kid": "active"},
        )

    def decode_access_token(self, token: str) -> UUID:
        return self.decode_access_token_claims(token).user_id

    def decode_access_token_claims(self, token: str) -> AccessTokenClaims:
        claims = None
        for secret_key in self._verification_secrets():
            try:
                claims = jwt.decode(
                    token,
                    secret_key,
                    algorithms=["HS256"],
                    issuer=_TOKEN_ISSUER,
                    audience=_TOKEN_AUDIENCE,
                    options={"require": ["exp", "iat", "sub", "iss", "aud"]},
                    leeway=0,
                )
                break
            except jwt.InvalidSignatureError:
                continue
        if claims is None:
            raise jwt.InvalidSignatureError("Token signature is invalid")
        session_id = claims.get("sid")
        return AccessTokenClaims(
            user_id=UUID(claims["sub"]),
            session_id=UUID(session_id) if session_id else None,
        )

    def create_refresh_session(
        self,
        db: Session,
        *,
        user_id: UUID,
        now: datetime | None = None,
    ) -> tuple[RefreshSession, str]:
        current_time = now or datetime.now(timezone.utc)
        raw_token = secrets.token_urlsafe(48)
        session = RefreshSession(
            session_id=uuid4(),
            family_id=uuid4(),
            user_id=user_id,
            refresh_token_hash=self._hash_refresh_token(raw_token),
            created_at=current_time,
            expires_at=current_time + self._refresh_token_lifetime,
        )
        db.add(session)
        db.flush()
        return session, raw_token

    def rotate_refresh_session(
        self,
        db: Session,
        *,
        refresh_token: str,
        now: datetime | None = None,
    ) -> tuple[RefreshSession | None, str | None, bool]:
        current_time = now or datetime.now(timezone.utc)
        session = db.scalar(
            select(RefreshSession)
            .where(
                RefreshSession.refresh_token_hash
                == self._hash_refresh_token(refresh_token)
            )
            .with_for_update()
        )
        if session is None:
            return None, None, False
        if session.revoked_at is not None:
            db.execute(
                update(RefreshSession)
                .where(
                    RefreshSession.family_id == session.family_id,
                    RefreshSession.revoked_at.is_(None),
                )
                .values(revoked_at=current_time)
            )
            db.flush()
            return None, None, True
        expires_at = self._as_utc(session.expires_at)
        if expires_at <= current_time:
            session.revoked_at = current_time
            db.flush()
            return None, None, False

        raw_token = secrets.token_urlsafe(48)
        replacement = RefreshSession(
            session_id=uuid4(),
            family_id=session.family_id,
            user_id=session.user_id,
            refresh_token_hash=self._hash_refresh_token(raw_token),
            created_at=current_time,
            expires_at=current_time + self._refresh_token_lifetime,
        )
        session.revoked_at = current_time
        session.replaced_by_session_id = replacement.session_id
        db.add(replacement)
        db.flush()
        return replacement, raw_token, False

    def revoke_refresh_session(
        self,
        db: Session,
        *,
        refresh_token: str,
        now: datetime | None = None,
    ) -> bool:
        current_time = now or datetime.now(timezone.utc)
        session = db.scalar(
            select(RefreshSession)
            .where(
                RefreshSession.refresh_token_hash
                == self._hash_refresh_token(refresh_token)
            )
            .with_for_update()
        )
        if session is None:
            return False
        db.execute(
            update(RefreshSession)
            .where(
                RefreshSession.family_id == session.family_id,
                RefreshSession.revoked_at.is_(None),
            )
            .values(revoked_at=current_time)
        )
        db.flush()
        return True

    def active_session(
        self,
        db: Session,
        *,
        session_id: UUID,
        user_id: UUID,
        now: datetime | None = None,
    ) -> bool:
        current_time = now or datetime.now(timezone.utc)
        session = db.get(RefreshSession, session_id)
        if session is None or session.user_id != user_id or session.revoked_at is not None:
            return False
        return self._as_utc(session.expires_at) > current_time

    def _configured_secret(self) -> str:
        secret_key = self._secret_key or os.getenv("JWT_SECRET_KEY")
        if not secret_key or len(secret_key) < 32:
            raise RuntimeError("JWT_SECRET_KEY must contain at least 32 characters")
        if os.getenv("APP_ENV", "development").lower() == "production":
            lowered = secret_key.lower()
            if any(value in lowered for value in ("change-me", "replace-", "example")):
                raise RuntimeError("JWT_SECRET_KEY must not be a placeholder")
        return secret_key

    def _verification_secrets(self) -> tuple[str, ...]:
        active = self._configured_secret()
        previous = os.getenv("JWT_SECRET_KEY_PREVIOUS")
        if previous and len(previous) >= 32:
            return active, previous
        return (active,)

    @staticmethod
    def _hash_refresh_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value

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
