from functools import lru_cache

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

if __package__ == "backend.api":
    from ..database.models import User
    from ..database.session import get_db
    from ..engines.access import access_engine
    from ..engines.files import FileEngine, create_file_engine
    from ..engines.storage import StorageRegistry, create_storage_registry
else:
    from database.models import User
    from database.session import get_db
    from engines.access import access_engine
    from engines.files import FileEngine, create_file_engine
    from engines.storage import StorageRegistry, create_storage_registry


bearer_scheme = HTTPBearer(auto_error=False)


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    db: Session = Depends(get_db),
) -> User:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Bearer"},
            detail={
                "code": "UNAUTHORIZED",
                "message": "A Bearer access token is required.",
            },
        )
    try:
        user_id = access_engine.decode_access_token(credentials.credentials)
    except (jwt.InvalidTokenError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Bearer"},
            detail={
                "code": "INVALID_TOKEN",
                "message": "The access token is invalid or expired.",
            },
        ) from exc

    user = db.scalar(select(User).where(User.user_id == user_id))
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Bearer"},
            detail={
                "code": "INVALID_TOKEN",
                "message": "The access token no longer maps to an active user.",
            },
        )
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "code": "FORBIDDEN",
                "message": "Administrator role is required.",
            },
        )
    return user


def get_file_engine() -> FileEngine:
    return create_file_engine()


@lru_cache(maxsize=1)
def get_storage_registry() -> StorageRegistry:
    return create_storage_registry()
