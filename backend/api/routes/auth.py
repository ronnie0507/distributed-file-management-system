import os

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

if __package__ == "backend.api.routes":
    from ...database.session import get_db
    from ...database.models import User
    from ...engines.audit import record_audit_event
    from ...engines.access import access_engine
    from ...observability import request_context_values
    from ..dependencies import get_current_user
else:
    from database.session import get_db
    from database.models import User
    from engines.audit import record_audit_event
    from engines.access import access_engine
    from observability import request_context_values
    from api.dependencies import get_current_user


router = APIRouter(prefix="/api/auth", tags=["authentication"])
_REFRESH_COOKIE_NAME = "dfms_refresh"
_REFRESH_COOKIE_PATH = "/api/auth"
_REFRESH_COOKIE_MAX_AGE = 30 * 24 * 60 * 60


class RegisterRequest(BaseModel):
    username: str = Field(min_length=3, max_length=100)
    email: EmailStr
    password: str = Field(min_length=12, max_length=128)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=128)


class UserResponse(BaseModel):
    user_id: str
    username: str
    email: str
    role: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = 900


def _refresh_cookie_secure() -> bool:
    default = "true" if os.getenv("APP_ENV", "development").lower() == "production" else "false"
    return os.getenv("REFRESH_COOKIE_SECURE", default).lower() == "true"


def _set_refresh_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=_REFRESH_COOKIE_NAME,
        value=token,
        max_age=_REFRESH_COOKIE_MAX_AGE,
        httponly=True,
        secure=_refresh_cookie_secure(),
        samesite="strict",
        path=_REFRESH_COOKIE_PATH,
    )


def _clear_refresh_cookie(response: Response) -> None:
    response.delete_cookie(
        key=_REFRESH_COOKIE_NAME,
        httponly=True,
        secure=_refresh_cookie_secure(),
        samesite="strict",
        path=_REFRESH_COOKIE_PATH,
    )


@router.post(
    "/register",
    status_code=status.HTTP_201_CREATED,
    response_model=UserResponse,
)
def register(
    http_request: Request,
    payload: RegisterRequest,
    db: Session = Depends(get_db),
) -> UserResponse:
    try:
        user = access_engine.register_user(
            db,
            username=payload.username,
            email=str(payload.email),
            password=payload.password,
        )
        request_id, client_ip = request_context_values(http_request)
        record_audit_event(
            db,
            action="registration_succeeded",
            resource_type="user",
            actor_user_id=user.user_id,
            resource_id=user.user_id,
            ip_address=client_ip,
            request_id=request_id,
        )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        request_id, client_ip = request_context_values(http_request)
        record_audit_event(
            db,
            action="registration_failed",
            resource_type="authentication",
            ip_address=client_ip,
            request_id=request_id,
            details={"reason": "account_conflict"},
        )
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "ACCOUNT_ALREADY_EXISTS",
                "message": "The username or email is already registered.",
            },
        ) from exc
    return UserResponse(
        user_id=str(user.user_id),
        username=user.username,
        email=user.email,
        role=user.role,
    )


@router.post("/login", response_model=TokenResponse)
def login(
    http_request: Request,
    response: Response,
    payload: LoginRequest,
    db: Session = Depends(get_db),
) -> TokenResponse:
    user = access_engine.authenticate_user(
        db,
        email=str(payload.email),
        password=payload.password,
    )
    request_id, client_ip = request_context_values(http_request)
    if user is None:
        record_audit_event(
            db,
            action="login_failed",
            resource_type="authentication",
            ip_address=client_ip,
            request_id=request_id,
            details={"reason": "invalid_credentials"},
        )
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Bearer"},
            detail={
                "code": "INVALID_CREDENTIALS",
                "message": "Email or password is incorrect.",
            },
        )
    session, refresh_token = access_engine.create_refresh_session(
        db,
        user_id=user.user_id,
    )
    token = access_engine.create_access_token(
        user.user_id,
        session_id=session.session_id,
    )
    record_audit_event(
        db,
        action="login_succeeded",
        resource_type="authentication_session",
        actor_user_id=user.user_id,
        resource_id=session.session_id,
        ip_address=client_ip,
        request_id=request_id,
    )
    db.commit()
    _set_refresh_cookie(response, refresh_token)
    return TokenResponse(access_token=token)


@router.post("/refresh", response_model=TokenResponse)
def refresh(
    http_request: Request,
    response: Response,
    db: Session = Depends(get_db),
) -> TokenResponse:
    refresh_token = http_request.cookies.get(_REFRESH_COOKIE_NAME)
    request_id, client_ip = request_context_values(http_request)
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "INVALID_REFRESH_TOKEN", "message": "Refresh token is required."},
        )

    session, replacement_token, reused = access_engine.rotate_refresh_session(
        db,
        refresh_token=refresh_token,
    )
    if session is None or replacement_token is None:
        if reused:
            record_audit_event(
                db,
                action="refresh_token_reuse_detected",
                resource_type="authentication",
                ip_address=client_ip,
                request_id=request_id,
            )
        db.commit()
        _clear_refresh_cookie(response)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "INVALID_REFRESH_TOKEN",
                "message": "Refresh token is invalid, expired, or revoked.",
            },
        )

    record_audit_event(
        db,
        action="refresh_token_rotated",
        resource_type="authentication_session",
        actor_user_id=session.user_id,
        resource_id=session.session_id,
        ip_address=client_ip,
        request_id=request_id,
    )
    db.commit()
    _set_refresh_cookie(response, replacement_token)
    return TokenResponse(
        access_token=access_engine.create_access_token(
            session.user_id,
            session_id=session.session_id,
        )
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(
    http_request: Request,
    response: Response,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Response:
    refresh_token = http_request.cookies.get(_REFRESH_COOKIE_NAME)
    request_id, client_ip = request_context_values(http_request)
    if refresh_token:
        access_engine.revoke_refresh_session(db, refresh_token=refresh_token)
    record_audit_event(
        db,
        action="logout",
        resource_type="user",
        actor_user_id=user.user_id,
        resource_id=user.user_id,
        ip_address=client_ip,
        request_id=request_id,
    )
    db.commit()
    _clear_refresh_cookie(response)
    response.status_code = status.HTTP_204_NO_CONTENT
    return response


@router.get("/me", response_model=UserResponse)
def get_me(user: User = Depends(get_current_user)) -> UserResponse:
    return UserResponse(
        user_id=str(user.user_id),
        username=user.username,
        email=user.email,
        role=user.role,
    )
