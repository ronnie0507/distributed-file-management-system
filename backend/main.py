import logging
import os
from time import perf_counter
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.exceptions import RedisError
from starlette.exceptions import HTTPException as StarletteHTTPException

try:
    from .api.rate_limit import RateLimiter, RedisRateLimiter, rate_policy
    from .api.routes import auth, files, health, nodes
    from .engines.access import access_engine
    from .observability import (
        HTTP_REQUESTS,
        HTTP_REQUEST_DURATION,
        configure_structured_logging,
        request_context_values,
        request_id_context,
    )
except ImportError:
    from api.rate_limit import RateLimiter, RedisRateLimiter, rate_policy
    from api.routes import auth, files, health, nodes
    from engines.access import access_engine
    from observability import (
        HTTP_REQUESTS,
        HTTP_REQUEST_DURATION,
        configure_structured_logging,
        request_context_values,
        request_id_context,
    )

_LOGGER = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(_app: FastAPI):
    configure_structured_logging()
    access_engine.validate_configuration()
    limiter = create_rate_limiter()
    _app.state.rate_limiter = limiter
    try:
        yield
    finally:
        await limiter.aclose()


def create_rate_limiter() -> RateLimiter:
    return RedisRateLimiter.from_env()


app = FastAPI(
    title="Distributed File Metadata Access and Management System",
    version="0.5.0",
    docs_url=None if os.getenv("APP_ENV", "development") == "production" else "/docs",
    redoc_url=None if os.getenv("APP_ENV", "development") == "production" else "/redoc",
    openapi_url=(
        None
        if os.getenv("APP_ENV", "development") == "production"
        else "/openapi.json"
    ),
    lifespan=lifespan,
)

_CORS_ORIGINS = tuple(
    origin.strip()
    for origin in os.getenv(
        "CORS_ALLOWED_ORIGINS",
        "http://localhost:3000,http://localhost:5173",
    ).split(",")
    if origin.strip()
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(_CORS_ORIGINS),
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
    expose_headers=["X-Request-ID"],
)

app.include_router(health.router)
app.include_router(auth.router)
app.include_router(files.router)
app.include_router(nodes.router)


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


@app.middleware("http")
async def request_and_rate_limit_middleware(request: Request, call_next):
    request_id, client_ip = request_context_values(request)
    context_token = request_id_context.set(request_id)
    started_at = perf_counter()
    policy = rate_policy(request.method, request.url.path)
    try:
        if policy is not None:
            limiter: RateLimiter | None = getattr(
                request.app.state,
                "rate_limiter",
                None,
            )
            if limiter is None:
                response = JSONResponse(
                    status_code=503,
                    content={
                        "error": {
                            "code": "RATE_LIMIT_UNAVAILABLE",
                            "message": "Request protection is temporarily unavailable.",
                            "path": request.url.path,
                        }
                    },
                )
            else:
                try:
                    allowed = await limiter.allow(
                        f"dfms:rate:{policy.name}:{client_ip or 'unknown'}",
                        limit=policy.limit,
                        window_seconds=policy.window_seconds,
                    )
                except (RedisError, OSError, TimeoutError):
                    _LOGGER.warning("rate_limit_backend_unavailable")
                    response = JSONResponse(
                        status_code=503,
                        content={
                            "error": {
                                "code": "RATE_LIMIT_UNAVAILABLE",
                                "message": (
                                    "Request protection is temporarily unavailable."
                                ),
                                "path": request.url.path,
                            }
                        },
                    )
                else:
                    if not allowed:
                        response = JSONResponse(
                            status_code=429,
                            headers={"Retry-After": str(policy.window_seconds)},
                            content={
                                "error": {
                                    "code": "RATE_LIMIT_EXCEEDED",
                                    "message": "Too many requests.",
                                    "path": request.url.path,
                                }
                            },
                        )
                    else:
                        response = await call_next(request)
        else:
            response = await call_next(request)

        response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Cache-Control"] = "no-store"
        if request.url.scheme == "https":
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )
        _LOGGER.info(
            "http_request_complete method=%s path=%s status=%s duration_ms=%d",
            request.method,
            request.url.path,
            response.status_code,
            int((perf_counter() - started_at) * 1000),
        )
        duration_seconds = (perf_counter() - started_at)
        HTTP_REQUESTS.labels(
            method=request.method,
            path=request.url.path,
            status=str(response.status_code),
        ).inc()
        HTTP_REQUEST_DURATION.labels(
            method=request.method,
            path=request.url.path,
        ).observe(duration_seconds)
        return response
    finally:
        request_id_context.reset(context_token)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(
    request: Request,
    exc: StarletteHTTPException,
) -> JSONResponse:
    detail = exc.detail
    if isinstance(detail, dict):
        code = detail.get("code", "HTTP_ERROR")
        message = detail.get("message", str(detail))
    else:
        code = "HTTP_ERROR"
        message = str(detail)

    return JSONResponse(
        status_code=exc.status_code,
        headers=exc.headers,
        content={
            "error": {
                "code": code,
                "message": message,
                "path": str(request.url.path),
            }
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "The request contains invalid fields.",
                "path": str(request.url.path),
                "details": [
                    {
                        key: value
                        for key, value in error.items()
                        if key != "input"
                    }
                    for error in exc.errors()
                ],
            }
        },
    )
