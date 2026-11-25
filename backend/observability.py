import json
import logging
import os
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from fastapi import Request
from prometheus_client import Counter, Histogram


request_id_context: ContextVar[str] = ContextVar("request_id", default="-")

HTTP_REQUESTS = Counter(
    "dfms_http_requests_total",
    "Completed HTTP requests",
    ["method", "path", "status"],
)
HTTP_REQUEST_DURATION = Histogram(
    "dfms_http_request_duration_seconds",
    "HTTP request duration",
    ["method", "path"],
)


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "service": os.getenv("SERVICE_NAME", "dfms-api"),
            "environment": os.getenv("APP_ENV", "development"),
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": request_id_context.get(),
        }
        if record.exc_info:
            event["exception"] = self.formatException(record.exc_info)
        return json.dumps(event, separators=(",", ":"), ensure_ascii=True)


def configure_structured_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


def request_context_values(request: Request) -> tuple[str, str | None]:
    request_id_value = request.headers.get("x-request-id")
    try:
        request_id = str(UUID(request_id_value)) if request_id_value else str(uuid4())
    except ValueError:
        request_id = str(uuid4())
    request.state.request_id = request_id

    client_ip = request.headers.get("x-real-ip")
    if not client_ip and request.client is not None:
        client_ip = request.client.host
    request.state.client_ip = client_ip
    return request_id, client_ip
