from uuid import UUID

from sqlalchemy.orm import Session

if __package__ == "backend.engines":
    from ..database.models import AuditLog
else:
    from database.models import AuditLog


def record_audit_event(
    db: Session,
    *,
    action: str,
    resource_type: str,
    actor_user_id: UUID | None = None,
    resource_id: UUID | None = None,
    ip_address: str | None = None,
    request_id: str | None = None,
    details: dict | None = None,
) -> AuditLog:
    event_details = dict(details or {})
    if request_id:
        event_details["request_id"] = request_id
    event = AuditLog(
        actor_user_id=actor_user_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        ip_address=ip_address,
        details=event_details,
    )
    db.add(event)
    db.flush()
    return event
