from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

if __package__ == "backend.engines":
    from ..database.models import OutboxEvent, ReplicationJob
else:
    from database.models import OutboxEvent, ReplicationJob


_PUBLISH_LEASE_SECONDS = 30


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


@dataclass(frozen=True)
class OutboxClaim:
    event_id: UUID
    job_id: UUID
    event_key: str
    payload: dict[str, Any]
    lock_token: UUID


class OutboxEngine:
    def __init__(self, *, lease_seconds: int = _PUBLISH_LEASE_SECONDS) -> None:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        self._lease_seconds = lease_seconds

    def enqueue_job(
        self,
        db: Session,
        *,
        job_id: UUID,
        job_type: str,
        idempotency_key: str,
        dispatch_generation: int,
        available_at: datetime | None = None,
    ) -> OutboxEvent:
        event = OutboxEvent(
            event_id=uuid4(),
            job_id=job_id,
            event_key=f"job:{job_id}:dispatch:{dispatch_generation}",
            event_type="background_job",
            payload={
                "job_id": str(job_id),
                "job_type": job_type,
                "idempotency_key": idempotency_key,
            },
            available_at=available_at or datetime.now(timezone.utc),
        )
        db.add(event)
        db.flush()
        return event

    def reconcile_jobs(
        self,
        db: Session,
        *,
        now: datetime | None = None,
        stale_after_seconds: int = 300,
    ) -> int:
        if stale_after_seconds < 1:
            raise ValueError("stale_after_seconds must be positive")
        current_time = now or datetime.now(timezone.utc)
        eligible = or_(
            ReplicationJob.status.in_(("PENDING", "RETRY"))
            & (ReplicationJob.available_at <= current_time),
            (ReplicationJob.status == "PROCESSING")
            & (ReplicationJob.locked_until <= current_time),
        )
        repaired = 0
        jobs = db.scalars(
            select(ReplicationJob)
            .where(eligible)
            .order_by(ReplicationJob.created_at, ReplicationJob.job_id)
        )
        for job in jobs:
            if job.attempt_count >= job.max_attempts:
                job.status = "DEAD"
                job.locked_until = None
                job.lock_token = None
                job.last_error = job.last_error or "Background job retry limit reached"
                job.dispatch_generation += 1
                self.enqueue_job(
                    db,
                    job_id=job.job_id,
                    job_type=job.job_type,
                    idempotency_key=job.idempotency_key,
                    dispatch_generation=job.dispatch_generation,
                    available_at=current_time,
                )
                repaired += 1
                continue
            current_key = (
                f"job:{job.job_id}:dispatch:{job.dispatch_generation}"
            )
            event = db.scalar(
                select(OutboxEvent)
                .where(OutboxEvent.event_key == current_key)
            )
            should_enqueue = event is None
            if (
                event is not None
                and event.status == "PUBLISHED"
                and event.published_at is not None
                and _as_utc(event.published_at)
                <= current_time - timedelta(seconds=stale_after_seconds)
            ):
                should_enqueue = True
            if should_enqueue:
                job.dispatch_generation += 1
                self.enqueue_job(
                    db,
                    job_id=job.job_id,
                    job_type=job.job_type,
                    idempotency_key=job.idempotency_key,
                    dispatch_generation=job.dispatch_generation,
                    available_at=job.available_at,
                )
                repaired += 1
        db.flush()
        return repaired

    def claim_next(
        self,
        db: Session,
        *,
        now: datetime | None = None,
    ) -> OutboxClaim | None:
        current_time = now or datetime.now(timezone.utc)
        eligible = or_(
            (OutboxEvent.status == "PENDING")
            & (OutboxEvent.available_at <= current_time),
            (OutboxEvent.status == "PUBLISHING")
            & (OutboxEvent.locked_until <= current_time),
        )
        event = db.scalar(
            select(OutboxEvent)
            .where(eligible)
            .order_by(OutboxEvent.created_at, OutboxEvent.event_id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if event is None:
            return None
        token = uuid4()
        event.status = "PUBLISHING"
        event.attempt_count += 1
        event.locked_until = current_time + timedelta(
            seconds=self._lease_seconds
        )
        event.lock_token = token
        db.flush()
        return OutboxClaim(
            event_id=event.event_id,
            job_id=event.job_id,
            event_key=event.event_key,
            payload=dict(event.payload),
            lock_token=token,
        )

    def mark_published(
        self,
        db: Session,
        *,
        claim: OutboxClaim,
        now: datetime | None = None,
    ) -> bool:
        current_time = now or datetime.now(timezone.utc)
        event = db.scalar(
            select(OutboxEvent)
            .where(OutboxEvent.event_id == claim.event_id)
            .with_for_update()
        )
        locked_until = (
            _as_utc(event.locked_until)
            if event is not None and event.locked_until is not None
            else None
        )
        if (
            event is None
            or event.status != "PUBLISHING"
            or event.lock_token != claim.lock_token
            or locked_until is None
            or locked_until <= current_time
        ):
            return False
        event.status = "PUBLISHED"
        event.published_at = current_time
        event.locked_until = None
        event.lock_token = None
        event.last_error = None
        db.flush()
        return True

    def release_for_retry(
        self,
        db: Session,
        *,
        claim: OutboxClaim,
        error: str,
        now: datetime | None = None,
    ) -> bool:
        current_time = now or datetime.now(timezone.utc)
        event = db.scalar(
            select(OutboxEvent)
            .where(OutboxEvent.event_id == claim.event_id)
            .with_for_update()
        )
        if (
            event is None
            or event.status != "PUBLISHING"
            or event.lock_token != claim.lock_token
        ):
            return False
        event.status = "PENDING"
        event.locked_until = None
        event.lock_token = None
        event.available_at = current_time + timedelta(
            seconds=min(2 ** max(event.attempt_count - 1, 0), 60)
        )
        event.last_error = error[:4000]
        db.flush()
        return True


outbox_engine = OutboxEngine()
