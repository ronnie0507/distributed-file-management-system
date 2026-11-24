import os
from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

if __package__ == "backend.api.routes":
    from ...api.dependencies import require_admin
    from ...database.models import NodeHealth, ReplicationJob, StorageNode, User
    from ...database.session import get_db
    from ...engines.distribution import distribution_engine
    from ...engines.replication import replication_engine
else:
    from api.dependencies import require_admin
    from database.models import NodeHealth, ReplicationJob, StorageNode, User
    from database.session import get_db
    from engines.distribution import distribution_engine
    from engines.replication import replication_engine


router = APIRouter(prefix="/api/nodes", tags=["nodes"])


class RegisterNodeRequest(BaseModel):
    node_name: str = Field(min_length=1, max_length=100)
    node_address: str = Field(min_length=1)
    capacity_bytes: int | None = Field(default=None, ge=0)


class HeartbeatRequest(BaseModel):
    status: str
    latency_ms: int | None = Field(default=None, ge=0)
    used_bytes: int | None = Field(default=None, ge=0)


@router.get("/health")
def node_health(db: Session = Depends(get_db)) -> dict:
    nodes = db.scalars(select(StorageNode).order_by(StorageNode.node_name))
    heartbeat_cutoff = datetime.now(timezone.utc) - timedelta(
        seconds=int(os.getenv("STORAGE_HEARTBEAT_TTL_SECONDS", "60"))
    )
    def display_status(node: StorageNode) -> str:
        if node.last_heartbeat_at is None:
            return node.status
        last_heartbeat = node.last_heartbeat_at
        if last_heartbeat.tzinfo is None:
            last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)
        return (
            node.status
            if last_heartbeat >= heartbeat_cutoff
            else "offline"
        )

    return {
        "storage_nodes": [
            {
                "node_id": node.node_id,
                "node_name": node.node_name,
                "status": display_status(node),
                "capacity_bytes": node.capacity_bytes,
                "used_bytes": node.used_bytes,
                "last_heartbeat_at": node.last_heartbeat_at,
            }
            for node in nodes
        ]
    }


@router.post("", status_code=status.HTTP_201_CREATED)
def register_node(
    request: RegisterNodeRequest,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    try:
        node = distribution_engine.register_node(
            db,
            node_name=request.node_name,
            node_address=request.node_address,
            capacity_bytes=request.capacity_bytes,
        )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "NODE_ALREADY_EXISTS",
                "message": "The node name or address is already registered.",
            },
        ) from exc

    return {
        "node_id": node.node_id,
        "node_name": node.node_name,
        "status": node.status,
    }


@router.post("/{node_id}/heartbeat", status_code=status.HTTP_202_ACCEPTED)
def record_heartbeat(
    node_id: UUID,
    request: HeartbeatRequest,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    try:
        health: NodeHealth | None = distribution_engine.record_heartbeat(
            db,
            node_id=node_id,
            status=request.status,
            latency_ms=request.latency_ms,
            used_bytes=request.used_bytes,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "INVALID_HEARTBEAT", "message": str(exc)},
        ) from exc

    if health is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "NODE_NOT_FOUND", "message": "Storage node not found."},
        )
    db.commit()
    return {"node_id": node_id, "status": health.status, "recorded_at": health.recorded_at}


@router.post(
    "/replication-jobs/{job_id}/retry",
    status_code=status.HTTP_202_ACCEPTED,
)
def retry_replication_job(
    job_id: UUID,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    job: ReplicationJob | None = replication_engine.retry_dead_job(
        db,
        job_id=job_id,
    )
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "REPLICATION_JOB_NOT_RETRYABLE",
                "message": "The replication job does not exist or is not dead.",
            },
        )
    db.commit()
    return {
        "job_id": job.job_id,
        "status": job.status,
        "attempt_count": job.attempt_count,
    }
