from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

if __package__ == "backend.api.routes":
    from ...api.dependencies import require_admin
    from ...database.models import NodeHealth, StorageNode, User
    from ...database.session import get_db
    from ...engines.distribution import distribution_engine
else:
    from api.dependencies import require_admin
    from database.models import NodeHealth, StorageNode, User
    from database.session import get_db
    from engines.distribution import distribution_engine


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
    return {
        "storage_nodes": [
            {
                "node_id": node.node_id,
                "node_name": node.node_name,
                "status": node.status,
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
