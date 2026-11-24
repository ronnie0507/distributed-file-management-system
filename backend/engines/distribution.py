import hashlib
from bisect import bisect_left
from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

if __package__ == "backend.engines":
    from ..database.models import NodeHealth, StorageNode
else:
    from database.models import NodeHealth, StorageNode


_NODE_STATUSES = {"healthy", "degraded", "unhealthy", "offline", "unknown"}


class InsufficientHealthyNodesError(ValueError):
    pass


class ConsistentHashRing:
    def __init__(
        self,
        node_ids: list[UUID],
        *,
        virtual_nodes: int = 64,
    ) -> None:
        if virtual_nodes < 1:
            raise ValueError("virtual_nodes must be positive")
        if len(set(node_ids)) != len(node_ids):
            raise ValueError("node_ids must be unique")
        if not node_ids:
            raise InsufficientHealthyNodesError("No healthy storage nodes are available")

        ring: list[tuple[int, UUID]] = []
        for node_id in node_ids:
            for index in range(virtual_nodes):
                token = self._hash(f"{node_id}:{index}")
                ring.append((token, node_id))
        ring.sort(key=lambda entry: entry[0])
        self._ring = ring
        self._tokens = [entry[0] for entry in ring]

    def select(self, key: str, replica_count: int) -> list[UUID]:
        if replica_count < 1:
            raise ValueError("replica_count must be positive")
        available_nodes = {node_id for _, node_id in self._ring}
        if replica_count > len(available_nodes):
            raise InsufficientHealthyNodesError(
                f"Requested {replica_count} replicas but only "
                f"{len(available_nodes)} healthy nodes are available"
            )

        start = bisect_left(self._tokens, self._hash(key))
        if start == len(self._tokens):
            start = 0

        selected: list[UUID] = []
        for offset in range(len(self._ring)):
            node_id = self._ring[(start + offset) % len(self._ring)][1]
            if node_id not in selected:
                selected.append(node_id)
                if len(selected) == replica_count:
                    return selected
        raise InsufficientHealthyNodesError("Could not select the requested replicas")

    @staticmethod
    def _hash(value: str) -> int:
        return int.from_bytes(hashlib.sha256(value.encode("utf-8")).digest(), "big")


class DistributionEngine:
    def register_node(
        self,
        db: Session,
        *,
        node_name: str,
        node_address: str,
        capacity_bytes: int | None,
    ) -> StorageNode:
        if capacity_bytes is not None and capacity_bytes < 0:
            raise ValueError("capacity_bytes must not be negative")
        node = StorageNode(
            node_name=node_name.strip(),
            node_address=node_address.strip(),
            capacity_bytes=capacity_bytes,
            used_bytes=0,
            status="unknown",
        )
        if not node.node_name or not node.node_address:
            raise ValueError("node_name and node_address must not be empty")
        db.add(node)
        db.flush()
        return node

    def record_heartbeat(
        self,
        db: Session,
        *,
        node_id: UUID,
        status: str,
        latency_ms: int | None,
        used_bytes: int | None,
    ) -> NodeHealth | None:
        if status not in _NODE_STATUSES:
            raise ValueError("status is not a supported storage-node state")
        if latency_ms is not None and latency_ms < 0:
            raise ValueError("latency_ms must not be negative")
        if used_bytes is not None and used_bytes < 0:
            raise ValueError("used_bytes must not be negative")

        node = db.get(StorageNode, node_id)
        if node is None:
            return None

        recorded_at = datetime.now(timezone.utc)
        node.status = status
        node.last_heartbeat_at = recorded_at
        if used_bytes is not None:
            node.used_bytes = used_bytes
        health = NodeHealth(
            node_id=node_id,
            status=status,
            latency_ms=latency_ms,
            used_bytes=used_bytes,
            recorded_at=recorded_at,
        )
        db.add(health)
        db.flush()
        return health

    def select_targets(
        self,
        db: Session,
        *,
        file_id: UUID,
        replica_count: int = 3,
        file_size_bytes: int = 0,
        exclude_node_ids: set[UUID] | None = None,
        health_ttl_seconds: int = 60,
    ) -> list[StorageNode]:
        if file_size_bytes < 0:
            raise ValueError("file_size_bytes must not be negative")
        if health_ttl_seconds < 1:
            raise ValueError("health_ttl_seconds must be positive")
        excluded = exclude_node_ids or set()
        healthy_after = datetime.now(timezone.utc) - timedelta(
            seconds=health_ttl_seconds
        )
        candidates = list(
            db.scalars(
                select(StorageNode)
                .where(
                    StorageNode.status == "healthy",
                    StorageNode.last_heartbeat_at >= healthy_after,
                    StorageNode.node_id.not_in(excluded),
                    (StorageNode.capacity_bytes.is_(None))
                    | (
                        (StorageNode.used_bytes < StorageNode.capacity_bytes)
                        & (
                            StorageNode.used_bytes + file_size_bytes
                            <= StorageNode.capacity_bytes
                        )
                    ),
                )
                .order_by(StorageNode.node_id)
            )
        )
        ring = ConsistentHashRing([node.node_id for node in candidates])
        target_ids = ring.select(str(file_id), replica_count)
        by_id = {node.node_id: node for node in candidates}
        return [by_id[node_id] for node_id in target_ids]


distribution_engine = DistributionEngine()
