from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backend.database.base import Base
from backend.engines.distribution import (
    ConsistentHashRing,
    DistributionEngine,
    InsufficientHealthyNodesError,
)


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    with session_factory() as session:
        yield session
    engine.dispose()


def test_ring_selection_is_deterministic_and_selects_distinct_nodes() -> None:
    node_ids = [uuid4() for _ in range(5)]
    ring = ConsistentHashRing(node_ids, virtual_nodes=32)

    first = ring.select("file-key", 3)
    second = ring.select("file-key", 3)

    assert first == second
    assert len(first) == len(set(first)) == 3
    assert set(first).issubset(node_ids)


def test_ring_rejects_more_replicas_than_nodes() -> None:
    ring = ConsistentHashRing([uuid4(), uuid4()])

    with pytest.raises(InsufficientHealthyNodesError):
        ring.select("file-key", 3)


def test_target_selection_uses_only_healthy_nodes_with_capacity(
    db_session: Session,
) -> None:
    distribution = DistributionEngine()
    healthy_nodes = [
        distribution.register_node(
            db_session,
            node_name=f"healthy-{index}",
            node_address=f"http://node-{index}",
            capacity_bytes=1000,
        )
        for index in range(3)
    ]
    for node in healthy_nodes:
        distribution.record_heartbeat(
            db_session,
            node_id=node.node_id,
            status="healthy",
            latency_ms=2,
            used_bytes=100,
        )
    full_node = distribution.register_node(
        db_session,
        node_name="full",
        node_address="http://full",
        capacity_bytes=100,
    )
    distribution.record_heartbeat(
        db_session,
        node_id=full_node.node_id,
        status="healthy",
        latency_ms=1,
        used_bytes=100,
    )
    db_session.flush()

    targets = distribution.select_targets(
        db_session,
        file_id=uuid4(),
        replica_count=3,
    )

    assert len(targets) == 3
    assert {node.node_id for node in targets} == {
        node.node_id for node in healthy_nodes
    }


def test_target_selection_accounts_for_requested_file_size(
    db_session: Session,
) -> None:
    distribution = DistributionEngine()
    usable = [
        distribution.register_node(
            db_session,
            node_name=f"usable-{index}",
            node_address=f"http://usable-{index}",
            capacity_bytes=100,
        )
        for index in range(2)
    ]
    nearly_full = distribution.register_node(
        db_session,
        node_name="nearly-full",
        node_address="http://nearly-full",
        capacity_bytes=100,
    )
    for node, used in [(usable[0], 10), (usable[1], 20), (nearly_full, 95)]:
        distribution.record_heartbeat(
            db_session,
            node_id=node.node_id,
            status="healthy",
            latency_ms=1,
            used_bytes=used,
        )
    db_session.flush()

    targets = distribution.select_targets(
        db_session,
        file_id=uuid4(),
        replica_count=2,
        file_size_bytes=10,
    )

    assert {node.node_id for node in targets} == {node.node_id for node in usable}


def test_target_selection_excludes_stale_heartbeats(db_session: Session) -> None:
    distribution = DistributionEngine()
    stale = distribution.register_node(
        db_session,
        node_name="stale",
        node_address="http://stale",
        capacity_bytes=100,
    )
    stale.last_heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    stale.status = "healthy"
    fresh = distribution.register_node(
        db_session,
        node_name="fresh",
        node_address="http://fresh",
        capacity_bytes=100,
    )
    distribution.record_heartbeat(
        db_session,
        node_id=fresh.node_id,
        status="healthy",
        latency_ms=1,
        used_bytes=0,
    )
    db_session.flush()

    targets = distribution.select_targets(
        db_session,
        file_id=uuid4(),
        replica_count=1,
    )

    assert [node.node_id for node in targets] == [fresh.node_id]


def test_unknown_node_heartbeat_is_not_created(db_session: Session) -> None:
    result = DistributionEngine().record_heartbeat(
        db_session,
        node_id=uuid4(),
        status="healthy",
        latency_ms=1,
        used_bytes=1,
    )

    assert result is None


@pytest.mark.parametrize(
    ("status", "latency_ms", "used_bytes"),
    [
        ("unexpected", 1, 1),
        ("healthy", -1, 1),
        ("healthy", 1, -1),
    ],
)
def test_invalid_heartbeat_data_is_rejected(
    db_session: Session,
    status: str,
    latency_ms: int,
    used_bytes: int,
) -> None:
    node = DistributionEngine().register_node(
        db_session,
        node_name="node",
        node_address="http://node",
        capacity_bytes=100,
    )

    with pytest.raises(ValueError):
        DistributionEngine().record_heartbeat(
            db_session,
            node_id=node.node_id,
            status=status,
            latency_ms=latency_ms,
            used_bytes=used_bytes,
        )
