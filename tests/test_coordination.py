from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.coordination.etcd import (
    CoordinationUnavailableError,
    EtcdConfig,
    EtcdCoordinator,
    EtcdEndpoint,
)


@dataclass
class FakeCompare:
    key: bytes
    operation: str
    expected: bytes | int | None = None

    def __eq__(self, expected: object) -> "FakeCompare":
        self.expected = expected
        return self


@dataclass(frozen=True)
class FakePut:
    key: bytes
    value: bytes
    lease_id: int


@dataclass(frozen=True)
class FakeDelete:
    key: bytes


class FakeTransactions:
    @staticmethod
    def create(key: bytes) -> FakeCompare:
        return FakeCompare(key, "create")

    @staticmethod
    def value(key: bytes) -> FakeCompare:
        return FakeCompare(key, "value")

    @staticmethod
    def put(key: bytes, value: bytes, *, lease) -> FakePut:
        return FakePut(key, value, lease.id)

    @staticmethod
    def delete(key: bytes) -> FakeDelete:
        return FakeDelete(key)


class FakeLease:
    def __init__(self, cluster: "FakeEtcdCluster", lease_id: int) -> None:
        self.cluster = cluster
        self.id = lease_id

    async def revoke(self) -> None:
        self.cluster.revoke(self.id)


class FakeEtcdCluster:
    def __init__(self) -> None:
        self.values: dict[bytes, tuple[bytes, int]] = {}
        self.leases: dict[int, int] = {}
        self.next_lease_id = 1
        self.unavailable = False

    def revoke(self, lease_id: int) -> None:
        self.leases.pop(lease_id, None)
        for key, (_value, current_lease_id) in list(self.values.items()):
            if current_lease_id == lease_id:
                self.values.pop(key, None)


class FakeEtcdClient:
    def __init__(self, cluster: FakeEtcdCluster, **_kwargs) -> None:
        self.cluster = cluster
        self.transactions = FakeTransactions()

    async def connect(self) -> None:
        if self.cluster.unavailable:
            raise OSError("etcd quorum unavailable")

    async def close(self) -> None:
        return None

    async def lease(self, _ttl: int) -> FakeLease:
        lease_id = self.cluster.next_lease_id
        self.cluster.next_lease_id += 1
        self.cluster.leases[lease_id] = 30
        return FakeLease(self.cluster, lease_id)

    async def refresh_lease(self, lease_id: int):
        if self.cluster.unavailable:
            raise OSError("etcd quorum unavailable")
        return SimpleNamespace(TTL=self.cluster.leases.get(lease_id, 0))

    async def put(self, key: bytes, value: bytes, *, lease) -> None:
        lease_id = lease.id if isinstance(lease, FakeLease) else lease
        self.cluster.values[key] = (value, lease_id)

    async def revoke_lease(self, lease_id: int) -> None:
        self.cluster.revoke(lease_id)

    async def transaction(self, *, compare, success, failure):
        comparison = compare[0]
        current = self.cluster.values.get(comparison.key)
        if comparison.operation == "create":
            succeeded = current is None
        else:
            succeeded = current is not None and current[0] == comparison.expected
        operations = success if succeeded else failure
        for operation in operations:
            if isinstance(operation, FakePut):
                self.cluster.values[operation.key] = (
                    operation.value,
                    operation.lease_id,
                )
            elif isinstance(operation, FakeDelete):
                self.cluster.values.pop(operation.key, None)
        return succeeded, []


def _coordinator(cluster: FakeEtcdCluster, worker_id=None) -> EtcdCoordinator:
    return EtcdCoordinator(
        EtcdConfig(
            endpoints=(
                EtcdEndpoint("etcd-1", 2379),
                EtcdEndpoint("etcd-2", 2379),
                EtcdEndpoint("etcd-3", 2379),
            ),
            username="dfms",
            password="test-secret",
        ),
        worker_id=worker_id or uuid4(),
        client_factory=lambda **kwargs: FakeEtcdClient(cluster, **kwargs),
    )


def test_storage_membership_uses_renewable_leased_registration() -> None:
    cluster = FakeEtcdCluster()
    coordinator = _coordinator(cluster)
    node_id = uuid4()
    try:
        lease_id = coordinator.register_membership(
            node_id=node_id,
            node_name="node-1",
            endpoint="minio-node-1:9000",
            status="healthy",
            capacity_bytes=1024,
        )
        renewed_lease_id = coordinator.register_membership(
            node_id=node_id,
            node_name="node-1",
            endpoint="minio-node-1:9000",
            status="offline",
            capacity_bytes=1024,
        )
    finally:
        coordinator.close()

    assert renewed_lease_id == lease_id
    key = next(key for key in cluster.values if str(node_id).encode() in key)
    assert b'"status":"offline"' in cluster.values[key][0]
    assert cluster.values[key][1] == lease_id


def test_expired_membership_lease_is_recreated() -> None:
    cluster = FakeEtcdCluster()
    coordinator = _coordinator(cluster)
    node_id = uuid4()
    try:
        first_lease = coordinator.register_membership(
            node_id=node_id,
            node_name="node-1",
            endpoint="minio-node-1:9000",
            status="healthy",
            capacity_bytes=1024,
        )
        cluster.revoke(first_lease)
        replacement_lease = coordinator.register_membership(
            node_id=node_id,
            node_name="node-1",
            endpoint="minio-node-1:9000",
            status="healthy",
            capacity_bytes=1024,
        )
    finally:
        coordinator.close()

    assert replacement_lease != first_lease


def test_recovery_leadership_is_exclusive_and_replaced_after_release() -> None:
    cluster = FakeEtcdCluster()
    first = _coordinator(cluster)
    second = _coordinator(cluster)
    try:
        assert first.try_acquire_recovery_leadership()
        assert not second.try_acquire_recovery_leadership()
        assert first.release_recovery_leadership()
        assert second.try_acquire_recovery_leadership()
    finally:
        first.close()
        second.close()


def test_expired_recovery_leader_lease_allows_takeover() -> None:
    cluster = FakeEtcdCluster()
    first = _coordinator(cluster)
    second = _coordinator(cluster)
    try:
        assert first.try_acquire_recovery_leadership()
        leader_lease_id = cluster.values[b"/dfms/elections/recovery-coordinator"][1]
        cluster.revoke(leader_lease_id)

        assert second.try_acquire_recovery_leadership()
        assert not first.try_acquire_recovery_leadership()
    finally:
        first.close()
        second.close()


def test_quorum_loss_reports_coordination_unavailable() -> None:
    cluster = FakeEtcdCluster()
    coordinator = _coordinator(cluster)
    cluster.unavailable = True
    try:
        with pytest.raises(CoordinationUnavailableError):
            coordinator.try_acquire_recovery_leadership()
    finally:
        coordinator.close()
