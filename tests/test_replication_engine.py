import hashlib
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import BinaryIO
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker

from backend.database.base import Base
from backend.database.models import (
    FileMetadata,
    FileReplica,
    FileVersion,
    ReplicationJob,
    StorageNode,
    User,
)
from backend.engines.distribution import DistributionEngine
from backend.engines.files import FileEngine
from backend.engines.metadata import metadata_engine
from backend.engines.replication import ReplicationEngine
from backend.engines.storage import ConfiguredStorageNode, StorageRegistry
from backend.workers.replication_worker import ReplicationWorker


CONTENT = b"durable verified replication"


class MemoryObjectReader(BytesIO):
    def release_conn(self) -> None:
        return None


class MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.fail_put = False

    def put_object(
        self,
        *,
        bucket: str,
        object_key: str,
        data: BinaryIO,
        size_bytes: int,
        content_type: str,
        checksum: str,
    ) -> None:
        if self.fail_put:
            raise OSError("temporary storage failure")
        content = data.read()
        assert len(content) == size_bytes
        self.objects[f"{bucket}/{object_key}"] = content
        assert content_type
        assert checksum

    def get_object(self, *, bucket: str, object_key: str) -> MemoryObjectReader:
        try:
            return MemoryObjectReader(self.objects[f"{bucket}/{object_key}"])
        except KeyError as exc:
            raise OSError("object is missing") from exc

    def remove_object(self, *, bucket: str, object_key: str) -> None:
        self.objects.pop(f"{bucket}/{object_key}", None)

    def health_check(self) -> None:
        return None

    def used_bytes(self, bucket: str) -> int:
        prefix = f"{bucket}/"
        return sum(
            len(content)
            for key, content in self.objects.items()
            if key.startswith(prefix)
        )


@dataclass
class ReplicationEnvironment:
    db: Session
    session_factory: sessionmaker[Session]
    distribution: DistributionEngine
    replication: ReplicationEngine
    file: FileMetadata
    nodes: list[StorageNode]
    stores: dict[str, MemoryObjectStore]
    storage: StorageRegistry
    job_id: UUID
    engine: object

    def add_node(self, name: str) -> StorageNode:
        node = self.distribution.register_node(
            self.db,
            node_name=name,
            node_address=f"{name}:9000",
            capacity_bytes=1024 * 1024,
        )
        self.distribution.record_heartbeat(
            self.db,
            node_id=node.node_id,
            status="healthy",
            latency_ms=1,
            used_bytes=0,
        )
        self.nodes.append(node)
        store = MemoryObjectStore()
        self.stores[name] = store
        configured = list(self.storage.configured_nodes)
        configured.append(
            ConfiguredStorageNode(
                node_name=name,
                node_address=f"{name}:9000",
                capacity_bytes=1024 * 1024,
                bucket="distributed-files",
                object_store=store,
            )
        )
        self.storage = StorageRegistry(configured)
        self.db.commit()
        return node

    def claim(self, now: datetime | None = None) -> UUID | None:
        job_id = self.replication.claim_next(self.db, now=now)
        self.db.commit()
        return job_id


@pytest.fixture
def replication_environment() -> Generator[ReplicationEnvironment, None, None]:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    db = session_factory()
    distribution = DistributionEngine()
    replication = ReplicationEngine(
        distribution,
        heartbeat_ttl_seconds=120,
        lease_seconds=300,
    )
    owner = User(
        username="replica-owner",
        email="replica-owner@example.com",
        password_hash="not-used",
        role="viewer",
    )
    db.add(owner)
    db.flush()
    nodes: list[StorageNode] = []
    stores: dict[str, MemoryObjectStore] = {}
    configured_nodes: list[ConfiguredStorageNode] = []
    for index in range(1, 4):
        name = f"node-{index}"
        node = distribution.register_node(
            db,
            node_name=name,
            node_address=f"{name}:9000",
            capacity_bytes=1024 * 1024,
        )
        distribution.record_heartbeat(
            db,
            node_id=node.node_id,
            status="healthy",
            latency_ms=1,
            used_bytes=0,
        )
        store = MemoryObjectStore()
        nodes.append(node)
        stores[name] = store
        configured_nodes.append(
            ConfiguredStorageNode(
                node_name=name,
                node_address=f"{name}:9000",
                capacity_bytes=1024 * 1024,
                bucket="distributed-files",
                object_store=store,
            )
        )
    storage = StorageRegistry(configured_nodes)
    file_id = uuid4()
    stored = FileEngine(stores["node-1"]).store_content(
        file_id=file_id,
        version_number=1,
        source=BytesIO(CONTENT),
        content_type="text/plain",
    )
    file = metadata_engine.register_file(
        db,
        owner_id=owner.user_id,
        file_id=file_id,
        filename="replicated.txt",
        checksum=stored.checksum,
        file_size=stored.size_bytes,
        mime_type="text/plain",
    )
    file.upload_state = "UPLOADED"
    file.replication_factor = 3
    file.availability_state = "DEGRADED"
    file_version = db.scalar(
        select(FileVersion).where(FileVersion.file_id == file_id)
    )
    assert file_version is not None
    db.add(
        FileReplica(
            file_id=file_id,
            version_id=file_version.version_id,
            node_id=nodes[0].node_id,
            object_key=stored.object_key,
            checksum=stored.checksum,
            status="VERIFIED",
            checksum_verified_at=datetime.now(timezone.utc),
        )
    )
    job = replication.enqueue(
        db,
        file_id=file_id,
        version_id=file_version.version_id,
        target_count=3,
        idempotency_key=f"file:{file_id}:version:1:replicas:3",
    )
    db.commit()
    environment = ReplicationEnvironment(
        db=db,
        session_factory=session_factory,
        distribution=distribution,
        replication=replication,
        file=file,
        nodes=nodes,
        stores=stores,
        storage=storage,
        job_id=job.job_id,
        engine=engine,
    )
    try:
        yield environment
    finally:
        db.close()
        engine.dispose()


def _process_next(environment: ReplicationEnvironment) -> bool:
    job_id = environment.claim()
    assert job_id is not None
    return environment.replication.process(
        environment.db,
        job_id=job_id,
        storage=environment.storage,
    )


def test_replication_verifies_sha256_on_three_distinct_nodes(
    replication_environment: ReplicationEnvironment,
) -> None:
    environment = replication_environment

    assert _process_next(environment)

    replicas = list(
        environment.db.scalars(
            select(FileReplica).where(FileReplica.file_id == environment.file.file_id)
        )
    )
    assert len(replicas) == 3
    assert {replica.node_id for replica in replicas} == {
        node.node_id for node in environment.nodes
    }
    assert {replica.status for replica in replicas} == {"VERIFIED"}
    assert {replica.checksum for replica in replicas} == {
        hashlib.sha256(CONTENT).hexdigest()
    }
    assert environment.file.availability_state == "AVAILABLE"


def test_unhealthy_node_is_not_selected_for_replication(
    replication_environment: ReplicationEnvironment,
) -> None:
    environment = replication_environment
    environment.distribution.record_heartbeat(
        environment.db,
        node_id=environment.nodes[2].node_id,
        status="offline",
        latency_ms=None,
        used_bytes=None,
    )
    environment.db.commit()

    assert not _process_next(environment)

    assert environment.db.scalar(
        select(func.count()).select_from(FileReplica).where(
            FileReplica.node_id == environment.nodes[2].node_id
        )
    ) == 0
    assert environment.db.get(ReplicationJob, environment.job_id).status == "RETRY"


def test_partial_replication_failure_retries_without_losing_valid_replicas(
    replication_environment: ReplicationEnvironment,
) -> None:
    environment = replication_environment
    targets = environment.distribution.select_targets(
        environment.db,
        file_id=environment.file.file_id,
        replica_count=3,
    )
    failing_target = next(node for node in targets if node.node_id != environment.nodes[0].node_id)
    environment.stores[failing_target.node_name].fail_put = True

    assert not _process_next(environment)
    assert environment.db.scalar(
        select(func.count()).select_from(FileReplica).where(
            FileReplica.status == "VERIFIED"
        )
    ) == 2
    assert environment.db.get(ReplicationJob, environment.job_id).status == "RETRY"

    environment.stores[failing_target.node_name].fail_put = False
    job = environment.db.get(ReplicationJob, environment.job_id)
    job.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    environment.db.commit()

    assert _process_next(environment)
    assert environment.db.get(ReplicationJob, environment.job_id).status == "COMPLETED"


def test_duplicate_jobs_are_idempotent(
    replication_environment: ReplicationEnvironment,
) -> None:
    environment = replication_environment
    job = environment.db.get(ReplicationJob, environment.job_id)
    duplicate = environment.replication.enqueue(
        environment.db,
        file_id=job.file_id,
        version_id=job.version_id,
        target_count=job.target_count,
        idempotency_key=job.idempotency_key,
    )

    assert duplicate.job_id == job.job_id
    assert environment.db.scalar(select(func.count()).select_from(ReplicationJob)) == 1


def test_dead_job_can_be_retried_explicitly(
    replication_environment: ReplicationEnvironment,
) -> None:
    environment = replication_environment
    job = environment.db.get(ReplicationJob, environment.job_id)
    job.status = "DEAD"
    job.attempt_count = job.max_attempts
    job.last_error = "node unavailable"
    environment.db.commit()

    retried = environment.replication.retry_dead_job(
        environment.db,
        job_id=environment.job_id,
    )
    environment.db.commit()

    assert retried is not None
    assert retried.status == "PENDING"
    assert retried.attempt_count == 0
    assert retried.last_error is None


def test_expired_job_lease_is_reclaimed_after_worker_restart(
    replication_environment: ReplicationEnvironment,
) -> None:
    environment = replication_environment
    started = datetime.now(timezone.utc)
    first_claim = environment.claim(now=started)

    assert first_claim == environment.job_id
    assert environment.replication.claim_next(
        environment.db,
        now=started + timedelta(seconds=10),
    ) is None
    environment.db.rollback()
    restarted_claim = environment.replication.claim_next(
        environment.db,
        now=started + timedelta(seconds=301),
    )
    environment.db.commit()

    assert restarted_claim == environment.job_id
    assert environment.db.get(ReplicationJob, environment.job_id).attempt_count == 2


def test_duplicate_reconciliation_does_not_duplicate_jobs(
    replication_environment: ReplicationEnvironment,
) -> None:
    environment = replication_environment

    assert environment.replication.reconcile_under_replicated(environment.db) == 1
    assert environment.replication.reconcile_under_replicated(environment.db) == 1

    assert environment.db.scalar(select(func.count()).select_from(ReplicationJob)) == 1


def test_node_failure_marks_file_degraded_and_replacement_repairs_replica(
    replication_environment: ReplicationEnvironment,
) -> None:
    environment = replication_environment
    assert _process_next(environment)
    failed_node = environment.nodes[0]
    environment.distribution.record_heartbeat(
        environment.db,
        node_id=failed_node.node_id,
        status="offline",
        latency_ms=None,
        used_bytes=None,
    )
    environment.add_node("node-4")

    assert environment.replication.reconcile_under_replicated(environment.db) == 1
    assert environment.file.availability_state == "DEGRADED"
    assert _process_next(environment)

    replicas = list(
        environment.db.scalars(
            select(FileReplica).where(
                FileReplica.version_id
                == environment.db.scalar(
                    select(ReplicationJob.version_id).where(
                        ReplicationJob.job_id == environment.job_id
                    )
                ),
                FileReplica.status == "VERIFIED",
            )
        )
    )
    assert environment.file.availability_state == "AVAILABLE"
    assert any(replica.node_id == environment.nodes[3].node_id for replica in replicas)


def test_corrupt_source_replica_is_not_used_for_recovery(
    replication_environment: ReplicationEnvironment,
) -> None:
    environment = replication_environment
    source_replica = environment.db.scalar(
        select(FileReplica).where(FileReplica.node_id == environment.nodes[0].node_id)
    )
    assert source_replica is not None
    object_id = f"distributed-files/{source_replica.object_key}"
    environment.stores["node-1"].objects[object_id] = b"corrupted"

    assert not _process_next(environment)

    assert source_replica.status == "CORRUPT"
    assert environment.file.availability_state == "UNAVAILABLE"
    assert environment.db.get(ReplicationJob, environment.job_id).status == "RETRY"


def test_database_failure_after_copy_leaves_job_recoverable(
    replication_environment: ReplicationEnvironment,
) -> None:
    environment = replication_environment
    failed_once = False

    def fail_commit_after_target_write(session: Session) -> None:
        nonlocal failed_once
        if not failed_once and any(
            key.startswith("distributed-files/")
            for name, store in environment.stores.items()
            if name != "node-1"
            for key in store.objects
        ):
            failed_once = True
            raise RuntimeError("simulated database commit failure")

    event.listen(environment.db, "before_commit", fail_commit_after_target_write)
    claimed = environment.claim()
    assert claimed == environment.job_id
    with pytest.raises(RuntimeError, match="simulated database commit failure"):
        environment.replication.process(
            environment.db,
            job_id=claimed,
            storage=environment.storage,
        )
    event.remove(environment.db, "before_commit", fail_commit_after_target_write)
    environment.db.rollback()

    assert any(
        store.objects
        for name, store in environment.stores.items()
        if name != "node-1"
    )
    assert environment.db.get(ReplicationJob, environment.job_id).status == "PROCESSING"

    restarted_claim = environment.replication.claim_next(
        environment.db,
        now=datetime.now(timezone.utc) + timedelta(seconds=301),
    )
    environment.db.commit()
    assert restarted_claim == environment.job_id
    assert environment.replication.process(
        environment.db,
        job_id=restarted_claim,
        storage=environment.storage,
    )


def test_worker_backfills_legacy_content_only_after_checksum_validation(
    replication_environment: ReplicationEnvironment,
) -> None:
    environment = replication_environment
    source_replica = environment.db.scalar(
        select(FileReplica).where(
            FileReplica.node_id == environment.nodes[0].node_id
        )
    )
    assert source_replica is not None
    environment.db.delete(source_replica)
    environment.db.commit()
    worker = ReplicationWorker(
        environment.session_factory,
        environment.storage,
        engine=environment.replication,
    )

    worker._backfill_legacy_replicas(environment.db)

    replica = environment.db.scalar(
        select(FileReplica).where(
            FileReplica.node_id == environment.nodes[0].node_id
        )
    )
    assert replica is not None
    assert replica.status == "VERIFIED"
    assert replica.checksum == hashlib.sha256(CONTENT).hexdigest()
