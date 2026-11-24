import logging
import os
import time
from datetime import datetime, timezone
from time import perf_counter

from minio.error import S3Error
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from urllib3.exceptions import HTTPError

if __package__ == "backend.workers":
    from ..database.models import FileMetadata, FileReplica, FileVersion, StorageNode
    from ..database.session import get_session_factory
    from ..engines.distribution import distribution_engine
    from ..engines.files import ChecksumMismatchError, FileEngine
    from ..engines.replication import ReplicationEngine, replication_engine
    from ..engines.storage import StorageRegistry, create_storage_registry
else:
    from database.models import FileMetadata, FileReplica, FileVersion, StorageNode
    from database.session import get_session_factory
    from engines.distribution import distribution_engine
    from engines.files import ChecksumMismatchError, FileEngine
    from engines.replication import ReplicationEngine, replication_engine
    from engines.storage import StorageRegistry, create_storage_registry


_LOGGER = logging.getLogger(__name__)
_POLL_INTERVAL_SECONDS = float(os.getenv("REPLICATION_POLL_SECONDS", "2"))
_HEARTBEAT_INTERVAL_SECONDS = float(os.getenv("STORAGE_HEALTH_INTERVAL_SECONDS", "10"))
_RETRYABLE_HEALTH_ERRORS = (HTTPError, S3Error, OSError)


class ReplicationWorker:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        storage: StorageRegistry,
        *,
        engine: ReplicationEngine = replication_engine,
        health_interval_seconds: float = _HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        if health_interval_seconds <= 0:
            raise ValueError("health_interval_seconds must be positive")
        self._session_factory = session_factory
        self._storage = storage
        self._engine = engine
        self._health_interval_seconds = health_interval_seconds
        self._last_health_check = 0.0

    def run_once(self) -> bool:
        with self._session_factory() as db:
            if (
                time.monotonic() - self._last_health_check
                >= self._health_interval_seconds
            ):
                self._refresh_nodes(db)
                self._backfill_legacy_replicas(db)
                self._last_health_check = time.monotonic()
            self._engine.reconcile_under_replicated(db)
            job_id = self._engine.claim_next(db)
            db.commit()
        if job_id is None:
            return False
        with self._session_factory() as db:
            return self._engine.process(
                db,
                job_id=job_id,
                storage=self._storage,
            )

    def run_forever(self) -> None:
        while True:
            processed = self.run_once()
            if not processed:
                time.sleep(_POLL_INTERVAL_SECONDS)

    def _refresh_nodes(self, db: Session) -> None:
        for configured in self._storage.configured_nodes:
            node = db.scalar(
                select(StorageNode).where(
                    StorageNode.node_name == configured.node_name
                )
            )
            if node is None:
                node = distribution_engine.register_node(
                    db,
                    node_name=configured.node_name,
                    node_address=configured.node_address,
                    capacity_bytes=configured.capacity_bytes,
                )
                db.commit()
            elif node.node_address != configured.node_address:
                conflicting_node = db.scalar(
                    select(StorageNode.node_id).where(
                        StorageNode.node_address == configured.node_address,
                        StorageNode.node_id != node.node_id,
                    )
                )
                if conflicting_node is not None:
                    raise RuntimeError(
                        f"Configured endpoint for {node.node_name!r} is already "
                        "registered to another storage node"
                    )
                _LOGGER.warning(
                    "Replacing stale endpoint registration for %s with its "
                    "trusted configured endpoint",
                    node.node_name,
                )
                node.node_address = configured.node_address

            if node.capacity_bytes != configured.capacity_bytes:
                node.capacity_bytes = configured.capacity_bytes
            started_at = perf_counter()
            try:
                configured.object_store.health_check()
                used_bytes = configured.object_store.used_bytes(configured.bucket)
                distribution_engine.record_heartbeat(
                    db,
                    node_id=node.node_id,
                    status="healthy",
                    latency_ms=int((perf_counter() - started_at) * 1000),
                    used_bytes=used_bytes,
                )
            except _RETRYABLE_HEALTH_ERRORS as exc:
                distribution_engine.record_heartbeat(
                    db,
                    node_id=node.node_id,
                    status="offline",
                    latency_ms=int((perf_counter() - started_at) * 1000),
                    used_bytes=None,
                )
                _LOGGER.warning(
                    "Storage health check failed for %s: %s",
                    node.node_name,
                    exc,
                )
            db.commit()
        self._refresh_file_availability(db)

    def _refresh_file_availability(self, db: Session) -> None:
        files = db.scalars(
            select(FileMetadata).where(FileMetadata.deleted_at.is_(None))
        )
        for file in files:
            self._engine.refresh_availability(db, file=file)
        db.commit()

    def _backfill_legacy_replicas(self, db: Session) -> None:
        files = list(
            db.scalars(
                select(FileMetadata).where(
                    FileMetadata.deleted_at.is_(None),
                    FileMetadata.upload_state == "UPLOADED",
                )
            )
        )
        for file in files:
            version = db.scalar(
                select(FileVersion).where(
                    FileVersion.file_id == file.file_id,
                    FileVersion.version_number == file.current_version,
                )
            )
            if version is None:
                continue
            existing_replicas = {
                replica.node_id: replica
                for replica in db.scalars(
                    select(FileReplica).where(
                        FileReplica.version_id == version.version_id
                    )
                )
            }
            object_key = FileEngine.object_key(
                file_id=file.file_id,
                version_number=version.version_number,
                checksum=version.checksum,
            )
            for configured in self._storage.configured_nodes:
                node = db.scalar(
                    select(StorageNode).where(
                        StorageNode.node_name == configured.node_name,
                        StorageNode.status == "healthy",
                    )
                )
                if node is None:
                    continue
                replica = existing_replicas.get(node.node_id)
                if replica is not None and replica.status == "VERIFIED":
                    continue
                try:
                    file_engine = FileEngine(
                        configured.object_store,
                        bucket=configured.bucket,
                    )
                    verified = file_engine.verify_content(
                        object_key,
                        version.checksum,
                    )
                except ChecksumMismatchError:
                    _LOGGER.error(
                        "Legacy object checksum mismatch for file %s on node %s",
                        file.file_id,
                        node.node_name,
                    )
                    continue
                except _RETRYABLE_HEALTH_ERRORS as exc:
                    _LOGGER.info(
                        "No legacy object for file %s on node %s: %s",
                        file.file_id,
                        node.node_name,
                        exc,
                    )
                    continue
                if not verified:
                    _LOGGER.error(
                        "Legacy object checksum mismatch for file %s on node %s",
                        file.file_id,
                        node.node_name,
                    )
                    continue
                if replica is None:
                    replica = FileReplica(
                        file_id=file.file_id,
                        version_id=version.version_id,
                        node_id=node.node_id,
                        object_key=object_key,
                        checksum=version.checksum,
                    )
                    db.add(replica)
                replica.object_key = object_key
                replica.checksum = version.checksum
                replica.status = "VERIFIED"
                replica.checksum_verified_at = datetime.now(timezone.utc)
                replica.error_message = None
                db.commit()
                break


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    worker = ReplicationWorker(
        get_session_factory(),
        create_storage_registry(),
    )
    worker.run_forever()


if __name__ == "__main__":
    main()
