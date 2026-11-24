import os
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from minio.error import S3Error
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from urllib3.exceptions import HTTPError

if __package__ == "backend.engines":
    from ..database.models import (
        FileMetadata,
        FileReplica,
        FileVersion,
        ReplicationJob,
        StorageNode,
    )
    from .distribution import (
        DistributionEngine,
        InsufficientHealthyNodesError,
        distribution_engine,
    )
    from .files import ChecksumMismatchError, FileEngine
    from .storage import StorageRegistry
else:
    from database.models import (
        FileMetadata,
        FileReplica,
        FileVersion,
        ReplicationJob,
        StorageNode,
    )
    from engines.distribution import (
        DistributionEngine,
        InsufficientHealthyNodesError,
        distribution_engine,
    )
    from engines.files import ChecksumMismatchError, FileEngine
    from engines.storage import StorageRegistry


_RETRYABLE_STORAGE_ERRORS = (HTTPError, S3Error, OSError)
_HEARTBEAT_TTL_SECONDS = int(os.getenv("STORAGE_HEARTBEAT_TTL_SECONDS", "60"))
_LEASE_SECONDS = int(os.getenv("REPLICATION_JOB_LEASE_SECONDS", "300"))
_DEFAULT_MAX_ATTEMPTS = int(os.getenv("REPLICATION_MAX_ATTEMPTS", "5"))


class ReplicationEngine:
    def __init__(
        self,
        distribution: DistributionEngine = distribution_engine,
        *,
        heartbeat_ttl_seconds: int = _HEARTBEAT_TTL_SECONDS,
        lease_seconds: int = _LEASE_SECONDS,
    ) -> None:
        if heartbeat_ttl_seconds < 1 or lease_seconds < 1:
            raise ValueError("heartbeat TTL and job lease must be positive")
        self._distribution = distribution
        self._heartbeat_ttl_seconds = heartbeat_ttl_seconds
        self._lease_seconds = lease_seconds

    def enqueue(
        self,
        db: Session,
        *,
        file_id: UUID,
        version_id: UUID,
        target_count: int,
        idempotency_key: str,
        max_attempts: int | None = None,
    ) -> ReplicationJob:
        resolved_max_attempts = (
            _DEFAULT_MAX_ATTEMPTS if max_attempts is None else max_attempts
        )
        if target_count < 1 or resolved_max_attempts < 1:
            raise ValueError("target_count and max_attempts must be positive")
        job = db.scalar(
            select(ReplicationJob).where(
                ReplicationJob.idempotency_key == idempotency_key
            )
        )
        if job is None:
            try:
                with db.begin_nested():
                    candidate = ReplicationJob(
                        job_id=uuid4(),
                        file_id=file_id,
                        version_id=version_id,
                        target_count=target_count,
                        idempotency_key=idempotency_key,
                        max_attempts=resolved_max_attempts,
                    )
                    db.add(candidate)
                    db.flush()
                job = candidate
            except IntegrityError:
                job = db.scalar(
                    select(ReplicationJob).where(
                        ReplicationJob.idempotency_key == idempotency_key
                    )
                )
                if job is None:
                    raise
        elif job.status == "COMPLETED":
            job.status = "PENDING"
            job.attempt_count = 0
            job.available_at = datetime.now(timezone.utc)
            job.locked_until = None
            job.last_error = None
        db.flush()
        return job

    def retry_dead_job(self, db: Session, *, job_id: UUID) -> ReplicationJob | None:
        job = db.get(ReplicationJob, job_id)
        if job is None or job.status != "DEAD":
            return None
        job.status = "PENDING"
        job.attempt_count = 0
        job.available_at = datetime.now(timezone.utc)
        job.locked_until = None
        job.last_error = None
        db.flush()
        return job

    def claim_next(self, db: Session, *, now: datetime | None = None) -> UUID | None:
        current_time = now or datetime.now(timezone.utc)
        eligible = or_(
            (
                ReplicationJob.status.in_(("PENDING", "RETRY"))
                & (ReplicationJob.available_at <= current_time)
            ),
            (
                (ReplicationJob.status == "PROCESSING")
                & (ReplicationJob.locked_until <= current_time)
            ),
        )
        while True:
            job = db.scalar(
                select(ReplicationJob)
                .where(eligible)
                .order_by(ReplicationJob.created_at, ReplicationJob.job_id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if job is None:
                return None
            if job.attempt_count >= job.max_attempts:
                job.status = "DEAD"
                job.locked_until = None
                job.last_error = job.last_error or "Replication retry limit reached"
                db.flush()
                continue
            job.status = "PROCESSING"
            job.attempt_count += 1
            job.locked_until = current_time + timedelta(
                seconds=self._lease_seconds
            )
            db.flush()
            return job.job_id

    def process(
        self,
        db: Session,
        *,
        job_id: UUID,
        storage: StorageRegistry,
        now: datetime | None = None,
    ) -> bool:
        current_time = now or datetime.now(timezone.utc)
        job = db.get(ReplicationJob, job_id)
        if job is None or job.status != "PROCESSING":
            return False
        file = db.get(FileMetadata, job.file_id)
        version = db.get(FileVersion, job.version_id)
        if file is None or version is None or file.deleted_at is not None:
            job.status = "COMPLETED"
            job.locked_until = None
            job.last_error = None
            db.commit()
            return True

        verified_replicas = self._healthy_verified_replicas(
            db,
            version_id=version.version_id,
            now=current_time,
        )
        if len(verified_replicas) >= job.target_count:
            job.status = "COMPLETED"
            job.locked_until = None
            job.last_error = None
            self.refresh_availability(db, file=file, now=current_time)
            db.commit()
            return True

        excluded_node_ids = {replica.node_id for replica in verified_replicas}
        try:
            targets = self._distribution.select_targets(
                db,
                file_id=file.file_id,
                replica_count=job.target_count - len(verified_replicas),
                file_size_bytes=version.file_size,
                exclude_node_ids=excluded_node_ids,
                health_ttl_seconds=self._heartbeat_ttl_seconds,
            )
        except InsufficientHealthyNodesError as exc:
            self._schedule_retry(job=job, message=str(exc), now=current_time)
            self.refresh_availability(db, file=file, now=current_time)
            db.commit()
            return False

        failures: list[str] = []
        for target in targets:
            replica = db.scalar(
                select(FileReplica).where(
                    FileReplica.version_id == version.version_id,
                    FileReplica.node_id == target.node_id,
                )
            )
            if replica is None:
                replica = FileReplica(
                    replica_id=uuid4(),
                    file_id=file.file_id,
                    version_id=version.version_id,
                    node_id=target.node_id,
                    object_key=FileEngine.object_key(
                        file_id=file.file_id,
                        version_number=version.version_number,
                        checksum=version.checksum,
                    ),
                    checksum=version.checksum,
                    status="PENDING",
                )
                db.add(replica)
                db.flush()
            if replica.status == "VERIFIED":
                continue
            replica.status = "COPYING"
            replica.error_message = None
            db.commit()

            source_spool = None
            source_failures: list[str] = []
            candidates = self._healthy_verified_replicas(
                db,
                version_id=version.version_id,
                now=current_time,
            )
            for candidate in candidates:
                source_node = db.get(StorageNode, candidate.node_id)
                if source_node is None:
                    continue
                try:
                    source_config = storage.for_node(source_node)
                    source_engine = FileEngine(
                        source_config.object_store,
                        bucket=source_config.bucket,
                    )
                    source_spool = source_engine.open_verified_content(
                        candidate.object_key,
                        version.checksum,
                    )
                    break
                except ChecksumMismatchError as exc:
                    candidate.status = "CORRUPT"
                    candidate.error_message = str(exc)
                    db.commit()
                    source_failures.append(
                        f"Replica {candidate.replica_id} failed checksum verification"
                    )
                except (*_RETRYABLE_STORAGE_ERRORS, ValueError) as exc:
                    source_failures.append(
                        f"Replica {candidate.replica_id} could not be read: {exc}"
                    )

            if source_spool is None:
                message = "; ".join(source_failures) or "No verified source is available"
                failures.append(message)
                replica.status = "FAILED"
                replica.error_message = message
                db.commit()
                continue

            try:
                target_config = storage.for_node(target)
                target_engine = FileEngine(
                    target_config.object_store,
                    bucket=target_config.bucket,
                )
                target_spool = source_spool
                target_spool.seek(0)
                target_content = target_engine.store_content(
                    file_id=file.file_id,
                    version_number=version.version_number,
                    source=target_spool,
                    content_type=file.mime_type,
                )
                if target_content.checksum != version.checksum:
                    raise ChecksumMismatchError(
                        "Copied content does not match the file version checksum"
                    )
                replica.object_key = target_content.object_key
                replica.checksum = target_content.checksum
                replica.status = "VERIFIED"
                replica.checksum_verified_at = datetime.now(timezone.utc)
                replica.error_message = None
                db.commit()
            except (*_RETRYABLE_STORAGE_ERRORS, ChecksumMismatchError, ValueError) as exc:
                replica.status = "FAILED"
                replica.error_message = str(exc)[:2000]
                failures.append(f"Node {target.node_name} copy failed: {exc}")
                db.commit()
            finally:
                source_spool.close()

        verified_count = len(
            self._healthy_verified_replicas(
                db,
                version_id=version.version_id,
                now=current_time,
            )
        )
        self.refresh_availability(db, file=file, now=current_time)
        if verified_count >= job.target_count:
            job.status = "COMPLETED"
            job.locked_until = None
            job.last_error = None
            db.commit()
            return True

        message = "; ".join(failures) or (
            f"Only {verified_count} of {job.target_count} replicas are healthy"
        )
        self._schedule_retry(job=job, message=message, now=current_time)
        db.commit()
        return False

    def refresh_availability(
        self,
        db: Session,
        *,
        file: FileMetadata,
        now: datetime | None = None,
    ) -> str:
        current_time = now or datetime.now(timezone.utc)
        version = db.scalar(
            select(FileVersion).where(
                FileVersion.file_id == file.file_id,
                FileVersion.version_number == file.current_version,
            )
        )
        healthy_count = (
            len(
                self._healthy_verified_replicas(
                    db,
                    version_id=version.version_id,
                    now=current_time,
                )
            )
            if version is not None
            else 0
        )
        if healthy_count == 0:
            state = "UNAVAILABLE"
        elif healthy_count >= file.replication_factor:
            state = "AVAILABLE"
        else:
            state = "DEGRADED"
        file.availability_state = state
        return state

    def reconcile_under_replicated(
        self,
        db: Session,
        *,
        now: datetime | None = None,
    ) -> int:
        current_time = now or datetime.now(timezone.utc)
        files = db.scalars(
            select(FileMetadata).where(
                FileMetadata.deleted_at.is_(None),
                FileMetadata.upload_state == "UPLOADED",
            )
        )
        enqueued = 0
        for file in files:
            version = db.scalar(
                select(FileVersion).where(
                    FileVersion.file_id == file.file_id,
                    FileVersion.version_number == file.current_version,
                )
            )
            if version is None:
                continue
            count = len(
                self._healthy_verified_replicas(
                    db,
                    version_id=version.version_id,
                    now=current_time,
                )
            )
            self.refresh_availability(db, file=file, now=current_time)
            if count < file.replication_factor:
                self.enqueue(
                    db,
                    file_id=file.file_id,
                    version_id=version.version_id,
                    target_count=file.replication_factor,
                    idempotency_key=(
                        f"file:{file.file_id}:version:{version.version_number}:"
                        f"replicas:{file.replication_factor}"
                    ),
                )
                enqueued += 1
        db.commit()
        return enqueued

    def _healthy_verified_replicas(
        self,
        db: Session,
        *,
        version_id: UUID,
        now: datetime,
    ) -> list[FileReplica]:
        fresh_after = now - timedelta(seconds=self._heartbeat_ttl_seconds)
        return list(
            db.scalars(
                select(FileReplica)
                .join(StorageNode, StorageNode.node_id == FileReplica.node_id)
                .where(
                    FileReplica.version_id == version_id,
                    FileReplica.status == "VERIFIED",
                    StorageNode.status == "healthy",
                    StorageNode.last_heartbeat_at >= fresh_after,
                )
                .order_by(StorageNode.node_name)
            )
        )

    @staticmethod
    def _schedule_retry(
        *,
        job: ReplicationJob,
        message: str,
        now: datetime,
    ) -> None:
        job.last_error = message[:4000]
        job.locked_until = None
        if job.attempt_count >= job.max_attempts:
            job.status = "DEAD"
            return
        job.status = "RETRY"
        delay_seconds = min(2 ** max(job.attempt_count - 1, 0), 60)
        job.available_at = now + timedelta(seconds=delay_seconds)


replication_engine = ReplicationEngine()
