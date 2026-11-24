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
        OutboxEvent,
        ReplicationJob,
        StorageNode,
    )
    from .distribution import (
        DistributionEngine,
        InsufficientHealthyNodesError,
        distribution_engine,
    )
    from .files import ChecksumMismatchError, FileEngine
    from .outbox import outbox_engine
    from .storage import StorageRegistry
else:
    from database.models import (
        FileMetadata,
        FileReplica,
        FileVersion,
        OutboxEvent,
        ReplicationJob,
        StorageNode,
    )
    from engines.distribution import (
        DistributionEngine,
        InsufficientHealthyNodesError,
        distribution_engine,
    )
    from engines.files import ChecksumMismatchError, FileEngine
    from engines.outbox import outbox_engine
    from engines.storage import StorageRegistry


_RETRYABLE_STORAGE_ERRORS = (HTTPError, S3Error, OSError)
_HEARTBEAT_TTL_SECONDS = int(os.getenv("STORAGE_HEARTBEAT_TTL_SECONDS", "60"))
_LEASE_SECONDS = int(os.getenv("REPLICATION_JOB_LEASE_SECONDS", "300"))
_DEFAULT_MAX_ATTEMPTS = int(os.getenv("REPLICATION_MAX_ATTEMPTS", "5"))
_FILE_LEASE_WAIT_MESSAGE = "Waiting for another background job on this file"


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


class LostJobLeaseError(RuntimeError):
    pass


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
        job_type: str = "REPLICATE",
        reopen_completed: bool = True,
        max_attempts: int | None = None,
    ) -> ReplicationJob:
        resolved_max_attempts = (
            _DEFAULT_MAX_ATTEMPTS if max_attempts is None else max_attempts
        )
        if target_count < 1 or resolved_max_attempts < 1:
            raise ValueError("target_count and max_attempts must be positive")
        if job_type not in {"REPLICATE", "VERIFY", "CLEANUP", "RECONCILE"}:
            raise ValueError(f"Unsupported background job type: {job_type}")
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
                        job_type=job_type,
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
                else:
                    return job
            job.dispatch_generation += 1
            outbox_engine.enqueue_job(
                db,
                job_id=job.job_id,
                job_type=job.job_type,
                idempotency_key=job.idempotency_key,
                dispatch_generation=job.dispatch_generation,
                available_at=job.available_at,
            )
        elif job.status == "COMPLETED" and reopen_completed:
            job.status = "PENDING"
            job.attempt_count = 0
            job.available_at = datetime.now(timezone.utc)
            job.locked_until = None
            job.lock_token = None
            job.last_error = None
            job.dispatch_generation += 1
            outbox_engine.enqueue_job(
                db,
                job_id=job.job_id,
                job_type=job.job_type,
                idempotency_key=job.idempotency_key,
                dispatch_generation=job.dispatch_generation,
                available_at=job.available_at,
            )
        db.flush()
        return job

    def schedule_maintenance(
        self,
        db: Session,
        *,
        now: datetime | None = None,
        reconcile_interval_seconds: int = 60,
        integrity_interval_seconds: int = 3600,
    ) -> int:
        if reconcile_interval_seconds < 1 or integrity_interval_seconds < 1:
            raise ValueError("maintenance intervals must be positive")
        current_time = now or datetime.now(timezone.utc)
        files = db.scalars(
            select(FileMetadata).where(
                FileMetadata.deleted_at.is_(None),
                FileMetadata.upload_state == "UPLOADED",
            )
        )
        scheduled = 0
        for file in files:
            version = db.scalar(
                select(FileVersion).where(
                    FileVersion.file_id == file.file_id,
                    FileVersion.version_number == file.current_version,
                )
            )
            if version is None:
                continue
            reconcile_bucket = int(current_time.timestamp()) // reconcile_interval_seconds
            integrity_bucket = int(current_time.timestamp()) // integrity_interval_seconds
            self.enqueue(
                db,
                file_id=file.file_id,
                version_id=version.version_id,
                target_count=max(1, file.replication_factor),
                idempotency_key=(
                    f"file:{file.file_id}:version:{version.version_number}:"
                    f"reconcile:{reconcile_bucket}"
                ),
                job_type="RECONCILE",
                reopen_completed=False,
            )
            self.enqueue(
                db,
                file_id=file.file_id,
                version_id=version.version_id,
                target_count=1,
                idempotency_key=(
                    f"file:{file.file_id}:version:{version.version_number}:"
                    f"verify:{integrity_bucket}"
                ),
                job_type="VERIFY",
                reopen_completed=False,
            )
            scheduled += 2
        db.commit()
        return scheduled

    def retry_dead_job(self, db: Session, *, job_id: UUID) -> ReplicationJob | None:
        job = db.get(ReplicationJob, job_id)
        if job is None or job.status != "DEAD":
            return None
        job.status = "PENDING"
        job.attempt_count = 0
        job.available_at = datetime.now(timezone.utc)
        job.locked_until = None
        job.lock_token = None
        job.last_error = None
        job.dispatch_generation += 1
        outbox_engine.enqueue_job(
            db,
            job_id=job.job_id,
            job_type=job.job_type,
            idempotency_key=job.idempotency_key,
            dispatch_generation=job.dispatch_generation,
            available_at=job.available_at,
        )
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
            token = self._claim_locked_job(
                db,
                job=job,
                now=current_time,
            )
            if token is not None:
                return job.job_id
            if job.status not in {"DEAD", "RETRY"}:
                return None

    def claim_job(
        self,
        db: Session,
        *,
        job_id: UUID,
        now: datetime | None = None,
    ) -> UUID | None:
        current_time = now or datetime.now(timezone.utc)
        job = db.scalar(
            select(ReplicationJob)
            .where(ReplicationJob.job_id == job_id)
            .with_for_update(skip_locked=True)
        )
        if job is None:
            return None
        locked_until = (
            _as_utc(job.locked_until)
            if job.locked_until is not None
            else None
        )
        is_expired = (
            job.status == "PROCESSING"
            and locked_until is not None
            and locked_until <= current_time
        )
        if job.status not in {"PENDING", "RETRY"} and not is_expired:
            return None
        if _as_utc(job.available_at) > current_time:
            return None
        return self._claim_locked_job(
            db,
            job=job,
            now=current_time,
        )

    def _claim_locked_job(
        self,
        db: Session,
        *,
        job: ReplicationJob,
        now: datetime,
    ) -> UUID | None:
        if job.attempt_count >= job.max_attempts:
            job.status = "DEAD"
            job.locked_until = None
            job.lock_token = None
            job.last_error = job.last_error or "Replication retry limit reached"
            job.dispatch_generation += 1
            outbox_engine.enqueue_job(
                db,
                job_id=job.job_id,
                job_type=job.job_type,
                idempotency_key=job.idempotency_key,
                dispatch_generation=job.dispatch_generation,
                available_at=now,
            )
            db.flush()
            return None
        file = db.scalar(
            select(FileMetadata)
            .where(FileMetadata.file_id == job.file_id)
            .with_for_update(skip_locked=True)
        )
        if file is None:
            return None
        work_locked_until = (
            _as_utc(file.work_locked_until)
            if file.work_locked_until is not None
            else None
        )
        if (
            file.work_lock_token is not None
            and work_locked_until is not None
            and work_locked_until > now
        ):
            job.status = "RETRY"
            job.locked_until = None
            job.lock_token = None
            job.available_at = work_locked_until
            job.last_error = _FILE_LEASE_WAIT_MESSAGE
            job.dispatch_generation += 1
            outbox_engine.enqueue_job(
                db,
                job_id=job.job_id,
                job_type=job.job_type,
                idempotency_key=job.idempotency_key,
                dispatch_generation=job.dispatch_generation,
                available_at=job.available_at,
            )
            db.flush()
            return None
        token = uuid4()
        locked_until = now + timedelta(seconds=self._lease_seconds)
        job.status = "PROCESSING"
        job.attempt_count += 1
        job.locked_until = locked_until
        job.lock_token = token
        job.last_error = None
        file.work_lock_token = token
        file.work_locked_until = locked_until
        db.flush()
        return token

    def process(
        self,
        db: Session,
        *,
        job_id: UUID,
        storage: StorageRegistry,
        lock_token: UUID | None = None,
        now: datetime | None = None,
    ) -> bool:
        current_time = now or datetime.now(timezone.utc)
        job = db.scalar(
            select(ReplicationJob)
            .where(ReplicationJob.job_id == job_id)
            .execution_options(populate_existing=True)
        )
        if job is None or job.status != "PROCESSING":
            return False
        token = lock_token or job.lock_token
        if token is None or not self._owns_job_lease(
            db,
            job_id=job_id,
            lock_token=token,
            now=current_time,
        ):
            return False
        try:
            if job.job_type == "REPLICATE":
                return self._process_replication(
                    db,
                    job_id=job_id,
                    storage=storage,
                    lock_token=token,
                    now=current_time,
                )
            if job.job_type == "RECONCILE":
                return self._process_reconcile(
                    db,
                    job_id=job_id,
                    lock_token=token,
                    now=current_time,
                )
            if job.job_type == "VERIFY":
                return self._process_verification(
                    db,
                    job_id=job_id,
                    storage=storage,
                    lock_token=token,
                    now=current_time,
                )
            if job.job_type == "CLEANUP":
                return self._process_cleanup(
                    db,
                    job_id=job_id,
                    storage=storage,
                    lock_token=token,
                    now=current_time,
                )
            raise ValueError(f"Unsupported background job type: {job.job_type}")
        except LostJobLeaseError:
            db.rollback()
            return False

    def _process_replication(
        self,
        db: Session,
        *,
        job_id: UUID,
        storage: StorageRegistry,
        lock_token: UUID,
        now: datetime | None = None,
    ) -> bool:
        current_time = now or datetime.now(timezone.utc)
        job = db.get(ReplicationJob, job_id)
        if (
            job is None
            or job.status != "PROCESSING"
            or job.lock_token != lock_token
        ):
            return False
        file = db.get(FileMetadata, job.file_id)
        version = db.get(FileVersion, job.version_id)
        if file is None or version is None or file.deleted_at is not None:
            return self._complete_job(
                db,
                job=job,
                lock_token=lock_token,
                file=None,
                now=current_time,
            )

        verified_replicas = self._healthy_verified_replicas(
            db,
            version_id=version.version_id,
            now=current_time,
        )
        if len(verified_replicas) >= job.target_count:
            return self._complete_job(
                db,
                job=job,
                lock_token=lock_token,
                file=file,
                now=current_time,
            )

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
            self._assert_job_lease(db, job_id=job_id, lock_token=lock_token)
            self._schedule_retry(
                db=db,
                job=job,
                message=str(exc),
                now=current_time,
            )
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
            self._assert_job_lease(db, job_id=job_id, lock_token=lock_token)
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
                    self._assert_job_lease(
                        db,
                        job_id=job_id,
                        lock_token=lock_token,
                    )
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
                self._assert_job_lease(db, job_id=job_id, lock_token=lock_token)
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
                self._assert_job_lease(db, job_id=job_id, lock_token=lock_token)
                db.commit()
            except (*_RETRYABLE_STORAGE_ERRORS, ChecksumMismatchError, ValueError) as exc:
                replica.status = "FAILED"
                replica.error_message = str(exc)[:2000]
                failures.append(f"Node {target.node_name} copy failed: {exc}")
                self._assert_job_lease(db, job_id=job_id, lock_token=lock_token)
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
            return self._complete_job(
                db,
                job=job,
                lock_token=lock_token,
                file=file,
                now=current_time,
            )

        message = "; ".join(failures) or (
            f"Only {verified_count} of {job.target_count} replicas are healthy"
        )
        self._assert_job_lease(db, job_id=job_id, lock_token=lock_token)
        self._schedule_retry(
            db=db,
            job=job,
            message=message,
            now=current_time,
        )
        db.commit()
        return False

    def _process_reconcile(
        self,
        db: Session,
        *,
        job_id: UUID,
        lock_token: UUID,
        now: datetime,
    ) -> bool:
        job = self._get_claimed_job(db, job_id=job_id, lock_token=lock_token)
        if job is None:
            return False
        file = db.get(FileMetadata, job.file_id)
        version = db.get(FileVersion, job.version_id)
        if file is None or version is None or file.deleted_at is not None:
            return self._complete_job(
                db,
                job=job,
                lock_token=lock_token,
                file=None,
                now=now,
            )
        healthy_count = len(
            self._healthy_verified_replicas(
                db,
                version_id=version.version_id,
                now=now,
            )
        )
        if healthy_count < file.replication_factor:
            self.enqueue(
                db,
                file_id=file.file_id,
                version_id=version.version_id,
                target_count=file.replication_factor,
                idempotency_key=(
                    f"file:{file.file_id}:version:{version.version_number}:"
                    f"replicas:{file.replication_factor}"
                ),
                job_type="REPLICATE",
            )
        return self._complete_job(
            db,
            job=job,
            lock_token=lock_token,
            file=file,
            now=now,
        )

    def _process_verification(
        self,
        db: Session,
        *,
        job_id: UUID,
        storage: StorageRegistry,
        lock_token: UUID,
        now: datetime,
    ) -> bool:
        job = self._get_claimed_job(db, job_id=job_id, lock_token=lock_token)
        if job is None:
            return False
        file = db.get(FileMetadata, job.file_id)
        version = db.get(FileVersion, job.version_id)
        if file is None or version is None or file.deleted_at is not None:
            return self._complete_job(
                db,
                job=job,
                lock_token=lock_token,
                file=None,
                now=now,
            )

        corrupt_found = False
        failures: list[str] = []
        replicas = list(
            db.scalars(
                select(FileReplica).where(
                    FileReplica.version_id == version.version_id,
                    FileReplica.status == "VERIFIED",
                )
            )
        )
        for replica in replicas:
            node = db.get(StorageNode, replica.node_id)
            if node is None:
                failures.append(f"Storage node for replica {replica.replica_id} is missing")
                continue
            try:
                configured = storage.for_node(node)
                file_engine = FileEngine(
                    configured.object_store,
                    bucket=configured.bucket,
                )
                verified = file_engine.verify_content(
                    replica.object_key,
                    version.checksum,
                )
            except ChecksumMismatchError as exc:
                verified = False
                replica.error_message = str(exc)[:2000]
            except (*_RETRYABLE_STORAGE_ERRORS, ValueError) as exc:
                failures.append(
                    f"Replica {replica.replica_id} could not be verified: {exc}"
                )
                continue
            if not verified:
                replica.status = "CORRUPT"
                replica.error_message = (
                    replica.error_message or "Object checksum verification failed"
                )
                corrupt_found = True
                self._assert_job_lease(
                    db,
                    job_id=job_id,
                    lock_token=lock_token,
                )
                db.commit()
        if corrupt_found:
            self.enqueue(
                db,
                file_id=file.file_id,
                version_id=version.version_id,
                target_count=file.replication_factor,
                idempotency_key=(
                    f"file:{file.file_id}:version:{version.version_number}:"
                    f"replicas:{file.replication_factor}"
                ),
                job_type="REPLICATE",
            )
        self.refresh_availability(db, file=file, now=now)
        if failures:
            self._assert_job_lease(
                db,
                job_id=job_id,
                lock_token=lock_token,
            )
            self._schedule_retry(
                db=db,
                job=job,
                message="; ".join(failures),
                now=now,
            )
            db.commit()
            return False
        return self._complete_job(
            db,
            job=job,
            lock_token=lock_token,
            file=file,
            now=now,
        )

    def _process_cleanup(
        self,
        db: Session,
        *,
        job_id: UUID,
        storage: StorageRegistry,
        lock_token: UUID,
        now: datetime,
    ) -> bool:
        job = self._get_claimed_job(db, job_id=job_id, lock_token=lock_token)
        if job is None:
            return False
        file = db.get(FileMetadata, job.file_id)
        version = db.get(FileVersion, job.version_id)
        if file is None or version is None:
            return self._complete_job(
                db,
                job=job,
                lock_token=lock_token,
                file=None,
                now=now,
            )
        if file.deleted_at is None:
            job.last_error = "Cleanup skipped because the file is not soft-deleted"
            return self._complete_job(
                db,
                job=job,
                lock_token=lock_token,
                file=file,
                now=now,
            )

        failures: list[str] = []
        replicas = list(
            db.scalars(
                select(FileReplica).where(
                    FileReplica.version_id == version.version_id,
                    FileReplica.status != "DELETED",
                )
            )
        )
        for replica in replicas:
            node = db.get(StorageNode, replica.node_id)
            if node is None:
                failures.append(f"Storage node for replica {replica.replica_id} is missing")
                continue
            try:
                configured = storage.for_node(node)
                FileEngine(
                    configured.object_store,
                    bucket=configured.bucket,
                ).delete_content(replica.object_key)
            except (*_RETRYABLE_STORAGE_ERRORS, ValueError) as exc:
                replica.status = "FAILED"
                replica.error_message = str(exc)[:2000]
                failures.append(
                    f"Replica {replica.replica_id} could not be deleted: {exc}"
                )
            else:
                replica.status = "DELETED"
                replica.error_message = None
            self._assert_job_lease(
                db,
                job_id=job_id,
                lock_token=lock_token,
            )
            db.commit()
        if failures:
            self._assert_job_lease(
                db,
                job_id=job_id,
                lock_token=lock_token,
            )
            self._schedule_retry(
                db=db,
                job=job,
                message="; ".join(failures),
                now=now,
            )
            db.commit()
            return False
        return self._complete_job(
            db,
            job=job,
            lock_token=lock_token,
            file=file,
            now=now,
        )

    def _complete_job(
        self,
        db: Session,
        *,
        job: ReplicationJob,
        lock_token: UUID,
        file: FileMetadata | None,
        now: datetime,
    ) -> bool:
        self._assert_job_lease(
            db,
            job_id=job.job_id,
            lock_token=lock_token,
        )
        self._release_file_lease(
            db,
            file_id=job.file_id,
            lock_token=lock_token,
            excluding_job_id=job.job_id,
            now=now,
        )
        job.status = "COMPLETED"
        job.locked_until = None
        job.lock_token = None
        job.last_error = None
        if file is not None and file.deleted_at is None:
            self.refresh_availability(db, file=file, now=now)
        db.commit()
        return True

    def _release_file_lease(
        self,
        db: Session,
        *,
        file_id: UUID,
        lock_token: UUID,
        excluding_job_id: UUID,
        now: datetime,
    ) -> None:
        file = db.get(FileMetadata, file_id)
        if file is None or file.work_lock_token != lock_token:
            return
        file.work_lock_token = None
        file.work_locked_until = None
        waiting_jobs = db.scalars(
            select(ReplicationJob)
            .where(
                ReplicationJob.file_id == file_id,
                ReplicationJob.job_id != excluding_job_id,
                ReplicationJob.status == "RETRY",
                ReplicationJob.last_error == _FILE_LEASE_WAIT_MESSAGE,
            )
            .with_for_update(skip_locked=True)
        )
        for waiting_job in waiting_jobs:
            waiting_job.available_at = now
            current_key = (
                f"job:{waiting_job.job_id}:dispatch:"
                f"{waiting_job.dispatch_generation}"
            )
            event = db.scalar(
                select(OutboxEvent).where(OutboxEvent.event_key == current_key)
            )
            if event is not None and event.status == "PENDING":
                event.available_at = now
                continue
            if event is not None and event.status == "PUBLISHING":
                continue
            waiting_job.dispatch_generation += 1
            outbox_engine.enqueue_job(
                db,
                job_id=waiting_job.job_id,
                job_type=waiting_job.job_type,
                idempotency_key=waiting_job.idempotency_key,
                dispatch_generation=waiting_job.dispatch_generation,
                available_at=now,
            )

    @staticmethod
    def _get_claimed_job(
        db: Session,
        *,
        job_id: UUID,
        lock_token: UUID,
    ) -> ReplicationJob | None:
        job = db.get(ReplicationJob, job_id)
        if (
            job is None
            or job.status != "PROCESSING"
            or job.lock_token != lock_token
        ):
            return None
        return job

    @staticmethod
    def _owns_job_lease(
        db: Session,
        *,
        job_id: UUID,
        lock_token: UUID,
        now: datetime,
    ) -> bool:
        lease = db.execute(
            select(
                ReplicationJob.status,
                ReplicationJob.file_id,
                ReplicationJob.lock_token,
                ReplicationJob.locked_until,
                FileMetadata.work_lock_token,
                FileMetadata.work_locked_until,
            )
            .join(FileMetadata, FileMetadata.file_id == ReplicationJob.file_id)
            .where(ReplicationJob.job_id == job_id)
        ).one_or_none()
        locked_until = lease.locked_until if lease is not None else None
        if locked_until is not None and locked_until.tzinfo is None:
            locked_until = locked_until.replace(tzinfo=timezone.utc)
        work_locked_until = lease.work_locked_until if lease is not None else None
        if work_locked_until is not None and work_locked_until.tzinfo is None:
            work_locked_until = work_locked_until.replace(tzinfo=timezone.utc)
        return bool(
            lease is not None
            and lease.status == "PROCESSING"
            and lease.lock_token == lock_token
            and lease.work_lock_token == lock_token
            and locked_until is not None
            and locked_until > now
            and work_locked_until is not None
            and work_locked_until > now
        )

    @staticmethod
    def _assert_job_lease(
        db: Session,
        *,
        job_id: UUID,
        lock_token: UUID,
    ) -> None:
        current_time = datetime.now(timezone.utc)
        with db.no_autoflush:
            lease = db.execute(
                select(
                    ReplicationJob.status,
                    ReplicationJob.file_id,
                    ReplicationJob.lock_token,
                    ReplicationJob.locked_until,
                )
                .where(ReplicationJob.job_id == job_id)
                .with_for_update()
            ).one_or_none()
            file_lease = (
                db.execute(
                    select(
                        FileMetadata.work_lock_token,
                        FileMetadata.work_locked_until,
                    )
                    .where(FileMetadata.file_id == lease.file_id)
                    .with_for_update()
                ).one_or_none()
                if lease is not None
                else None
            )
        locked_until = lease.locked_until if lease is not None else None
        if locked_until is not None and locked_until.tzinfo is None:
            locked_until = locked_until.replace(tzinfo=timezone.utc)
        work_locked_until = (
            file_lease.work_locked_until if file_lease is not None else None
        )
        if work_locked_until is not None and work_locked_until.tzinfo is None:
            work_locked_until = work_locked_until.replace(tzinfo=timezone.utc)
        if (
            lease is None
            or file_lease is None
            or lease.status != "PROCESSING"
            or lease.lock_token != lock_token
            or file_lease.work_lock_token != lock_token
            or locked_until is None
            or locked_until <= current_time
            or work_locked_until is None
            or work_locked_until <= current_time
        ):
            raise LostJobLeaseError(
                f"Worker no longer owns replication job {job_id}"
            )

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

    def _schedule_retry(
        self,
        db: Session,
        *,
        job: ReplicationJob,
        message: str,
        now: datetime,
    ) -> None:
        lock_token = job.lock_token
        job.last_error = message[:4000]
        job.locked_until = None
        job.lock_token = None
        if job.attempt_count >= job.max_attempts:
            job.status = "DEAD"
        else:
            job.status = "RETRY"
            delay_seconds = min(2 ** max(job.attempt_count - 1, 0), 60)
            job.available_at = now + timedelta(seconds=delay_seconds)
            job.dispatch_generation += 1
            outbox_engine.enqueue_job(
                db,
                job_id=job.job_id,
                job_type=job.job_type,
                idempotency_key=job.idempotency_key,
                dispatch_generation=job.dispatch_generation,
                available_at=job.available_at,
            )
        if lock_token is not None:
            self._release_file_lease(
                db,
                file_id=job.file_id,
                lock_token=lock_token,
                excluding_job_id=job.job_id,
                now=now,
            )


replication_engine = ReplicationEngine()
