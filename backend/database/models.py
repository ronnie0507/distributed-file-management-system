from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid
from sqlalchemy.dialects.postgresql import INET, JSONB

from .base import Base


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint(
            "role IN ('admin', 'manager', 'editor', 'viewer')",
            name="users_role_check",
        ),
    )

    user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    username: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(String(30), nullable=False, default="viewer")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class RefreshSession(Base):
    __tablename__ = "refresh_sessions"
    __table_args__ = (
        UniqueConstraint("refresh_token_hash", name="uq_refresh_sessions_token_hash"),
        Index("idx_refresh_sessions_family_id", "family_id"),
        Index("idx_refresh_sessions_user_id", "user_id"),
        Index("idx_refresh_sessions_expires_at", "expires_at"),
    )

    session_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    family_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    refresh_token_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    replaced_by_session_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))


class AuditLog(Base):
    __tablename__ = "audit_logs"
    __table_args__ = (
        Index("idx_audit_logs_actor", "actor_user_id"),
        Index("idx_audit_logs_resource", "resource_type", "resource_id"),
        Index("idx_audit_logs_created_at", "created_at"),
    )

    audit_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    actor_user_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.user_id")
    )
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(50), nullable=False)
    resource_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    ip_address: Mapped[str | None] = mapped_column(
        String(45).with_variant(INET, "postgresql")
    )
    details: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), nullable=False, default=dict
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class FileMetadata(Base):
    __tablename__ = "files"
    __table_args__ = (
        CheckConstraint("file_size >= 0", name="files_size_check"),
        CheckConstraint("current_version > 0", name="files_version_check"),
        CheckConstraint(
            "upload_state IN ('PENDING', 'UPLOADED', 'REPLICATING', 'AVAILABLE', 'FAILED')",
            name="files_upload_state_check",
        ),
        CheckConstraint(
            "availability_state IN ('AVAILABLE', 'DEGRADED', 'UNAVAILABLE')",
            name="files_availability_state_check",
        ),
        CheckConstraint("replication_factor > 0", name="files_replication_factor_check"),
        Index("idx_files_owner_id", "owner_id"),
        Index("idx_files_filename", "filename"),
        Index("idx_files_checksum", "checksum"),
        Index("idx_files_upload_state", "upload_state"),
    )

    file_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    owner_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.user_id"), nullable=False
    )
    filename: Mapped[str] = mapped_column(Text, nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    file_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mime_type: Mapped[str | None] = mapped_column(String(255))
    current_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    upload_state: Mapped[str] = mapped_column(
        String(20), nullable=False, default="PENDING"
    )
    availability_state: Mapped[str] = mapped_column(
        String(20), nullable=False, default="UNAVAILABLE"
    )
    replication_factor: Mapped[int] = mapped_column(
        Integer, nullable=False, default=3
    )
    work_lock_token: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    work_locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class FileVersion(Base):
    __tablename__ = "file_versions"
    __table_args__ = (
        CheckConstraint("version_number > 0", name="file_versions_number_check"),
        CheckConstraint("file_size >= 0", name="file_versions_size_check"),
        UniqueConstraint("file_id", "version_number", name="uq_file_version_number"),
        Index("idx_file_versions_file_id", "file_id"),
    )

    version_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    file_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("files.file_id", ondelete="CASCADE"), nullable=False
    )
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    file_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    created_by: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.user_id"), nullable=False
    )


class Permission(Base):
    __tablename__ = "permissions"
    __table_args__ = (
        CheckConstraint(
            "role IN ('owner', 'editor', 'viewer')",
            name="permissions_role_check",
        ),
        UniqueConstraint("user_id", "file_id", name="uq_permission_user_file"),
        Index("idx_permissions_file_id", "file_id"),
        Index("idx_permissions_user_id", "user_id"),
    )

    permission_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False
    )
    file_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("files.file_id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[str] = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class StorageNode(Base):
    __tablename__ = "storage_nodes"
    __table_args__ = (
        CheckConstraint(
            "status IN ('healthy', 'degraded', 'unhealthy', 'offline', 'unknown')",
            name="storage_nodes_status_check",
        ),
        CheckConstraint("capacity_bytes IS NULL OR capacity_bytes >= 0"),
        CheckConstraint("used_bytes >= 0"),
    )

    node_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    node_name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    node_address: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="unknown"
    )
    capacity_bytes: Mapped[int | None] = mapped_column(BigInteger)
    used_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class FileReplica(Base):
    __tablename__ = "file_replicas"
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING', 'COPYING', 'VERIFIED', 'FAILED', 'DELETED', 'CORRUPT', 'MISSING')",
            name="file_replicas_status_check",
        ),
        UniqueConstraint("node_id", "object_key", name="uq_replica_node_object"),
        UniqueConstraint("version_id", "node_id", name="uq_replica_version_node"),
        Index("idx_file_replicas_file_id", "file_id"),
        Index("idx_file_replicas_version_id", "version_id"),
        Index("idx_file_replicas_node_id", "node_id"),
    )

    replica_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    file_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("files.file_id", ondelete="CASCADE"), nullable=False
    )
    version_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("file_versions.version_id", ondelete="CASCADE"),
        nullable=False,
    )
    node_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("storage_nodes.node_id"), nullable=False
    )
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    error_message: Mapped[str | None] = mapped_column(Text)
    checksum_verified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class ReplicationJob(Base):
    __tablename__ = "replication_jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'RETRY', 'COMPLETED', 'DEAD')",
            name="replication_jobs_status_check",
        ),
        CheckConstraint(
            "job_type IN ('REPLICATE', 'VERIFY', 'CLEANUP', 'RECONCILE')",
            name="replication_jobs_type_check",
        ),
        CheckConstraint("target_count > 0", name="replication_jobs_target_count_check"),
        CheckConstraint("attempt_count >= 0", name="replication_jobs_attempt_count_check"),
        CheckConstraint("max_attempts > 0", name="replication_jobs_max_attempts_check"),
        CheckConstraint(
            "dispatch_generation >= 0",
            name="replication_jobs_dispatch_generation_check",
        ),
        UniqueConstraint("idempotency_key", name="uq_replication_job_idempotency"),
        Index("idx_replication_jobs_due", "status", "available_at"),
    )

    job_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    file_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("files.file_id", ondelete="CASCADE"),
        nullable=False,
    )
    version_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("file_versions.version_id", ondelete="CASCADE"),
        nullable=False,
    )
    job_type: Mapped[str] = mapped_column(
        String(20), nullable=False, default="REPLICATE"
    )
    target_count: Mapped[int] = mapped_column(Integer, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    dispatch_generation: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lock_token: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING', 'PUBLISHING', 'PUBLISHED')",
            name="outbox_events_status_check",
        ),
        CheckConstraint(
            "attempt_count >= 0",
            name="outbox_events_attempt_count_check",
        ),
        UniqueConstraint("event_key", name="uq_outbox_event_key"),
        Index("idx_outbox_events_due", "status", "available_at"),
        Index("idx_outbox_events_job_id", "job_id"),
    )

    event_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    job_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("replication_jobs.job_id", ondelete="CASCADE"),
        nullable=False,
    )
    event_key: Mapped[str] = mapped_column(String(300), nullable=False)
    event_type: Mapped[str] = mapped_column(
        String(80), nullable=False, default="background_job"
    )
    payload: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), nullable=False, default=dict
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="PENDING"
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lock_token: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class NodeHealth(Base):
    __tablename__ = "node_health"
    __table_args__ = (Index("idx_node_health_node_recorded", "node_id", "recorded_at"),)

    health_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    node_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("storage_nodes.node_id"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    used_bytes: Mapped[int | None] = mapped_column(BigInteger)
    details: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), nullable=False, default=dict
    )
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
