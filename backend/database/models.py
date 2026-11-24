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
from sqlalchemy.dialects.postgresql import JSONB

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


class FileMetadata(Base):
    __tablename__ = "files"
    __table_args__ = (
        CheckConstraint("file_size >= 0", name="files_size_check"),
        CheckConstraint("current_version > 0", name="files_version_check"),
        CheckConstraint(
            "upload_state IN ('PENDING', 'UPLOADED', 'REPLICATING', 'AVAILABLE', 'FAILED')",
            name="files_upload_state_check",
        ),
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
            "status IN ('PENDING', 'UPLOADING', 'AVAILABLE', 'CORRUPT', 'MISSING')",
            name="file_replicas_status_check",
        ),
        UniqueConstraint("node_id", "object_key", name="uq_replica_node_object"),
        Index("idx_file_replicas_file_id", "file_id"),
        Index("idx_file_replicas_node_id", "node_id"),
    )

    replica_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    file_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("files.file_id", ondelete="CASCADE"), nullable=False
    )
    node_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("storage_nodes.node_id"), nullable=False
    )
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
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
