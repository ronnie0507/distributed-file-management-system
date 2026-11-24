"""Persist verified, versioned replicas and durable replication jobs.

Revision ID: 0001_phase4_replication
Revises:
"""

from alembic import op
import sqlalchemy as sa


revision = "0001_phase4_replication"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "files",
        sa.Column(
            "availability_state",
            sa.String(length=20),
            nullable=False,
            server_default="UNAVAILABLE",
        ),
    )
    op.add_column(
        "files",
        sa.Column(
            "replication_factor",
            sa.Integer(),
            nullable=False,
            server_default="3",
        ),
    )
    op.create_check_constraint(
        "files_availability_state_check",
        "files",
        "availability_state IN ('AVAILABLE', 'DEGRADED', 'UNAVAILABLE')",
    )
    op.create_check_constraint(
        "files_replication_factor_check",
        "files",
        "replication_factor > 0",
    )

    op.drop_constraint("file_replicas_status_check", "file_replicas", type_="check")
    op.add_column(
        "file_replicas",
        sa.Column("version_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "file_replicas",
        sa.Column("checksum", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "file_replicas",
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.execute(
        sa.text(
            """
            UPDATE file_replicas AS replica
            SET version_id = version.version_id,
                checksum = version.checksum,
                status = CASE
                    WHEN replica.status = 'AVAILABLE' THEN 'PENDING'
                    WHEN replica.status = 'UPLOADING' THEN 'PENDING'
                    ELSE replica.status
                END
            FROM files AS file
            JOIN file_versions AS version
              ON version.file_id = file.file_id
             AND version.version_number = file.current_version
            WHERE replica.file_id = file.file_id
            """
        )
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM file_replicas WHERE version_id IS NULL
            ) THEN
                RAISE EXCEPTION
                    'Cannot migrate replicas without a matching file version';
            END IF;
        END
        $$;
        """
    )

    op.alter_column("file_replicas", "version_id", nullable=False)
    op.alter_column("file_replicas", "checksum", nullable=False)
    op.create_foreign_key(
        "fk_file_replicas_version_id",
        "file_replicas",
        "file_versions",
        ["version_id"],
        ["version_id"],
        ondelete="CASCADE",
    )
    op.create_unique_constraint(
        "uq_replica_version_node",
        "file_replicas",
        ["version_id", "node_id"],
    )
    op.create_index(
        "idx_file_replicas_version_id",
        "file_replicas",
        ["version_id"],
    )
    op.create_check_constraint(
        "file_replicas_status_check",
        "file_replicas",
        "status IN ('PENDING', 'COPYING', 'VERIFIED', 'FAILED', 'DELETED', "
        "'CORRUPT', 'MISSING')",
    )

    op.create_table(
        "replication_jobs",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("file_id", sa.Uuid(), nullable=False),
        sa.Column("version_id", sa.Uuid(), nullable=False),
        sa.Column("target_count", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="PENDING"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5"),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint("target_count > 0", name="replication_jobs_target_count_check"),
        sa.CheckConstraint("attempt_count >= 0", name="replication_jobs_attempt_count_check"),
        sa.CheckConstraint("max_attempts > 0", name="replication_jobs_max_attempts_check"),
        sa.CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'RETRY', 'COMPLETED', 'DEAD')",
            name="replication_jobs_status_check",
        ),
        sa.ForeignKeyConstraint(
            ["file_id"], ["files.file_id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["version_id"], ["file_versions.version_id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("job_id"),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_replication_job_idempotency",
        ),
    )
    op.create_index(
        "idx_replication_jobs_due",
        "replication_jobs",
        ["status", "available_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_replication_jobs_due", table_name="replication_jobs")
    op.drop_table("replication_jobs")

    op.drop_constraint("file_replicas_status_check", "file_replicas", type_="check")
    op.execute(
        sa.text(
            """
            UPDATE file_replicas
            SET status = CASE
                WHEN status = 'VERIFIED' THEN 'AVAILABLE'
                WHEN status = 'COPYING' THEN 'UPLOADING'
                WHEN status IN ('FAILED', 'DELETED') THEN 'MISSING'
                ELSE status
            END
            """
        )
    )
    op.create_check_constraint(
        "file_replicas_status_check",
        "file_replicas",
        "status IN ('PENDING', 'UPLOADING', 'AVAILABLE', 'CORRUPT', 'MISSING')",
    )
    op.drop_index("idx_file_replicas_version_id", table_name="file_replicas")
    op.drop_constraint("uq_replica_version_node", "file_replicas", type_="unique")
    op.drop_constraint(
        "fk_file_replicas_version_id",
        "file_replicas",
        type_="foreignkey",
    )
    op.drop_column("file_replicas", "error_message")
    op.drop_column("file_replicas", "checksum")
    op.drop_column("file_replicas", "version_id")
    op.drop_constraint("files_replication_factor_check", "files", type_="check")
    op.drop_constraint("files_availability_state_check", "files", type_="check")
    op.drop_column("files", "replication_factor")
    op.drop_column("files", "availability_state")
