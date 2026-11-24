"""Add transactional job publication and worker fencing.

Revision ID: 0002_phase5_coordination
Revises: 0001_phase4_replication
"""

from alembic import op
import sqlalchemy as sa


revision = "0002_phase5_coordination"
down_revision = "0001_phase4_replication"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "files",
        sa.Column("work_lock_token", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "files",
        sa.Column("work_locked_until", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "replication_jobs",
        sa.Column(
            "job_type",
            sa.String(length=20),
            nullable=False,
            server_default="REPLICATE",
        ),
    )
    op.add_column(
        "replication_jobs",
        sa.Column(
            "dispatch_generation",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "replication_jobs",
        sa.Column("lock_token", sa.Uuid(), nullable=True),
    )
    op.create_check_constraint(
        "replication_jobs_type_check",
        "replication_jobs",
        "job_type IN ('REPLICATE', 'VERIFY', 'CLEANUP', 'RECONCILE')",
    )
    op.create_check_constraint(
        "replication_jobs_dispatch_generation_check",
        "replication_jobs",
        "dispatch_generation >= 0",
    )

    op.create_table(
        "outbox_events",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("event_key", sa.String(length=300), nullable=False),
        sa.Column(
            "event_type",
            sa.String(length=80),
            nullable=False,
            server_default="background_job",
        ),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            server_default="PENDING",
        ),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lock_token", sa.Uuid(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.CheckConstraint(
            "status IN ('PENDING', 'PUBLISHING', 'PUBLISHED')",
            name="outbox_events_status_check",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name="outbox_events_attempt_count_check",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["replication_jobs.job_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("event_id"),
        sa.UniqueConstraint("event_key", name="uq_outbox_event_key"),
    )
    op.create_index(
        "idx_outbox_events_due",
        "outbox_events",
        ["status", "available_at"],
    )
    op.create_index(
        "idx_outbox_events_job_id",
        "outbox_events",
        ["job_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_outbox_events_job_id", table_name="outbox_events")
    op.drop_index("idx_outbox_events_due", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_constraint(
        "replication_jobs_dispatch_generation_check",
        "replication_jobs",
        type_="check",
    )
    op.drop_constraint(
        "replication_jobs_type_check",
        "replication_jobs",
        type_="check",
    )
    op.drop_column("replication_jobs", "lock_token")
    op.drop_column("replication_jobs", "dispatch_generation")
    op.drop_column("replication_jobs", "job_type")
    op.drop_column("files", "work_locked_until")
    op.drop_column("files", "work_lock_token")
