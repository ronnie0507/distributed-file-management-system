"""Add persistent refresh-token sessions and audit query index.

Revision ID: 0003_phase6_auth_sessions
Revises: 0002_phase5_coordination
"""

from alembic import op
import sqlalchemy as sa


revision = "0003_phase6_auth_sessions"
down_revision = "0002_phase5_coordination"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "refresh_sessions",
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("family_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("refresh_token_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("replaced_by_session_id", sa.Uuid(), nullable=True),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.user_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("session_id"),
        sa.UniqueConstraint(
            "refresh_token_hash",
            name="uq_refresh_sessions_token_hash",
        ),
    )
    op.create_index(
        "idx_refresh_sessions_family_id",
        "refresh_sessions",
        ["family_id"],
    )
    op.create_index(
        "idx_refresh_sessions_user_id",
        "refresh_sessions",
        ["user_id"],
    )
    op.create_index(
        "idx_refresh_sessions_expires_at",
        "refresh_sessions",
        ["expires_at"],
    )
    op.create_index(
        "idx_audit_logs_created_at",
        "audit_logs",
        ["created_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_audit_logs_created_at", table_name="audit_logs")
    op.drop_index("idx_refresh_sessions_expires_at", table_name="refresh_sessions")
    op.drop_index("idx_refresh_sessions_user_id", table_name="refresh_sessions")
    op.drop_index("idx_refresh_sessions_family_id", table_name="refresh_sessions")
    op.drop_table("refresh_sessions")
