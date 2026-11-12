"""allow_one_active_run_per_pipeline

Revision ID: d5a9b3e81c47
Revises: c4f8a1d27b65
Create Date: 2026-11-12 11:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd5a9b3e81c47'
down_revision: Union[str, Sequence[str], None] = 'c4f8a1d27b65'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Before this, workers could run several runs of one pipeline at once, so a database may already hold more than
    # one active run for a pipeline, and the unique index below could not be built. The newest active run of each
    # pipeline is left alone; the older ones go back to 'queued' so they run again, one after the other. That is what
    # taking over a run does anyway, and steps can be repeated. last_enqueued_at is emptied so the reaper queues them.
    op.execute(
        """
        UPDATE pipeline_runs
        SET status = 'queued', lease_expires_at = NULL, last_enqueued_at = NULL
        WHERE id IN (
            SELECT id FROM (
                SELECT id, row_number() OVER (PARTITION BY pipeline_id ORDER BY id DESC) AS newest_first
                FROM pipeline_runs
                WHERE status IN ('running', 'retrying')
            ) AS active
            WHERE newest_first > 1
        )
        """
    )
    # At most one run of a pipeline is running at a time. The index is the lock: a second claim of the same pipeline
    # fails in the database whichever worker makes it.
    op.create_index(
        'uq_pipeline_runs_one_active',
        'pipeline_runs',
        ['pipeline_id'],
        unique=True,
        postgresql_where=sa.text("status IN ('running', 'retrying')"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('uq_pipeline_runs_one_active', table_name='pipeline_runs')
