"""add_lease_and_enqueue_tracking_to_pipeline_runs

Revision ID: b7e2c4d91f30
Revises: a1c3f6b92d47
Create Date: 2026-11-11 21:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b7e2c4d91f30'
down_revision: Union[str, Sequence[str], None] = 'a1c3f6b92d47'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # When the run id was last put on the queue. A queued run that was never put there, or not for a while, is the
    # reaper's to push again. Both columns are timestamptz and are only compared with the database's own now(), so
    # they do not depend on the clock or the time zone of whichever machine wrote them.
    op.add_column('pipeline_runs', sa.Column('last_enqueued_at', sa.DateTime(timezone=True), nullable=True))
    # Until when the worker that claimed the run owns it. The worker extends it while it works. A running run whose
    # lease has passed belongs to a worker that stopped.
    op.add_column('pipeline_runs', sa.Column('lease_expires_at', sa.DateTime(timezone=True), nullable=True))
    # How many times a worker has claimed the run. A run that keeps losing its worker is failed instead of tried forever.
    op.add_column('pipeline_runs', sa.Column('attempts', sa.Integer(), server_default='0', nullable=False))
    # Only the runs that are not finished are ever scanned for recovery, so only those are indexed.
    op.create_index(
        'ix_pipeline_runs_unfinished',
        'pipeline_runs',
        ['status', 'lease_expires_at', 'last_enqueued_at'],
        postgresql_where=sa.text("status IN ('queued', 'running', 'retrying')"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('ix_pipeline_runs_unfinished', table_name='pipeline_runs')
    op.drop_column('pipeline_runs', 'attempts')
    op.drop_column('pipeline_runs', 'lease_expires_at')
    op.drop_column('pipeline_runs', 'last_enqueued_at')
