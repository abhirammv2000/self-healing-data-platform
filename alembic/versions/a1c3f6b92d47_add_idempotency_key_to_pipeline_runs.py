"""add_idempotency_key_to_pipeline_runs

Revision ID: a1c3f6b92d47
Revises: d8b82acd59ee
Create Date: 2026-09-24 05:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1c3f6b92d47'
down_revision: Union[str, Sequence[str], None] = 'd8b82acd59ee'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('pipeline_runs', sa.Column('idempotency_key', sa.String(length=255), nullable=True))
    # Plain UNIQUE(pipeline_id, idempotency_key): Postgres treats every NULL as distinct,
    # so runs that don't supply a key (the majority) never collide with each other.
    # Only two runs on the same pipeline with the same key collide.
    op.create_unique_constraint('uq_pipeline_runs_pipeline_id_idempotency_key', 'pipeline_runs', ['pipeline_id', 'idempotency_key'])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('uq_pipeline_runs_pipeline_id_idempotency_key', 'pipeline_runs', type_='unique')
    op.drop_column('pipeline_runs', 'idempotency_key')
