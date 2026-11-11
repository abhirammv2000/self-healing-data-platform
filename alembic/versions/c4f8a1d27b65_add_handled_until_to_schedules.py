"""add_handled_until_to_schedules

Revision ID: c4f8a1d27b65
Revises: b7e2c4d91f30
Create Date: 2026-11-12 09:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c4f8a1d27b65'
down_revision: Union[str, Sequence[str], None] = 'b7e2c4d91f30'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Every fire time up to this moment has been dealt with: a run was started for it, or it was passed over on
    # purpose (the schedule was just created, changed or switched back on). The scheduler starts a run only for a
    # fire time after this. timestamptz, and compared with the database's own now(), so no machine's clock decides.
    op.add_column('schedules', sa.Column('handled_until', sa.DateTime(timezone=True), nullable=True))
    # Schedules that exist now start from this moment. Without it the scheduler would not know where to start from.
    op.execute("UPDATE schedules SET handled_until = now()")


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('schedules', 'handled_until')
