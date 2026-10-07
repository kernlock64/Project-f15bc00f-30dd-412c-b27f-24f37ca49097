"""empty message

Revision ID: c60e016b5fc8
Revises: 06a57e20d298
Create Date: 2026-10-07 10:33:29.370864

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c60e016b5fc8'
down_revision: Union[str, Sequence[str], None] = '06a57e20d298'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
