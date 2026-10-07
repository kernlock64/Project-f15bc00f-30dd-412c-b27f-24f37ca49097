"""empty message

Revision ID: 9fa7b214f6a6
Revises: c60e016b5fc8
Create Date: 2026-10-07 10:34:32.601524

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9fa7b214f6a6'
down_revision: Union[str, Sequence[str], None] = 'c60e016b5fc8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
