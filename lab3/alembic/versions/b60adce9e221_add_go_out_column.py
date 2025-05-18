"""Add go_out column

Revision ID: b60adce9e221
Revises: 6bef8c17b660
Create Date: 2025-05-18 12:53:19.134599

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b60adce9e221'
down_revision: Union[str, None] = '6bef8c17b660'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("weather", sa.Column("go_out", sa.Boolean))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("weather", "go_out")
