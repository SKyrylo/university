"""Create Weather Table

Revision ID: 6bef8c17b660
Revises: 
Create Date: 2025-05-18 12:33:14.326317

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6bef8c17b660'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "weather",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("country", sa.String(50)),
        sa.Column("wind_degree", sa.Integer()),
        sa.Column("wind_kph", sa.Float()),
        sa.Column("wind_direction", sa.String(3)),
        sa.Column("last_updated", sa.String(16)),
        sa.Column("sunrise", sa.String(8)),
        sa.Column("precip_mm", sa.Float())
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("weather")
