"""add channel hash and region metadata to messages

Revision ID: 9a1c2d3e4f5a
Revises: 4e2e787a1660
Create Date: 2026-03-13 14:15:00.000000+00:00

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9a1c2d3e4f5a"
down_revision: Union[str, None] = "4e2e787a1660"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("messages", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("channel_hash", sa.String(length=6), nullable=True)
        )
        batch_op.add_column(
            sa.Column("channel_region_flag", sa.Integer(), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("messages", schema=None) as batch_op:
        batch_op.drop_column("channel_region_flag")
        batch_op.drop_column("channel_hash")
