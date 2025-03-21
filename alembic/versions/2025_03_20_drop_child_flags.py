"""Drop child flag columns from ephemeral_events

Revision ID: 2025_03_20_drop_child_flags
Revises: 2025_09_01_child_flags
Create Date: 2025-03-20 15:10:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = '2025_03_20_drop_child_flags'
down_revision = '2025_09_01_child_flags'
branch_labels = None
depends_on = None

def upgrade():
    with op.batch_alter_table('ephemeral_events') as batch_op:
        batch_op.drop_column('is_child')
        batch_op.drop_column('device_child_flag')

def downgrade():
    with op.batch_alter_table('ephemeral_events') as batch_op:
        batch_op.add_column(sa.Column('is_child', sa.Boolean(), server_default=sa.text('false')))
        batch_op.add_column(sa.Column('device_child_flag', sa.Boolean(), server_default=sa.text('false'))) 