"""add privacy columns to ephemeral_events

Revision ID: 2025_04_15_add_privacy_columns
Revises: 2025_04_01_add_bridging_config
Create Date: 2025-04-15
"""
from alembic import op
import sqlalchemy as sa

revision = '2025_04_15_privacy_columns'
down_revision = '2025_04_01_bridging_config'
branch_labels = None
depends_on = None

def upgrade():
    with op.batch_alter_table('ephemeral_events') as batch_op:
        batch_op.add_column(sa.Column('privacy_tcf_string', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('privacy_us_string', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('privacy_gpp_string', sa.String(), nullable=True))

def downgrade():
    with op.batch_alter_table('ephemeral_events') as batch_op:
        batch_op.drop_column('privacy_gpp_string')
        batch_op.drop_column('privacy_us_string')
        batch_op.drop_column('privacy_tcf_string')
