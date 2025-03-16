"""add bridging_config table

Revision ID: 2025_04_01_add_bridging_config
Revises: 2025_03_10_add_anonymized_tables
Create Date: 2025-04-01
"""
from alembic import op
import sqlalchemy as sa

revision = '2025_04_01_bridging_config'
down_revision = '2025_03_10_anonymized_tables'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'bridging_config',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('threshold', sa.Float(), nullable=True),
        sa.Column('partial_key_weights', sa.JSON(), nullable=True),
        sa.Column('last_updated', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

def downgrade():
    op.drop_table('bridging_config')
