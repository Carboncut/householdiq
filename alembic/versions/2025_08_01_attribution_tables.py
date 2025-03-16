"""Add multi-touch attribution tables

Revision ID: 2025_08_01_add_attribution_tables
Revises: 2025_07_10_add_multitenant_fields
Create Date: 2025-08-01
"""
from alembic import op
import sqlalchemy as sa

revision = '2025_08_01_attribution_tables'
down_revision = '2025_07_10_multitenant_fields'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'attribution_journeys',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('household_id', sa.String(), nullable=False),
        sa.Column('conversion_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('touch_points', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

    op.create_table(
        'lookalike_segments',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('seed_segment', sa.String(), nullable=False),
        sa.Column('matched_households', sa.ARRAY(sa.String()), server_default='{}'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

def downgrade():
    op.drop_table('lookalike_segments')
    op.drop_table('attribution_journeys')
