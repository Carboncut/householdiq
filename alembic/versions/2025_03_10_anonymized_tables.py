"""add anonymized_events, daily_aggregates

Revision ID: 2025_03_10_add_anonymized_tables
Revises: 2025_02_01_add_data_sharing
Create Date: 2025-03-10
"""
from alembic import op
import sqlalchemy as sa

revision = '2025_03_10_anonymized_tables'
down_revision = '2025_02_01_data_sharing'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'anonymized_events',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('event_id', sa.Integer(), nullable=False),
        sa.Column('hashed_device_sig', sa.String(), nullable=True),
        sa.Column('hashed_user_sig', sa.String(), nullable=True),
        sa.Column('event_day', sa.String(), nullable=True),
        sa.Column('event_type', sa.String(), nullable=True),
        sa.Column('partner_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

    op.create_table(
        'daily_aggregates',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('date_str', sa.String(), nullable=False),
        sa.Column('partner_id', sa.Integer(), nullable=False),
        sa.Column('device_type', sa.String(), nullable=False),
        sa.Column('event_type', sa.String(), nullable=False),
        sa.Column('count', sa.Integer(), nullable=False, server_default="0"),
        sa.Column('last_updated', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

def downgrade():
    op.drop_table('daily_aggregates')
    op.drop_table('anonymized_events')
