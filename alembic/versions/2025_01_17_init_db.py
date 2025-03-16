"""init db

Revision ID: 2025_01_17_init_db
Revises: 
Create Date: 2025-01-17
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '2025_01_17_init_db'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # partners
    op.create_table(
        'partners',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(), unique=True, nullable=False),
        sa.Column('salt', sa.String(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

    # consent_flags
    op.create_table(
        'consent_flags',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('cross_device_bridging', sa.Boolean(), server_default=sa.text('true')),
        sa.Column('targeting_segments', sa.Boolean(), server_default=sa.text('true')),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

    # ephemeral_events
    op.create_table(
        'ephemeral_events',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('ephem_id', sa.String(), index=True),
        sa.Column('partial_keys', postgresql.JSON(astext_type=sa.Text()), server_default=sa.text("'{}'")),
        sa.Column('event_type', sa.String(), nullable=False, server_default='impression'),
        sa.Column('timestamp', sa.DateTime(timezone=True), server_default=sa.text('NOW()')),
        sa.Column('campaign_id', sa.String(), nullable=True),
        sa.Column('partner_id', sa.Integer(), sa.ForeignKey('partners.id'), nullable=False),
        sa.Column('consent_flags_id', sa.Integer(), sa.ForeignKey('consent_flags.id'), nullable=True)
    )

    # bridging_references
    op.create_table(
        'bridging_references',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('household_ephem_id', sa.String(), nullable=True),
        sa.Column('linked_ephem_ids', postgresql.ARRAY(sa.String()), server_default='{}'),
        sa.Column('confidence_score', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()')),
        sa.Column('expiry_timestamp', sa.DateTime(timezone=True), nullable=True)
    )

def downgrade():
    op.drop_table('bridging_references')
    op.drop_table('ephemeral_events')
    op.drop_table('consent_flags')
    op.drop_table('partners')
