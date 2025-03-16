"""Add plugin registry & webhooks tables

Revision ID: 2025_08_15_add_plugins_webhooks
Revises: 2025_08_01_add_attribution_tables
Create Date: 2025-08-15
"""
from alembic import op
import sqlalchemy as sa

revision = '2025_08_15_plugins_webhooks'
down_revision = '2025_08_01_attribution_tables'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'plugin_registry',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('plugin_name', sa.String(), nullable=False),
        sa.Column('plugin_path', sa.String(), nullable=False),
        sa.Column('enabled', sa.Boolean(), server_default=sa.text('true'))
    )

    op.create_table(
        'webhook_subscriptions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('subscriber_name', sa.String(), nullable=False),
        sa.Column('callback_url', sa.String(), nullable=False),
        sa.Column('event_type', sa.String(), nullable=False),
        sa.Column('active', sa.Boolean(), server_default=sa.text('true')),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

def downgrade():
    op.drop_table('webhook_subscriptions')
    op.drop_table('plugin_registry')
