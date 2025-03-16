"""Add multi-tenant fields for partner/namespace isolation

Revision ID: 2025_07_10_add_multitenant_fields
Revises: 2025_06_01_add_ml_bridging_threshold
Create Date: 2025-07-10
"""
from alembic import op
import sqlalchemy as sa

revision = '2025_07_10_multitenant_fields'
down_revision = '2025_06_01_ml_bridging_threshold'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('partners', sa.Column('namespace', sa.String(), nullable=True))
    op.add_column('ephemeral_events', sa.Column('tenant_namespace', sa.String(), nullable=True))

def downgrade():
    with op.batch_alter_table('ephemeral_events') as batch_op:
        batch_op.drop_column('tenant_namespace')

    with op.batch_alter_table('partners') as batch_op:
        batch_op.drop_column('namespace')
