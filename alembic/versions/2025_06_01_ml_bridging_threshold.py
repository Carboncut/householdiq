"""Add table for ML bridging thresholds

Revision ID: 2025_06_01_add_ml_bridging_threshold
Revises: 2025_05_20_add_frequency_capping
Create Date: 2025-06-01
"""
from alembic import op
import sqlalchemy as sa

revision = '2025_06_01_ml_bridging_threshold'
down_revision = '2025_05_20_frequency_capping'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'ml_bridging_thresholds',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('model_version', sa.String(), nullable=False),
        sa.Column('threshold_value', sa.Float(), nullable=False, server_default="0.7"),
        sa.Column('last_trained', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

    with op.batch_alter_table('bridging_config') as batch_op:
        batch_op.add_column(sa.Column('time_decay_factor', sa.Float(), server_default="0.5"))

def downgrade():
    with op.batch_alter_table('bridging_config') as batch_op:
        batch_op.drop_column('time_decay_factor')

    op.drop_table('ml_bridging_thresholds')
