"""Add frequency capping tables

Revision ID: 2025_05_20_add_frequency_capping
Revises: 2025_04_15_add_privacy_columns
Create Date: 2025-05-20
"""
from alembic import op
import sqlalchemy as sa

revision = '2025_05_20_frequency_capping'
down_revision = '2025_04_15_privacy_columns'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'frequency_capping',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('household_id', sa.String(), nullable=False),
        sa.Column('daily_impressions', sa.Integer(), nullable=False, server_default="0"),
        sa.Column('cap_limit', sa.Integer(), nullable=False, server_default="5"),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

    op.create_table(
        'consent_revocations',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('ephem_id', sa.String(), nullable=False),
        sa.Column('revoked_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

def downgrade():
    op.drop_table('consent_revocations')
    op.drop_table('frequency_capping')
