"""add data sharing agreements

Revision ID: 2025_02_01_add_data_sharing
Revises: 2025_01_17_init_db
Create Date: 2025-02-01
"""
from alembic import op
import sqlalchemy as sa

revision = '2025_02_01_data_sharing'
down_revision = '2025_01_17_init_db'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'data_sharing_agreements',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('partner_id_initiator', sa.Integer(), sa.ForeignKey("partners.id"), nullable=False),
        sa.Column('partner_id_recipient', sa.Integer(), sa.ForeignKey("partners.id"), nullable=False),
        sa.Column('agreement_details', sa.Text(), nullable=True),
        sa.Column('start_date', sa.DateTime(timezone=True), nullable=True),
        sa.Column('end_date', sa.DateTime(timezone=True), nullable=True),
        sa.Column('allow_aggregated_data_sharing', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('min_k_anonymity', sa.Integer(), nullable=False, server_default="50"),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'))
    )

def downgrade():
    op.drop_table('data_sharing_agreements')
