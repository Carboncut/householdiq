from sqlalchemy.orm import Session
from services.common_lib.logging_config import logger
from services.common_lib.models import Partner

class TenantManager:
    def assign_namespace_to_partner(self, db: Session, partner: Partner, namespace: str):
        partner.namespace = namespace
        db.commit()
        logger.info(f"Assigned partner_id={partner.id} to namespace={namespace}")
