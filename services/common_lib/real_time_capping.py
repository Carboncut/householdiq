from sqlalchemy.orm import Session
from services.common_lib.models import FrequencyCapping, ConsentRevocation
from services.common_lib.logging_config import logger

class RealTimeCappingManager:
    def check_cap(self, db: Session, household_id: str):
        fc = db.query(FrequencyCapping).filter_by(household_id=household_id).first()
        if not fc:
            fc = FrequencyCapping(household_id=household_id, daily_impressions=0, cap_limit=5)
            db.add(fc)
            db.commit()
            db.refresh(fc)
        can_serve = (fc.daily_impressions < fc.cap_limit)
        return {
            "household_id": household_id,
            "daily_impressions": fc.daily_impressions,
            "cap_limit": fc.cap_limit,
            "can_serve": can_serve
        }

    def increment_impression(self, db: Session, household_id: str):
        fc = db.query(FrequencyCapping).filter_by(household_id=household_id).first()
        if not fc:
            fc = FrequencyCapping(household_id=household_id, daily_impressions=0, cap_limit=5)
            db.add(fc)
        else:
            fc.daily_impressions += 1
        db.commit()
        db.refresh(fc)
        can_serve = (fc.daily_impressions <= fc.cap_limit)
        return {
            "household_id": household_id,
            "daily_impressions": fc.daily_impressions,
            "cap_limit": fc.cap_limit,
            "can_serve": can_serve
        }

    def revoke_consent(self, db: Session, ephem_id: str):
        rev = ConsentRevocation(ephem_id=ephem_id)
        db.add(rev)
        db.commit()
        logger.info(f"Consent revoked for ephemeral_id={ephem_id}")
