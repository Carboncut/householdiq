from sqlalchemy.orm import Session
from datetime import datetime
from services.common_lib.models import AttributionJourney, LookalikeSegment
from services.common_lib.logging_config import logger

class AttributionManager:
    def record_conversion(self, db: Session, household_id: str, touches: list):
        journey = AttributionJourney(
            household_id=household_id,
            conversion_time=datetime.utcnow(),
            touch_points=touches
        )
        db.add(journey)
        db.commit()
        logger.info(f"Recorded multi-touch journey for household {household_id}")

    def build_lookalike_segment(self, db: Session, seed_segment: str, matched_list: list):
        seg = LookalikeSegment(
            seed_segment=seed_segment,
            matched_households=matched_list
        )
        db.add(seg)
        db.commit()
        logger.info(f"Created lookalike segment from {seed_segment}, with {len(matched_list)} households.")
