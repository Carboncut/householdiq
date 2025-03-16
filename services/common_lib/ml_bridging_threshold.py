from sqlalchemy.orm import Session
from services.common_lib.models import MLBridgingThreshold
from services.common_lib.logging_config import logger
from datetime import datetime

class MLBridgingThresholdManager:
    def __init__(self, db: Session):
        self.db = db

    def get_current_threshold(self) -> float:
        row = self.db.query(MLBridgingThreshold).order_by(MLBridgingThreshold.last_trained.desc()).first()
        if row:
            return row.threshold_value
        return None

    def retrain_model(self):
        new_threshold = 0.65
        ml = MLBridgingThreshold(model_version="v2", threshold_value=new_threshold)
        self.db.add(ml)
        self.db.commit()
        logger.info(f"ML bridging threshold retrained to {new_threshold}")
