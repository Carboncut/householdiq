from sqlalchemy.orm import Session
from services.common_lib.models import BridgingConfig
from services.common_lib.config import settings
from services.common_lib.ml_bridging_threshold import MLBridgingThresholdManager

DEFAULT_PARTIAL_KEY_WEIGHTS = {
    "hashedEmail": 1.0,
    "hashedIP": 0.9,
    "wifiSSID": 0.3,
    "deviceType": 0.2,
    "profileID": 0.2
}

def get_bridging_threshold(db: Session) -> float:
    ml_manager = MLBridgingThresholdManager(db)
    ml_threshold = ml_manager.get_current_threshold()
    if ml_threshold is not None:
        return ml_threshold
    cfg = db.query(BridgingConfig).order_by(BridgingConfig.last_updated.desc()).first()
    if cfg and cfg.threshold is not None:
        return cfg.threshold
    return settings.BRIDGING_CONFIDENCE_THRESHOLD

def get_partial_key_weights(db: Session) -> dict:
    cfg = db.query(BridgingConfig).order_by(BridgingConfig.last_updated.desc()).first()
    if cfg and cfg.partial_key_weights:
        return cfg.partial_key_weights
    return DEFAULT_PARTIAL_KEY_WEIGHTS
