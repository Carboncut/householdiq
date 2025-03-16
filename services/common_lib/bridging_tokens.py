import jwt
import time
from datetime import timedelta
from services.common_lib.config import settings
from services.common_lib.models import EphemeralEvent
from services.common_lib.database import SessionLocal
from services.common_lib.aerospike_cache import AerospikeCache
from services.common_lib.logging_config import logger

JWT_SECRET = "HOUSEHOLDIQ_TOKEN_SECRET"
JWT_ALGO = "HS256"
TOKEN_EXPIRY_HOURS = 24

def generate_bridging_token(event_id: int, db) -> str:
    ev = db.query(EphemeralEvent).filter(EphemeralEvent.id == event_id).first()
    if not ev:
        return None
    aero_cache = AerospikeCache()
    hh = aero_cache.get_household_ref(ev.ephem_id)
    if not hh:
        return None
    now_ts = int(time.time())
    exp_ts = now_ts + (TOKEN_EXPIRY_HOURS * 3600)
    payload = {
        "sub": ev.ephem_id,
        "household": hh,
        "iat": now_ts,
        "exp": exp_ts,
        "ver": "1.0"
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)
    logger.debug(f"Issued bridging token for event_id={event_id}, household={hh}")
    return token
