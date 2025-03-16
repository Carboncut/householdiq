from celery import Celery
from sqlalchemy.orm import Session
from services.common_lib.database import SessionLocal
from services.common_lib.logging_config import logger
from services.common_lib.models import EphemeralEvent
from services.common_lib.aerospike_cache import AerospikeCache
from services.common_lib.bridging_logic import attempt_bridging
from services.common_lib.bridging_tokens import generate_bridging_token

import os

BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
BACKEND_URL = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

celery_app = Celery("householdiq_ingestion_tasks", broker=BROKER_URL, backend=BACKEND_URL)

@celery_app.task(name="tasks.queue_fuzzy_bridging")
def queue_fuzzy_bridging(event_id: int):
    """
    For events missing hashedEmail. We'll run bridging_fuzzy directly.
    """
    logger.info(f"Queue fuzzy bridging for event_id={event_id}")
    bridging_fuzzy(event_id)

def bridging_fuzzy(event_id: int):
    db = SessionLocal()
    try:
        ev = db.query(EphemeralEvent).filter(EphemeralEvent.id == event_id).first()
        if not ev:
            logger.error(f"No ephemeral event found for event_id={event_id}")
            return
        aero_client = AerospikeCache()
        attempt_bridging(ev, db, aero_client)
        logger.info(f"Fuzzy bridging complete for event_id={event_id}")
    except Exception as e:
        logger.exception(f"Error bridging fuzzy for event_id={event_id}: {e}")
    finally:
        db.close()

@celery_app.task(name="tasks.short_circuit_deterministic")
def short_circuit_deterministic(event_id: int, hashed_email: str):
    """
    Deterministic bridging if hashedEmail is present
    """
    db = SessionLocal()
    bridging_token = None
    try:
        ev = db.query(EphemeralEvent).filter(EphemeralEvent.id == event_id).first()
        if not ev:
            logger.error(f"Event not found: {event_id}")
            return None
        if ev.is_child or ev.device_child_flag:
            logger.info(f"Skip bridging for child event_id={event_id}")
            return None

        aero_client = AerospikeCache()
        # unify bridging
        attempt_bridging(ev, db, aero_client)
        bridging_token = generate_bridging_token(event_id, db)

        # Index event by hashedEmail
        aero_client.index_hashed_email(hashed_email, event_id)
        logger.info(f"Short-circuit bridging done for event_id={event_id} with hashed_email={hashed_email}")
    except Exception as e:
        logger.exception(f"Error short-circuit bridging event_id={event_id}: {e}")
    finally:
        db.close()
    return bridging_token
