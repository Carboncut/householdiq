from celery import Celery
from services.common_lib.config import settings
from services.common_lib.database import SessionLocal
from services.common_lib.models import EphemeralEvent
from services.common_lib.logging_config import logger
from services.common_lib.bridging_logic import attempt_bridging
from services.common_lib.aerospike_cache import AerospikeCache
from services.common_lib.bridging_tokens import generate_bridging_token

celery_app = Celery(
    "householdiq_tasks",
    broker=f"amqp://aggregator:aggregator@{settings.RABBITMQ_HOST}:5672//",
    backend=None
)

@celery_app.task(name="services.common_lib.tasks.queue_fuzzy_bridging")
def queue_fuzzy_bridging(event_id: int):
    """
    Celery task to enqueue an event for fuzzy bridging in Aerospike queue.
    """
    aero = AerospikeCache()
    aero.enqueue_fuzzy_event(event_id)
    logger.info(f"Enqueued event {event_id} for fuzzy bridging batch.")

@celery_app.task(name="services.common_lib.tasks.batch_fuzzy_bridging")
def batch_fuzzy_bridging():
    """
    Called periodically to unify all queued events with fuzzy bridging in one pass.
    """
    logger.info("batch_fuzzy_bridging triggered.")
    aero = AerospikeCache()
    batch_ids = aero.pop_fuzzy_batch()
    if not batch_ids:
        logger.info("No events to fuzzy-bridge in this batch.")
        return

    db = SessionLocal()
    try:
        for e_id_str in batch_ids:
            try:
                e_id = int(e_id_str)
                ev = db.query(EphemeralEvent).filter(EphemeralEvent.id == e_id).first()
                if ev:
                    attempt_bridging(ev, db, aero)
                else:
                    logger.warning(f"Event {e_id} not found in DB.")
            except Exception as ex:
                logger.exception(f"Error bridging event {e_id_str}: {ex}")
    finally:
        db.close()

@celery_app.task(name="services.common_lib.tasks.short_circuit_deterministic")
def short_circuit_deterministic(event_id: int, hashed_email: str):
    """
    If hashedEmail is present, we unify with existing events having same hashedEmail.
    Returns bridging token if bridging occurs.
    """
    db = SessionLocal()
    bridging_token = None
    try:
        ev = db.query(EphemeralEvent).filter(EphemeralEvent.id == event_id).first()
        if not ev:
            logger.error(f"No ephemeral event found with ID={event_id}")
            return None
        if ev.is_child or ev.device_child_flag:
            logger.info(f"Skipping bridging for child-labeled event {event_id}.")
            return None

        aero = AerospikeCache()
        existing_list = aero.get_indexed_event_ids_for_email(hashed_email)
        if existing_list:
            # bridging with them
            attempt_bridging(ev, db, aero)
            bridging_token = generate_bridging_token(ev.id, db)

        # Index current event
        aero.index_hashed_email(hashed_email, event_id)
        return bridging_token
    except Exception as e:
        logger.exception(f"short_circuit_deterministic error: {e}")
        return None
    finally:
        db.close()
