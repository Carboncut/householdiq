from celery import Celery, group
from services.common_lib.config import settings
from services.common_lib.database import SessionLocal
from services.common_lib.models import EphemeralEvent
from services.common_lib.logging_config import logger
from services.common_lib.bridging_logic import attempt_bridging
from services.common_lib.aerospike_cache import AerospikeCache
from services.common_lib.bridging_tokens import generate_bridging_token
from typing import List, Optional
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

celery_app = Celery(
    "householdiq_tasks",
    broker=f"amqp://aggregator:aggregator@{settings.RABBITMQ_HOST}:5672//",
    backend=None
)

# Configure Celery for better performance
celery_app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    task_time_limit=300,  # 5 minutes
    task_soft_time_limit=240,  # 4 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    result_expires=3600,  # 1 hour
    task_routes={
        'services.common_lib.tasks.queue_fuzzy_bridging': {'queue': 'bridging'},
        'services.common_lib.tasks.batch_fuzzy_bridging': {'queue': 'bridging'},
        'services.common_lib.tasks.short_circuit_deterministic': {'queue': 'bridging'},
        'services.common_lib.tasks_beat.flush_daily_agg_task': {'queue': 'celery'},
        'services.common_lib.tasks_beat.prune_neo4j_task': {'queue': 'celery'},
        'services.common_lib.tasks_beat.retrain_bridging_ml': {'queue': 'celery'}
    }
)

# Thread pool for async operations
executor = ThreadPoolExecutor(max_workers=10)

@celery_app.task(name="services.common_lib.tasks.queue_fuzzy_bridging")
def queue_fuzzy_bridging(event_id: int):
    """
    Celery task to enqueue an event for fuzzy bridging in Aerospike queue.
    """
    aero = AerospikeCache()
    asyncio.run(aero.enqueue_fuzzy_event(event_id))
    logger.info(f"Enqueued event {event_id} for fuzzy bridging batch.")

@celery_app.task(name="services.common_lib.tasks.batch_fuzzy_bridging")
def batch_fuzzy_bridging():
    """
    Called periodically to unify all queued events with fuzzy bridging in one pass.
    """
    logger.info("batch_fuzzy_bridging triggered.")
    aero = AerospikeCache()
    batch_ids = asyncio.run(aero.pop_fuzzy_batch())
    
    if not batch_ids:
        logger.info("No events to fuzzy-bridge in this batch.")
        return

    # Process events in parallel using Celery group
    tasks = group(bridging_fuzzy.s(event_id) for event_id in batch_ids)
    tasks.apply_async()

@celery_app.task(name="services.common_lib.tasks.bridging_fuzzy")
def bridging_fuzzy(event_id: int):
    """
    Process a single event for fuzzy bridging.
    """
    db = SessionLocal()
    try:
        ev = db.query(EphemeralEvent).filter(EphemeralEvent.id == event_id).first()
        if not ev:
            logger.error(f"No ephemeral event found for event_id={event_id}")
            return
        
        aero_client = AerospikeCache()
        asyncio.run(attempt_bridging(ev, db, aero_client))
        logger.info(f"Fuzzy bridging complete for event_id={event_id}")
    except Exception as e:
        logger.exception(f"Error bridging fuzzy for event_id={event_id}: {e}")
    finally:
        db.close()

@celery_app.task(name="services.common_lib.tasks.short_circuit_deterministic")
def short_circuit_deterministic(event_id: int, hashed_email: str) -> Optional[str]:
    """
    Deterministic bridging if hashedEmail is present.
    """
    db = SessionLocal()
    bridging_token = None
    try:
        ev = db.query(EphemeralEvent).filter(EphemeralEvent.id == event_id).first()
        if not ev:
            logger.error(f"Event not found: {event_id}")
            return None
        if ev.partial_keys.get('isChild', False) or ev.partial_keys.get('deviceChildFlag', False):
            logger.info(f"Skip bridging for child event_id={event_id}")
            return None

        aero_client = AerospikeCache()
        # unify bridging
        asyncio.run(attempt_bridging(ev, db, aero_client))
        bridging_token = generate_bridging_token(event_id, db)

        # Index event by hashedEmail
        asyncio.run(aero_client.index_hashed_email(hashed_email, event_id))
        logger.info(f"Short-circuit bridging done for event_id={event_id} with hashed_email={hashed_email}")
    except Exception as e:
        logger.exception(f"Error short-circuit bridging event_id={event_id}: {e}")
    finally:
        db.close()
    return bridging_token

# Cache for frequently accessed data
@lru_cache(maxsize=1000)
def get_event(event_id: int) -> Optional[EphemeralEvent]:
    """
    Cached event lookup.
    """
    db = SessionLocal()
    try:
        return db.query(EphemeralEvent).filter(EphemeralEvent.id == event_id).first()
    finally:
        db.close()
