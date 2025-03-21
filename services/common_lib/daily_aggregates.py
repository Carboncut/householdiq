from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from services.common_lib.logging_config import logger
from services.common_lib.aerospike_cache import AerospikeCache
from services.common_lib.models import DailyAggregate
from services.common_lib.config import settings
from services.common_lib.privacy import apply_differential_privacy
from typing import Dict, List, Tuple, Optional
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from datetime import datetime

# Connection pooling setup
engine = create_engine(
    settings.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800
)

# Thread pool for async operations
executor = ThreadPoolExecutor(max_workers=10)

BATCH_SIZE = 100  # Number of records to process in a batch

async def buffer_increment_daily_aggregate(
    aero_cache: AerospikeCache,
    date_str: str,
    partner_id: int,
    device_type: str,
    event_type: str
):
    """
    Increments the count for a given date/partner/device/event by 1 in Aerospike buffer.
    """
    try:
        await aero_cache.buffer_increment_daily_aggregate(
            date_str,
            partner_id,
            device_type,
            event_type
        )
    except Exception as e:
        logger.error(f"Error buffering daily aggregate: {e}")
        raise

async def _process_daily_aggregate_batch(
    db: Session,
    date_str: str,
    counts_map: Dict[str, int]
):
    """Process a batch of daily aggregates."""
    try:
        for field_key, count in counts_map.items():
            try:
                partner_id, device_type, event_type = field_key.split('|')
                partner_id = int(partner_id)

                # Apply differential privacy if enabled
                private_count = await asyncio.get_event_loop().run_in_executor(
                    executor,
                    lambda: apply_differential_privacy(count)
                )

                # Upsert the aggregate
                stmt = text("""
                    INSERT INTO daily_aggregates 
                    (date_str, partner_id, device_type, event_type, count)
                    VALUES (:date_str, :partner_id, :device_type, :event_type, :count)
                    ON CONFLICT (date_str, partner_id, device_type, event_type)
                    DO UPDATE SET count = :count
                """)

                await asyncio.get_event_loop().run_in_executor(
                    executor,
                    lambda: db.execute(
                        stmt,
                        {
                            'date_str': date_str,
                            'partner_id': partner_id,
                            'device_type': device_type,
                            'event_type': event_type,
                            'count': private_count
                        }
                    )
                )

            except Exception as e:
                logger.error(f"Error processing aggregate for {field_key}: {e}")
                continue

        await asyncio.get_event_loop().run_in_executor(
            executor,
            db.commit
        )

    except Exception as e:
        logger.error(f"Error processing daily aggregate batch: {e}")
        await asyncio.get_event_loop().run_in_executor(
            executor,
            db.rollback
        )
        raise

async def flush_daily_aggregate(aero_cache: AerospikeCache, db: Session):
    """
    Flush daily aggregates from Aerospike to Postgres with proper async handling.
    """
    try:
        async def flush_fn(date_str: str, counts_map: Dict[str, int]):
            await _process_daily_aggregate_batch(db, date_str, counts_map)

        await aero_cache.flush_daily_aggregate(flush_fn)

    except Exception as e:
        logger.error(f"Error during daily aggregate flush: {e}")
        raise

@lru_cache(maxsize=1000)
async def get_daily_aggregate(
    date_str: str,
    partner_id: int,
    device_type: str,
    event_type: str
) -> Optional[DailyAggregate]:
    """
    Cached daily aggregate lookup.
    """
    db = Session(engine)
    try:
        return db.query(DailyAggregate).filter(
            DailyAggregate.date_str == date_str,
            DailyAggregate.partner_id == partner_id,
            DailyAggregate.device_type == device_type,
            DailyAggregate.event_type == event_type
        ).first()
    finally:
        db.close()
