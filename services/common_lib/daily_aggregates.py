from sqlalchemy.orm import Session
from services.common_lib.logging_config import logger
from services.common_lib.aerospike_cache import AerospikeCache
from services.common_lib.models import DailyAggregate
from services.common_lib.config import settings
from services.common_lib.privacy import apply_differential_privacy

def buffer_increment_daily_aggregate(aero_cache: AerospikeCache, date_str: str, partner_id: int, device_type: str, event_type: str):
    """
    Increments the count for a given date/partner/device/event by 1 in Aerospike buffer.
    """
    aero_cache.buffer_increment_daily_aggregate(date_str, partner_id, device_type, event_type)

def flush_daily_aggregate(aero_cache: AerospikeCache, db: Session):
    """
    Reads daily aggregates from Aerospike, inserts or updates them in Postgres.
    If DP_MODE_ENABLED is true, applies Laplace noise before storing.
    """
    def flush_fn(date_str, counts_map):
        for field_key, str_count in counts_map.items():
            raw_count = int(str_count)
            if settings.DP_MODE_ENABLED:
                final_count = apply_differential_privacy(raw_count)
            else:
                final_count = raw_count

            partner_id, device_type, event_type = field_key.split("|")
            partner_id = int(partner_id)
            upsert_daily_aggregate(db, date_str, partner_id, device_type, event_type, final_count)

    aero_cache.flush_daily_aggregate(flush_fn)

def upsert_daily_aggregate(db: Session, date_str: str, partner_id: int, device_type: str, event_type: str, increment: float):
    row = (db.query(DailyAggregate)
           .filter(DailyAggregate.date_str == date_str,
                   DailyAggregate.partner_id == partner_id,
                   DailyAggregate.device_type == device_type,
                   DailyAggregate.event_type == event_type)
           .first())
    if row:
        row.count += int(increment)
    else:
        row = DailyAggregate(
            date_str=date_str,
            partner_id=partner_id,
            device_type=device_type,
            event_type=event_type,
            count=int(increment)
        )
        db.add(row)
    db.commit()
