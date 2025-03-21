from services.common_lib.aerospike_cache import AerospikeCache
from services.common_lib.logging_config import logger
from services.common_lib.database import SessionLocal
from services.common_lib.models import DailyAggregate
from datetime import datetime
from typing import Dict, Any

def trigger_flush():
    logger.info("Manually triggering daily aggregates flush")
    
    # Get today's date
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    # Connect to Aerospike
    aero = AerospikeCache()
    key = ('test', 'dailyAggSet', date_str)
    
    try:
        # Get the record from Aerospike
        _, _, record = aero.client.get(key)
        if not record:
            logger.info(f"No record found for {date_str}")
            return
            
        counts_map = record.get('counts', {})
        if not isinstance(counts_map, dict):
            logger.error(f"Invalid counts_map type: {type(counts_map)}")
            return
            
        logger.info(f"Found counts for {date_str}: {counts_map}")
        
        # Connect to Postgres using context manager
        with SessionLocal() as db:
            # Process each count
            for field_key, count in counts_map.items():
                try:
                    parts = field_key.split('|')
                    if len(parts) != 3:
                        logger.error(f"Invalid field_key format: {field_key}")
                        continue
                        
                    partner_id, device_type, event_type = parts
                    try:
                        partner_id = int(partner_id)
                        count = int(count)
                    except ValueError as e:
                        logger.error(f"Invalid numeric value in field_key: {e}")
                        continue
                    
                    # Upsert the record
                    row = (db.query(DailyAggregate)
                          .filter(DailyAggregate.date_str == date_str,
                                 DailyAggregate.partner_id == partner_id,
                                 DailyAggregate.device_type == device_type,
                                 DailyAggregate.event_type == event_type)
                          .first())
                          
                    if row:
                        row.count = count
                    else:
                        row = DailyAggregate(
                            date_str=date_str,
                            partner_id=partner_id,
                            device_type=device_type,
                            event_type=event_type,
                            count=count
                        )
                        db.add(row)
                    
                except Exception as e:
                    logger.error(f"Error processing field_key {field_key}: {e}")
                    continue
            
            db.commit()
            logger.info("Successfully flushed to Postgres")
            
            # Remove from Aerospike
            aero.client.remove(key)
            logger.info("Removed from Aerospike")
            
    except Exception as e:
        logger.error(f"Error during flush: {e}")
    
    logger.info("Flush complete")

if __name__ == "__main__":
    trigger_flush() 