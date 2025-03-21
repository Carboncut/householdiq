from services.common_lib.aerospike_cache import AerospikeCache
from services.common_lib.logging_config import logger
from datetime import datetime
from typing import Optional, Dict, Any

def check_daily_aggs(namespace: str = 'test', date_str: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Check daily aggregates in Aerospike for a given date.
    
    Args:
        namespace: The Aerospike namespace to check
        date_str: The date to check in YYYY-MM-DD format. If None, uses today.
        
    Returns:
        The record if found, None otherwise
    """
    aero = AerospikeCache()
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')
        
    key = (namespace, 'dailyAggSet', date_str)
    
    try:
        _, _, rec = aero.client.get(key)
        if rec:
            logger.info(f'Found record for {date_str}: {rec}')
            return rec
        else:
            logger.info(f'No record found for {date_str}')
            return None
    except Exception as e:
        logger.error(f'Error checking Aerospike record: {e}')
        return None

if __name__ == "__main__":
    result = check_daily_aggs()
    if result:
        print(f'Found record: {result}')
    else:
        print('No record found') 