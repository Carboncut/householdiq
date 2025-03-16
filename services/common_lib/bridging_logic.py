from sqlalchemy.orm import Session
from services.common_lib.config import settings
from services.common_lib.logging_config import logger
from services.common_lib.aerospike_cache import AerospikeCache
from services.common_lib.models import EphemeralEvent
from services.common_lib.bridging_graph import unified_neo4j_bridging_advanced

def attempt_bridging(new_event: EphemeralEvent, db: Session, aero_client: AerospikeCache):
    if not new_event.consent_flags or not new_event.consent_flags.cross_device_bridging:
        logger.debug(f"Skipping bridging for event {new_event.id}: no cross_device_bridging.")
        return
    if new_event.is_child or new_event.device_child_flag:
        logger.info(f"Skipping bridging for event {new_event.id}: child flag.")
        return
    if not settings.USE_NEO4J_BRIDGING:
        logger.debug("Neo4j bridging disabled by config.")
        return

    logger.info(f"Performing bridging for event_id={new_event.id}")
    unified_neo4j_bridging_advanced(new_event, db, aero_client)
