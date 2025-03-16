import math
import json  # <-- import json for serialization
from datetime import timedelta
from typing import List
from neo4j import GraphDatabase
from sqlalchemy.orm import Session

from services.common_lib.config import settings
from services.common_lib.logging_config import logger
from services.common_lib.hashing import hash_sha256
from services.common_lib.aerospike_cache import AerospikeCache  # or from aggregator. but let's see...
from services.common_lib.fuzzy import fuzzy_similarity
from services.common_lib.bridging_config_manager import get_bridging_threshold, get_partial_key_weights
from services.common_lib.models import EphemeralEvent, BridgingConfig

class Neo4jAdvancedLinker:
    """
    Handles advanced bridging steps: merges ephemeral events in Neo4j, calculates similarity,
    and connects them to device/user/household nodes.
    """
    def __init__(self):
        self.driver = GraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
        )

    def close(self):
        self.driver.close()

    def merge_event_node(self, event: EphemeralEvent):
        """
        Creates or updates an Event node in Neo4j, storing partial_keys, timestamp, etc.

        *IMPORTANT:* partial_keys must be stored in an allowed data format (e.g. string).
        """
        with self.driver.session() as session:
            # Convert the Python dict to JSON string:
            partial_keys_json = json.dumps(event.partial_keys) if event.partial_keys else "{}"

            session.run("""
                MERGE (e:Event {id: $eid})
                ON CREATE SET e.createdAt = timestamp(),
                              e.partialKeys = $partialKeysJSON,
                              e.timestamp = $timestamp
                SET e.lastSeen = timestamp()
            """,
            eid=event.ephem_id,
            partialKeysJSON=partial_keys_json,  # Passing JSON-serialized dict
            timestamp=str(event.timestamp))

    def compute_confidence(self, pk_a: dict, pk_b: dict, t_a, t_b, partial_key_weights: dict, time_decay_factor: float) -> float:
        """
        Combines partial key similarities with time-decay. If hashedEmail matches exactly, can short-circuit or raise score.
        """
        # If hashedEmail matches, we can consider that near 1.0
        if pk_a.get("hashedEmail") and pk_b.get("hashedEmail") and pk_a["hashedEmail"].lower() == pk_b["hashedEmail"].lower():
            return 1.0

        from datetime import datetime
        time_diff_hours = abs((t_a - t_b).total_seconds() / 3600.0)
        # Exponential decay based on time difference
        recency_factor = time_decay_factor ** (time_diff_hours / 24.0)

        score = 0.0
        for key, weight in partial_key_weights.items():
            val_a = pk_a.get(key, "").lower()
            val_b = pk_b.get(key, "").lower()
            if not val_a or not val_b:
                continue
            if key == "hashedEmail" and val_a == val_b:
                # If hashedEmail is same, that is near deterministic
                score += weight
            else:
                # fuzzy
                sim = fuzzy_similarity(val_a, val_b)
                score += weight * sim * recency_factor

        return min(score, 1.0)

    def link_event_to_device(self, event_id: str, device_id: str, confidence: float):
        with self.driver.session() as session:
            session.run("""
                MERGE (d:Device {device_id: $did})
                ON CREATE SET d.createdAt = timestamp()
                SET d.lastUpdated = timestamp()
                MERGE (e:Event {id: $eid})
                MERGE (e)-[r:FROM_DEVICE]->(d)
                ON CREATE SET r.createdAt = timestamp(), r.confidence = $confidence
                SET r.lastUpdated = timestamp(),
                    r.confidence = CASE WHEN r.confidence < $confidence THEN $confidence ELSE r.confidence END
            """, eid=event_id, did=device_id, confidence=confidence)

    def link_device_to_user(self, device_id: str, user_id: str, confidence: float):
        with self.driver.session() as session:
            session.run("""
                MERGE (u:User {user_id: $uid})
                ON CREATE SET u.createdAt = timestamp()
                SET u.lastUpdated = timestamp()
                MERGE (d:Device {device_id: $did})
                MERGE (d)-[r:USED_BY]->(u)
                ON CREATE SET r.createdAt = timestamp(), r.confidence = $confidence
                SET r.lastUpdated = timestamp(),
                    r.confidence = CASE WHEN r.confidence < $confidence THEN $confidence ELSE r.confidence END
            """, uid=user_id, did=device_id, confidence=confidence)

    def link_user_to_household(self, user_id: str, household_id: str, confidence: float):
        with self.driver.session() as session:
            session.run("""
                MERGE (h:Household {household_id: $hid})
                ON CREATE SET h.createdAt = timestamp()
                SET h.lastUpdated = timestamp()
                MERGE (u:User {user_id: $uid})
                MERGE (u)-[r:MEMBER_OF]->(h)
                ON CREATE SET r.createdAt = timestamp(), r.confidence = $confidence
                SET r.lastUpdated = timestamp(),
                    r.confidence = CASE WHEN r.confidence < $confidence THEN $confidence ELSE r.confidence END
            """, uid=user_id, hid=household_id, confidence=confidence)

    def advanced_bridging(self, new_event: EphemeralEvent, recent_events: List[EphemeralEvent], aero_cache, db: Session):
        """
        Merges new_event node, then tries to link it with each recent event if bridging is allowed.
        """
        # First, MERGE or update the new_event node in Neo4j:
        self.merge_event_node(new_event)

        bc = db.query(BridgingConfig).order_by(BridgingConfig.last_updated.desc()).first()
        time_decay_factor = bc.time_decay_factor if (bc and bc.time_decay_factor) else 0.5

        threshold = get_bridging_threshold(db)
        partial_weights = get_partial_key_weights(db)

        for ev in recent_events:
            if ev.id == new_event.id:
                continue
            if not ev.consent_flags or not ev.consent_flags.cross_device_bridging:
                continue
            if ev.is_child or ev.device_child_flag:
                continue

            score = self.compute_confidence(
                new_event.partial_keys,
                ev.partial_keys,
                new_event.timestamp,
                ev.timestamp,
                partial_weights,
                time_decay_factor
            )
            if score < threshold:
                continue

            # build device signatures
            dev_sig_a = (new_event.partial_keys.get("hashedIP", "") + new_event.partial_keys.get("deviceType", "")).lower()
            dev_sig_b = (ev.partial_keys.get("hashedIP", "") + ev.partial_keys.get("deviceType", "")).lower()

            device_id_a = hash_sha256(dev_sig_a + "device")
            device_id_b = hash_sha256(dev_sig_b + "device")

            wifi_a = new_event.partial_keys.get("wifiSSID", "")
            wifi_b = ev.partial_keys.get("wifiSSID", "")

            # link event->device
            if wifi_a and wifi_b and wifi_a.lower() == wifi_b.lower():
                merged_dev = hash_sha256(wifi_a + "mergedDevice")
                self.link_event_to_device(new_event.ephem_id, merged_dev, score)
                self.link_event_to_device(ev.ephem_id, merged_dev, score)
                dev_id_a_final = merged_dev
                dev_id_b_final = merged_dev
            else:
                self.link_event_to_device(new_event.ephem_id, device_id_a, score)
                self.link_event_to_device(ev.ephem_id, device_id_b, score)
                dev_id_a_final = device_id_a
                dev_id_b_final = device_id_b

            # link device->user
            prof_a = new_event.partial_keys.get("profileID", "")
            prof_b = ev.partial_keys.get("profileID", "")
            hashedEmailA = new_event.partial_keys.get("hashedEmail", "").lower()
            hashedEmailB = ev.partial_keys.get("hashedEmail", "").lower()

            if hashedEmailA and hashedEmailA == hashedEmailB:
                user_merged = hash_sha256(hashedEmailA + "sameUser")
                self.link_device_to_user(dev_id_a_final, user_merged, score)
                self.link_device_to_user(dev_id_b_final, user_merged, score)
                user_a = user_merged
                user_b = user_merged
            elif prof_a and prof_b and prof_a.lower() == prof_b.lower():
                user_merged = hash_sha256(prof_a + "sameUser")
                self.link_device_to_user(dev_id_a_final, user_merged, score)
                self.link_device_to_user(dev_id_b_final, user_merged, score)
                user_a = user_merged
                user_b = user_merged
            else:
                user_a = hash_sha256(dev_id_a_final + prof_a + hashedEmailA + "user")
                user_b = hash_sha256(dev_id_b_final + prof_b + hashedEmailB + "user")
                self.link_device_to_user(dev_id_a_final, user_a, score)
                self.link_device_to_user(dev_id_b_final, user_b, score)

            # link user->household
            if wifi_a and wifi_b and wifi_a.lower() == wifi_b.lower():
                hh_merged = hash_sha256(wifi_a + "household")
                self.link_user_to_household(user_a, hh_merged, score)
                self.link_user_to_household(user_b, hh_merged, score)
                aero_cache.set_household_ref(new_event.ephem_id, hh_merged)
                aero_cache.set_household_ref(ev.ephem_id, hh_merged)
                aero_cache.add_ephemeral_to_household(hh_merged, new_event.ephem_id)
                aero_cache.add_ephemeral_to_household(hh_merged, ev.ephem_id)
                aero_cache.add_household_edge(hh_merged, new_event.ephem_id, ev.ephem_id, score)
            else:
                hh_a = hash_sha256(user_a + "soloHouse")
                hh_b = hash_sha256(user_b + "soloHouse")
                self.link_user_to_household(user_a, hh_a, score)
                self.link_user_to_household(user_b, hh_b, score)
                aero_cache.set_household_ref(new_event.ephem_id, hh_a)
                aero_cache.set_household_ref(ev.ephem_id, hh_b)

def unified_neo4j_bridging_advanced(new_event: EphemeralEvent, db: Session, aero_cache):
    """
    Orchestrates advanced bridging for a new event. Called by bridging_logic.
    """
    cutoff_time = new_event.timestamp - timedelta(days=settings.DATA_RETENTION_DAYS)
    recent_events = db.query(EphemeralEvent).filter(EphemeralEvent.timestamp >= cutoff_time).all()

    linker = Neo4jAdvancedLinker()
    try:
        linker.advanced_bridging(new_event, recent_events, aero_cache, db)
    finally:
        linker.close()
    logger.debug(f"Advanced bridging complete for event={new_event.ephem_id}.")
