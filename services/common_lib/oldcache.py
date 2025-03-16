import aerospike
from services.common_lib.config import settings
from services.common_lib.logging_config import logger

# These are just constants naming the Aerospike namespace and sets used
AEROSPIKE_NAMESPACE = "test"
BRIDGE_SET = "bridgeSet"
HOUSE_SET = "houseSet"
HOUSE_SCORE_SET = "houseScoreSet"
DAILY_AGG_SET = "dailyAggSet"
EMAIL_INDEX_SET = "emailIndexSet"
FUZZY_QUEUE_SET = "fuzzyQueueSet"

class HouseholdIQCache:
    """
    A production-ready Aerospike-based cache class for bridging references, daily aggregates,
    hashed email indexing, fuzzy bridging queue, etc.
    """

    def __init__(self):
        config = {
            'hosts': [(settings.AEROSPIKE_HOST, settings.AEROSPIKE_PORT)],
            'policies': {
                'timeout': 1000  # 1 second default operation timeout
            }
        }
        try:
            self.client = aerospike.client(config).connect()
            logger.info("Connected to Aerospike (HouseholdIQCache).")
        except aerospike.exception as e:
            logger.exception(f"Failed to connect to Aerospike: {e}")
            raise

        self.ttl_seconds = settings.DATA_RETENTION_DAYS * 24 * 3600

    # --------------------------------------------------
    # 1) Hashed Email Index
    # --------------------------------------------------
    def index_hashed_email(self, hashed_email: str, event_id: int):
        """
        Maintains a list of event IDs for a given hashedEmail, enabling
        deterministic bridging short-circuit logic.
        """
        key = (AEROSPIKE_NAMESPACE, EMAIL_INDEX_SET, hashed_email)
        try:
            self.client.operate(
                key,
                [
                    {
                        "op": aerospike.OPERATOR_APPEND_LIST,
                        "bin": "event_ids",
                        "val": [str(event_id)]
                    }
                ],
                meta={"ttl": self.ttl_seconds}
            )
        except aerospike.exception.RecordNotFound:
            bins = {"event_ids": [str(event_id)]}
            self.client.put(key, bins, meta={"ttl": self.ttl_seconds})
        except Exception as e:
            logger.error(f"Error indexing hashedEmail {hashed_email}: {e}")

    def get_indexed_event_ids_for_email(self, hashed_email: str):
        """
        Retrieve the list of event IDs previously stored for this hashed email.
        """
        key = (AEROSPIKE_NAMESPACE, EMAIL_INDEX_SET, hashed_email)
        try:
            (_, _, record) = self.client.get(key)
            if record:
                return record.get("event_ids", [])
            return []
        except aerospike.exception.RecordNotFound:
            return []
        except Exception as e:
            logger.error(f"Error reading email index for {hashed_email}: {e}")
            return []

    # --------------------------------------------------
    # 2) Fuzzy Bridging Queue
    # --------------------------------------------------
    def enqueue_fuzzy_event(self, event_id: int):
        """
        Enqueues an event ID into a fuzzy bridging queue for batch processing.
        """
        key = (AEROSPIKE_NAMESPACE, FUZZY_QUEUE_SET, "batchQueue")
        try:
            self.client.operate(
                key,
                [
                    {
                        "op": aerospike.OPERATOR_APPEND_LIST,
                        "bin": "batchEvents",
                        "val": [str(event_id)]
                    }
                ],
                meta={"ttl": 120}  # store for 2 minutes
            )
        except aerospike.exception.RecordNotFound:
            bins = {"batchEvents": [str(event_id)]}
            self.client.put(key, bins, meta={"ttl": 120})
        except Exception as e:
            logger.error(f"Error enqueueing fuzzy event {event_id}: {e}")

    def pop_fuzzy_batch(self):
        """
        Pops all event IDs from the fuzzy bridging queue and removes it.
        """
        key = (AEROSPIKE_NAMESPACE, FUZZY_QUEUE_SET, "batchQueue")
        try:
            (k, meta, record) = self.client.get(key)
            if not record:
                return []
            batch = record.get("batchEvents", [])
            self.client.remove(key)  # clear queue after read
            return batch
        except aerospike.exception.RecordNotFound:
            return []
        except Exception as e:
            logger.error(f"Error popping fuzzy batch: {e}")
            return []

    # --------------------------------------------------
    # 3) Household Reference
    # --------------------------------------------------
    def set_household_ref(self, ephemeral_id: str, household_id: str):
        """
        ephemeral_id -> household_id link for bridging references.
        """
        key = (AEROSPIKE_NAMESPACE, BRIDGE_SET, ephemeral_id)
        bins = {"household_id": household_id}
        self.client.put(key, bins, meta={"ttl": self.ttl_seconds})

    def get_household_ref(self, ephemeral_id: str):
        key = (AEROSPIKE_NAMESPACE, BRIDGE_SET, ephemeral_id)
        try:
            (_, _, record) = self.client.get(key)
            if record:
                return record.get("household_id")
            return None
        except aerospike.exception.RecordNotFound:
            return None
        except Exception as e:
            logger.error(f"Aerospike get error for ephemeral_id={ephemeral_id}: {e}")
            return None

    def add_ephemeral_to_household(self, household_id: str, ephemeral_id: str):
        """
        In Aerospike, maintain ephemeral IDs for each household for reference.
        """
        key = (AEROSPIKE_NAMESPACE, HOUSE_SET, household_id)
        try:
            self.client.operate(
                key,
                [
                    {
                        "op": aerospike.OPERATOR_APPEND_LIST,
                        "bin": "ephem_ids",
                        "val": [ephemeral_id]
                    }
                ],
                meta={"ttl": self.ttl_seconds}
            )
        except aerospike.exception.RecordNotFound:
            bins = {"ephem_ids": [ephemeral_id]}
            self.client.put(key, bins, meta={"ttl": self.ttl_seconds})
        except Exception as e:
            logger.error(f"Error adding ephemeral {ephemeral_id} to household {household_id}: {e}")

    def get_ephemerals_in_household(self, household_id: str):
        key = (AEROSPIKE_NAMESPACE, HOUSE_SET, household_id)
        try:
            (_, _, record) = self.client.get(key)
            if record:
                return record.get("ephem_ids", [])
            return []
        except aerospike.exception.RecordNotFound:
            return []
        except Exception as e:
            logger.error(f"Error reading ephemerals in household {household_id}: {e}")
            return []

    # --------------------------------------------------
    # 4) Household Edge Confidence
    # --------------------------------------------------
    def add_household_edge(self, household_id: str, ephem_a: str, ephem_b: str, score: float):
        """
        Stores bridging confidence edge between ephemeral A and ephemeral B in the same household.
        Also accumulates sum and count for average household bridging score.
        """
        sorted_key = "|".join(sorted([ephem_a, ephem_b]))
        key = (AEROSPIKE_NAMESPACE, HOUSE_SCORE_SET, household_id)
        try:
            current_data = {}
            _, _, existing = self.client.get(key)
            if existing:
                current_data = existing

            edges = current_data.get("edges", {})
            sum_val = current_data.get("sum_score", 0.0)
            count_val = current_data.get("count_score", 0)

            if sorted_key not in edges:
                edges[sorted_key] = score
                sum_val += score
                count_val += 1

            bins = {
                "edges": edges,
                "sum_score": sum_val,
                "count_score": count_val
            }
            self.client.put(key, bins, meta={"ttl": self.ttl_seconds})
        except aerospike.exception.RecordNotFound:
            new_bins = {
                "edges": {sorted_key: score},
                "sum_score": score,
                "count_score": 1
            }
            self.client.put(key, new_bins, meta={"ttl": self.ttl_seconds})
        except Exception as e:
            logger.error(f"Error adding household edge for {ephem_a} <-> {ephem_b}, household {household_id}: {e}")

    def get_household_average_score(self, household_id: str) -> float:
        key = (AEROSPIKE_NAMESPACE, HOUSE_SCORE_SET, household_id)
        try:
            (_, _, record) = self.client.get(key)
            if not record:
                return 0.0
            sum_val = record.get("sum_score", 0.0)
            count_val = record.get("count_score", 0)
            if count_val == 0:
                return 0.0
            return float(sum_val) / float(count_val)
        except aerospike.exception.RecordNotFound:
            return 0.0
        except Exception as e:
            logger.error(f"Error retrieving average bridging score for household {household_id}: {e}")
            return 0.0

    # --------------------------------------------------
    # 5) Daily Aggregates Buffer
    # --------------------------------------------------
    def buffer_increment_daily_aggregate(self, date_str: str, partner_id: int, device_type: str, event_type: str):
        """
        Increments the aggregator buffer for (partner_id, device_type, event_type) under the given date_str.
        """
        key = (AEROSPIKE_NAMESPACE, DAILY_AGG_SET, date_str)
        field_key = f"{partner_id}|{device_type}|{event_type}"
        try:
            _, _, rec = self.client.get(key)
            if not rec:
                rec = {"counts": {}}

            counts_map = rec.get("counts", {})
            counts_map[field_key] = counts_map.get(field_key, 0) + 1
            rec["counts"] = counts_map

            self.client.put(key, rec, meta={"ttl": self.ttl_seconds})
        except Exception as e:
            logger.error(f"Error buffering daily aggregates for {date_str}, {field_key}: {e}")

    def flush_daily_aggregate(self, flush_fn):
        """
        Scans the dailyAggSet, calling flush_fn(date_str, counts_map) for each record, 
        then removes it from Aerospike. Used by Celery tasks to commit daily aggregates to Postgres.
        """
        scan = self.client.scan(AEROSPIKE_NAMESPACE, DAILY_AGG_SET)
        records = []

        def handler(item):
            # item is a tuple of (key, metadata, record)
            key, metadata, rec = item
            records.append((key, rec))

        scan.foreach(handler)

        for (k, rec) in records:
            date_str = k[2]  # user_key
            counts_map = rec.get("counts", {})
            flush_fn(date_str, counts_map)
            self.client.remove(k)
