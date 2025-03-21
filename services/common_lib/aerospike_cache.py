import aerospike
from aerospike import POLICY_EXISTS_CREATE_OR_REPLACE
from aerospike_helpers.operations import list_operations as lops
from services.common_lib.config import settings
from services.common_lib.logging_config import logger
from typing import List, Dict, Optional, Any
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from datetime import datetime
import time

AEROSPIKE_NAMESPACE = "test"  # <--- Make sure this matches your real namespace

BRIDGE_SET = "bridgeSet"
HOUSE_SET = "houseSet"
HOUSE_SCORE_SET = "houseScoreSet"
DAILY_AGG_SET = "dailyAggSet"
EMAIL_INDEX_SET = "emailIndexSet"
FUZZY_QUEUE_SET = "fuzzyQueueSet"

# Increase TTL for fuzzy queue to 1 hour
FUZZY_QUEUE_TTL = 3600
BATCH_SIZE = 100

class AerospikeCache:
    """Singleton class for managing Aerospike cache operations."""
    _instance = None
    _lock = asyncio.Lock()
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """
        Initialize basic attributes but defer full initialization to _initialize().
        Only runs once due to the _initialized guard.
        """
        if not self._initialized:
            # Set ttl_seconds
            try:
                self.ttl_seconds = settings.DATA_RETENTION_DAYS * 24 * 3600
                logger.info(f"Setting TTL to {self.ttl_seconds} seconds")
            except Exception as e:
                logger.error(f"Failed to set TTL: {e}")
                self.ttl_seconds = 30 * 24 * 3600
                logger.info(f"Using default TTL of {self.ttl_seconds} seconds")

            self.client = None
            self.operation_buffer = []
            self.buffer_size = 100
            self.flush_interval = 5
            self.last_flush = time.time()
            self._flush_task = None
            self.executor = ThreadPoolExecutor(max_workers=10)
            self._initialize()

    def _initialize(self):
        """Perform synchronous initialization."""
        if self._initialized:
            return
        try:
            # Connect to Aerospike
            self.client = aerospike.client({
                'hosts': [(settings.AEROSPIKE_HOST, settings.AEROSPIKE_PORT)]
            }).connect()
            logger.info("Connected to Aerospike")

            # Initialize sets after connection
            self._initialize_sets()

            self._initialized = True
        except Exception as e:
            logger.error(f"Error in initialization: {e}")
            raise

    def _initialize_sets(self):
        """Initialize required Aerospike sets with config records."""
        if not self.ttl_seconds:
            raise RuntimeError("TTL not set before initializing sets")

        try:
            for set_name in [
                BRIDGE_SET, HOUSE_SET, HOUSE_SCORE_SET,
                DAILY_AGG_SET, EMAIL_INDEX_SET, FUZZY_QUEUE_SET
            ]:
                key = (AEROSPIKE_NAMESPACE, set_name, "config")
                bins = {
                    "initialized": True,
                    "created_at": str(datetime.now()),
                    "ttl": self.ttl_seconds
                }
                self.client.put(key, bins, meta={"ttl": self.ttl_seconds})
            logger.info("Successfully initialized all required Aerospike sets.")
        except Exception as e:
            logger.error(f"Error initializing Aerospike sets: {e}")
            raise

    async def close(self):
        """Close the Aerospike connection and thread pool."""
        if hasattr(self, 'client') and self.client is not None:
            self.client.close()
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)

    async def _execute_aerospike_operation(self, operation_fn, *args, **kwargs):
        """Run the given operation_fn in a thread pool, handle errors."""
        try:
            return await asyncio.get_event_loop().run_in_executor(
                self.executor,
                lambda: operation_fn(*args, **kwargs)
            )
        except Exception as e:
            logger.error(f"Aerospike operation error: {e}")
            raise

    @lru_cache(maxsize=1000)
    async def get_household_ref(self, ephemeral_id: str) -> Optional[str]:
        """Cached household reference lookup by ephemeral_id."""
        key = (AEROSPIKE_NAMESPACE, BRIDGE_SET, ephemeral_id)
        try:
            (_, _, record) = await self._execute_aerospike_operation(self.client.get, key)
            return record.get("household_id") if record else None
        except aerospike.exception.RecordNotFound:
            return None
        except Exception as e:
            logger.error(f"Aerospike get error for ephemeral_id={ephemeral_id}: {e}")
            return None

    async def buffer_increment_daily_aggregate(
        self,
        date_str: str,
        partner_id: int,
        device_type: str,
        event_type: str
    ):
        """
        Increment the daily aggregate in Aerospike for the given date, partner, device, and event.
        Raises an exception if the Aerospike operation fails.
        """
        key = (AEROSPIKE_NAMESPACE, DAILY_AGG_SET, date_str)
        field_key = f"{partner_id}|{device_type}|{event_type}"

        logger.debug(
            f"buffer_increment_daily_aggregate: key={key}, field_key={field_key}, "
            f"ttl={self.ttl_seconds}"
        )

        async with self._lock:
            try:
                # -- This is the KEY FIX: Catch RecordNotFound here --
                try:
                    (_, _, record) = await self._execute_aerospike_operation(
                        self.client.get,
                        key
                    )
                except aerospike.exception.RecordNotFound:
                    record = None

                if not record:
                    # Create new record if it doesn't exist
                    bins = {"counts": {field_key: 1}}
                    await self._execute_aerospike_operation(
                        self.client.put,
                        key,
                        bins,
                        {"ttl": self.ttl_seconds}
                    )
                    logger.debug(f"Created new daily aggregate record for {field_key}")
                else:
                    # Update existing record
                    counts_map = record.get("counts", {})
                    current_count = counts_map.get(field_key, 0)
                    counts_map[field_key] = current_count + 1

                    bins = {"counts": counts_map}
                    await self._execute_aerospike_operation(
                        self.client.put,
                        key,
                        bins,
                        {"ttl": self.ttl_seconds}
                    )
                    logger.debug(
                        f"Incremented daily aggregate for {field_key}, "
                        f"new count={counts_map[field_key]}"
                    )

            except Exception as e:
                logger.error(f"Error buffering daily aggregate: {e}")
                raise

    async def _flush_operation_buffer(self):
        """Flush buffered operations to Aerospike (unused example)."""
        if not self.operation_buffer:
            return

        async with self._lock:
            operations = self.operation_buffer.copy()
            self.operation_buffer.clear()

            key_groups = {}
            for op in operations:
                if op['type'] == 'increment':
                    key = op['key']
                    if key not in key_groups:
                        key_groups[key] = []
                    key_groups[key].append(op['field_key'])

            for key, field_keys in key_groups.items():
                try:
                    try:
                        (_, _, rec) = await self._execute_aerospike_operation(
                            self.client.get,
                            key
                        )
                    except aerospike.exception.RecordNotFound:
                        rec = {}

                    if not rec:
                        rec = {"counts": {}}

                    counts_map = rec.setdefault("counts", {})
                    for field_key in field_keys:
                        old_val = counts_map.get(field_key, 0)
                        counts_map[field_key] = old_val + 1

                    await self._execute_aerospike_operation(
                        self.client.put,
                        key,
                        rec,
                        {"ttl": self.ttl_seconds},
                        {"exists": POLICY_EXISTS_CREATE_OR_REPLACE}
                    )
                except Exception as e:
                    logger.error(f"Error processing batch for key {key}: {e}")

    async def enqueue_fuzzy_event(self, event_id: int):
        """Enqueue an event ID for fuzzy bridging."""
        key = (AEROSPIKE_NAMESPACE, FUZZY_QUEUE_SET, "batchQueue")
        try:
            ops = [lops.list_append("batchEvents", str(event_id))]
            await self._execute_aerospike_operation(
                self.client.operate, key, ops, {"ttl": FUZZY_QUEUE_TTL}
            )
        except aerospike.exception.RecordNotFound:
            bins = {"batchEvents": [str(event_id)]}
            await self._execute_aerospike_operation(
                self.client.put,
                key,
                bins,
                {"ttl": FUZZY_QUEUE_TTL}
            )
        except Exception as e:
            logger.error(f"Error enqueueing fuzzy event {event_id}: {e}")

    async def pop_fuzzy_batch(self) -> List[str]:
        """Pop the entire fuzzy bridging batch and remove it from Aerospike."""
        key = (AEROSPIKE_NAMESPACE, FUZZY_QUEUE_SET, "batchQueue")
        try:
            (_, _, record) = await self._execute_aerospike_operation(self.client.get, key)
            if not record:
                return []

            batch = record.get("batchEvents", [])
            await self._execute_aerospike_operation(self.client.remove, key)
            return batch
        except Exception as e:
            logger.error(f"Error popping fuzzy batch: {e}")
            return []

    async def flush_daily_aggregate(self, flush_fn):
        """
        Example method for scanning the dailyAggSet and calling flush_fn on each record.
        If your design doesn't need this, you can remove it.
        """
        logger.debug(f"flush_daily_aggregate: scanning set={DAILY_AGG_SET}")

        try:
            records = []

            def scan_handler(key, meta, record):
                if record:
                    records.append((key, record))

            await self._execute_aerospike_operation(
                lambda: self.client.scan(AEROSPIKE_NAMESPACE, DAILY_AGG_SET).foreach(
                    scan_handler, {'fail_on_cluster_change': False}
                )
            )

            logger.debug(f"flush_daily_aggregate: found {len(records)} records in dailyAggSet")

            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i : i + BATCH_SIZE]
                for (key, rec) in batch:
                    date_str = key[2] if len(key) > 2 else "unknown_date"
                    counts_map = rec.get("counts", {})
                    try:
                        await flush_fn(date_str, counts_map)
                        await self._execute_aerospike_operation(self.client.remove, key)
                    except Exception as flush_err:
                        logger.exception(f"Error in flush_fn for date_str={date_str}: {flush_err}")

        except Exception as e:
            logger.error(f"Error scanning dailyAggSet: {e}")

    # ------------------------------------------------------------
    #   Hashed Email Index
    # ------------------------------------------------------------
    def index_hashed_email(self, hashed_email: str, event_id: int):
        """
        Example method to append an event_id to a list stored under hashed_email.
        """
        key = (AEROSPIKE_NAMESPACE, EMAIL_INDEX_SET, hashed_email)
        logger.debug(f"index_hashed_email: key={key}, event_id={event_id}")
        try:
            ops = [lops.list_append("event_ids", str(event_id))]
            self.client.operate(key, ops, meta={"ttl": self.ttl_seconds})
        except aerospike.exception.RecordNotFound:
            logger.debug("index_hashed_email: creating new record.")
            bins = {"event_ids": [str(event_id)]}
            self.client.put(key, bins, meta={"ttl": self.ttl_seconds})
        except Exception as e:
            logger.error(f"Error indexing hashedEmail {hashed_email} => {e}")

    def get_indexed_event_ids_for_email(self, hashed_email: str) -> List[str]:
        """
        Retrieve list of event_ids associated with the given hashed_email.
        """
        key = (AEROSPIKE_NAMESPACE, EMAIL_INDEX_SET, hashed_email)
        logger.debug(f"get_indexed_event_ids_for_email: key={key}")
        try:
            (k, m, record) = self.client.get(key)
            logger.debug(f"get_indexed_event_ids_for_email: record={record}")
            return record.get("event_ids", []) if record else []
        except aerospike.exception.RecordNotFound:
            logger.debug("get_indexed_event_ids_for_email: no record => []")
            return []
        except Exception as e:
            logger.error(f"Error reading email index for {hashed_email}: {e}")
            return []

    # ------------------------------------------------------------
    #   Household bridging references
    # ------------------------------------------------------------
    def set_household_ref(self, ephemeral_id: str, household_id: str):
        """Store a direct ephemeral->household mapping."""
        key = (AEROSPIKE_NAMESPACE, BRIDGE_SET, ephemeral_id)
        bins = {"household_id": household_id}
        logger.debug(f"set_household_ref: key={key}, bins={bins}, ttl={self.ttl_seconds}")
        self.client.put(key, bins, meta={"ttl": self.ttl_seconds})

    def add_ephemeral_to_household(self, household_id: str, ephemeral_id: str):
        """Append ephemeral_id to the list of IDs associated with this household."""
        key = (AEROSPIKE_NAMESPACE, HOUSE_SET, household_id)
        logger.debug(f"add_ephemeral_to_household: key={key}, ephemeral_id={ephemeral_id}")
        try:
            ops = [lops.list_append("ephem_ids", ephemeral_id)]
            self.client.operate(key, ops, meta={"ttl": self.ttl_seconds})
        except aerospike.exception.RecordNotFound:
            logger.debug("add_ephemeral_to_household: creating new record.")
            bins = {"ephem_ids": [ephemeral_id]}
            self.client.put(key, bins, meta={"ttl": self.ttl_seconds})
        except Exception as e:
            logger.error(f"Error adding ephemeral to household: {e}")

    def get_ephemerals_in_household(self, household_id: str) -> List[str]:
        """Retrieve the ephemeral IDs from the given household record."""
        key = (AEROSPIKE_NAMESPACE, HOUSE_SET, household_id)
        logger.debug(f"get_ephemerals_in_household: key={key}")
        try:
            (k, m, record) = self.client.get(key)
            logger.debug(f"get_ephemerals_in_household: record={record}")
            return record.get("ephem_ids", []) if record else []
        except aerospike.exception.RecordNotFound:
            logger.debug("get_ephemerals_in_household: no record => []")
            return []
        except Exception as e:
            logger.error(f"Error reading household membership: {e}")
            return []

    def add_household_edge(self, household_id: str, ephem_a: str, ephem_b: str, score: float):
        """
        Example method to store some 'edge' between ephemeral A and ephemeral B in a household-based scoring record.
        """
        sorted_key = "|".join(sorted([ephem_a, ephem_b]))
        key = (AEROSPIKE_NAMESPACE, HOUSE_SCORE_SET, household_id)
        logger.debug(f"add_household_edge: key={key}, sorted_key={sorted_key}, score={score}")
        try:
            (_, _, existing) = self.client.get(key)
            logger.debug(f"add_household_edge: existing record={existing}")

            record = existing if existing else {}
            edges = record.get("edges", {})
            sum_val = record.get("sum_score", 0.0)
            count_val = record.get("count_score", 0)

            if sorted_key not in edges:
                edges[sorted_key] = score
                sum_val += score
                count_val += 1

            bins = {
                "edges": edges,
                "sum_score": sum_val,
                "count_score": count_val
            }
            logger.debug(f"add_household_edge: bins => {bins}, ttl={self.ttl_seconds}")
            self.client.put(key, bins, meta={"ttl": self.ttl_seconds})
        except aerospike.exception.RecordNotFound:
            logger.debug("add_household_edge: no record found => create new.")
            edges = {sorted_key: score}
            bins = {
                "edges": edges,
                "sum_score": score,
                "count_score": 1
            }
            self.client.put(key, bins, meta={"ttl": self.ttl_seconds})
        except Exception as e:
            logger.error(f"Error adding household edge: {e}")

    def get_household_average_score(self, household_id: str) -> float:
        """Compute and return the average bridging score for a household."""
        key = (AEROSPIKE_NAMESPACE, HOUSE_SCORE_SET, household_id)
        logger.debug(f"get_household_average_score: key={key}")
        try:
            (_, _, record) = self.client.get(key)
            logger.debug(f"get_household_average_score: record={record}")
            if not record:
                return 0.0
            sum_val = record.get("sum_score", 0.0)
            count_val = record.get("count_score", 0)
            if count_val == 0:
                return 0.0
            return float(sum_val) / float(count_val)
        except aerospike.exception.RecordNotFound:
            logger.debug("get_household_average_score: no record => 0.0")
            return 0.0
        except Exception as e:
            logger.error(f"Error retrieving household average score: {e}")
            return 0.0
