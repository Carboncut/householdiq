import aerospike
from aerospike import POLICY_EXISTS_CREATE_OR_REPLACE
from aerospike_helpers.operations import list_operations as lops
from services.common_lib.config import settings
from services.common_lib.logging_config import logger

AEROSPIKE_NAMESPACE = "test"  # <--- Use this consistently

BRIDGE_SET = "bridgeSet"
HOUSE_SET = "houseSet"
HOUSE_SCORE_SET = "houseScoreSet"
DAILY_AGG_SET = "dailyAggSet"
EMAIL_INDEX_SET = "emailIndexSet"
FUZZY_QUEUE_SET = "fuzzyQueueSet"


class AerospikeCache:
    def __init__(self):
        config = {
            'hosts': [(settings.AEROSPIKE_HOST, settings.AEROSPIKE_PORT)],
            'policies': {'timeout': 1000}
        }
        try:
            self.client = aerospike.client(config).connect()
            logger.info("Connected to Aerospike.")
        except aerospike.exception.AerospikeError as e:
            logger.exception(f"Failed to connect to Aerospike: {e}")
            raise

        self.ttl_seconds = settings.DATA_RETENTION_DAYS * 24 * 3600
        logger.debug(f"Aerospike TTL set to {self.ttl_seconds} seconds.")

    # ------------------------------------------------------------
    #   Hashed Email Index
    # ------------------------------------------------------------
    def index_hashed_email(self, hashed_email: str, event_id: int):
        key = (AEROSPIKE_NAMESPACE, EMAIL_INDEX_SET, hashed_email)
        logger.debug(f"index_hashed_email: key={key}, event_id={event_id}")
        try:
            ops = [lops.list_append("event_ids", str(event_id))]
            logger.debug(f"index_hashed_email: list_append(event_ids, {event_id}), ttl={self.ttl_seconds}")
            self.client.operate(key, ops, meta={"ttl": self.ttl_seconds})
        except aerospike.exception.RecordNotFound:
            logger.debug("index_hashed_email: RecordNotFound => create new record.")
            bins = {"event_ids": [str(event_id)]}
            self.client.put(key, bins, meta={"ttl": self.ttl_seconds})
        except Exception as e:
            logger.error(f"Error indexing hashedEmail {hashed_email} => {e}")

    def get_indexed_event_ids_for_email(self, hashed_email: str):
        key = (AEROSPIKE_NAMESPACE, EMAIL_INDEX_SET, hashed_email)
        logger.debug(f"get_indexed_event_ids_for_email: key={key}")
        try:
            (k, m, record) = self.client.get(key)
            logger.debug(f"get_indexed_event_ids_for_email: record={record}")
            return record.get("event_ids", []) if record else []
        except aerospike.exception.RecordNotFound:
            logger.debug("get_indexed_event_ids_for_email: RecordNotFound => returning []")
            return []
        except Exception as e:
            logger.error(f"Error reading email index for {hashed_email}: {e}")
            return []

    # ------------------------------------------------------------
    #   Fuzzy Bridging Queue
    # ------------------------------------------------------------
    def enqueue_fuzzy_event(self, event_id: int):
        key = (AEROSPIKE_NAMESPACE, FUZZY_QUEUE_SET, "batchQueue")
        logger.debug(f"enqueue_fuzzy_event: key={key}, event_id={event_id}")
        try:
            ops = [lops.list_append("batchEvents", str(event_id))]
            logger.debug(f"enqueue_fuzzy_event: list_append(batchEvents, {event_id}), ttl=120")
            self.client.operate(key, ops, meta={"ttl": 120})
        except aerospike.exception.RecordNotFound:
            logger.debug("enqueue_fuzzy_event: RecordNotFound => create new record.")
            bins = {"batchEvents": [str(event_id)]}
            self.client.put(key, bins, meta={"ttl": 120})
        except Exception as e:
            logger.error(f"Error enqueueing fuzzy event {event_id}: {e}")

    def pop_fuzzy_batch(self):
        key = (AEROSPIKE_NAMESPACE, FUZZY_QUEUE_SET, "batchQueue")
        logger.debug(f"pop_fuzzy_batch: key={key}")
        try:
            (k, m, record) = self.client.get(key)
            logger.debug(f"pop_fuzzy_batch: record={record}")
            if not record:
                return []
            batch = record.get("batchEvents", [])
            logger.debug(f"pop_fuzzy_batch: removing key={key}")
            self.client.remove(key)
            return batch
        except aerospike.exception.RecordNotFound:
            logger.debug("pop_fuzzy_batch: RecordNotFound => returning []")
            return []
        except Exception as e:
            logger.error(f"Error popping fuzzy batch: {e}")
            return []

    # ------------------------------------------------------------
    #   Household bridging references
    # ------------------------------------------------------------
    def set_household_ref(self, ephemeral_id: str, household_id: str):
        key = (AEROSPIKE_NAMESPACE, BRIDGE_SET, ephemeral_id)
        bins = {"household_id": household_id}
        logger.debug(f"set_household_ref: key={key}, bins={bins}, ttl={self.ttl_seconds}")
        self.client.put(key, bins, meta={"ttl": self.ttl_seconds})

    def get_household_ref(self, ephemeral_id: str):
        key = (AEROSPIKE_NAMESPACE, BRIDGE_SET, ephemeral_id)
        logger.debug(f"get_household_ref: key={key}")
        try:
            (k, m, record) = self.client.get(key)
            logger.debug(f"get_household_ref: record={record}")
            return record.get("household_id") if record else None
        except aerospike.exception.RecordNotFound:
            logger.debug("get_household_ref: RecordNotFound => None")
            return None
        except Exception as e:
            logger.error(f"Aerospike get error: {e}")
            return None

    def add_ephemeral_to_household(self, household_id: str, ephemeral_id: str):
        key = (AEROSPIKE_NAMESPACE, HOUSE_SET, household_id)
        logger.debug(f"add_ephemeral_to_household: key={key}, ephemeral_id={ephemeral_id}")
        try:
            ops = [lops.list_append("ephem_ids", ephemeral_id)]
            logger.debug(f"add_ephemeral_to_household: list_append(ephem_ids, {ephemeral_id}), ttl={self.ttl_seconds}")
            self.client.operate(key, ops, meta={"ttl": self.ttl_seconds})
        except aerospike.exception.RecordNotFound:
            logger.debug("add_ephemeral_to_household: RecordNotFound => create new record.")
            bins = {"ephem_ids": [ephemeral_id]}
            self.client.put(key, bins, meta={"ttl": self.ttl_seconds})
        except Exception as e:
            logger.error(f"Error adding ephemeral to household: {e}")

    def get_ephemerals_in_household(self, household_id: str):
        key = (AEROSPIKE_NAMESPACE, HOUSE_SET, household_id)
        logger.debug(f"get_ephemerals_in_household: key={key}")
        try:
            (k, m, record) = self.client.get(key)
            logger.debug(f"get_ephemerals_in_household: record={record}")
            return record.get("ephem_ids", []) if record else []
        except aerospike.exception.RecordNotFound:
            logger.debug("get_ephemerals_in_household: RecordNotFound => []")
            return []
        except Exception as e:
            logger.error(f"Error reading household membership: {e}")
            return []

    def add_household_edge(self, household_id: str, ephem_a: str, ephem_b: str, score: float):
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
            logger.debug("get_household_average_score: RecordNotFound => 0.0")
            return 0.0
        except Exception as e:
            logger.error(f"Error retrieving household average score: {e}")
            return 0.0

    # ------------------------------------------------------------
    #   Daily Aggregates
    # ------------------------------------------------------------
    def buffer_increment_daily_aggregate(self, date_str: str, partner_id: int, device_type: str, event_type: str):
        key = (AEROSPIKE_NAMESPACE, DAILY_AGG_SET, date_str)
        field_key = f"{partner_id}|{device_type}|{event_type}"

        logger.debug(f"buffer_increment_daily_aggregate: key={key}, field_key={field_key}")
        try:
            # Attempt to get existing record
            _, _, rec = self.client.get(key)
            logger.debug(f"buffer_increment_daily_aggregate: initial record={rec}")

            if not rec:
                rec = {"counts": {}}

            counts_map = rec.setdefault("counts", {})
            old_val = counts_map.get(field_key, 0)
            new_val = old_val + 1
            counts_map[field_key] = new_val

            logger.debug(f"buffer_increment_daily_aggregate: {field_key} old_val={old_val}, new_val={new_val}")

            self.client.put(
                key,
                rec,
                meta={"ttl": self.ttl_seconds},
                policy={"exists": POLICY_EXISTS_CREATE_OR_REPLACE}
            )
            # Debug readback
            (_, _, verify_rec) = self.client.get(key)
            logger.debug(f"buffer_increment_daily_aggregate: after put => verify_rec={verify_rec}")

        except aerospike.exception.RecordNotFound:
            logger.debug(f"buffer_increment_daily_aggregate: record not found for {key}, creating new.")
            new_rec = {"counts": {field_key: 1}}
            self.client.put(
                key,
                new_rec,
                meta={"ttl": self.ttl_seconds},
                policy={"exists": POLICY_EXISTS_CREATE_OR_REPLACE}
            )
        except Exception as e:
            logger.error(f"Error buffering daily aggregates: {e}")

    def flush_daily_aggregate(self, flush_fn):
        logger.debug(f"flush_daily_aggregate: scanning set={DAILY_AGG_SET}")
        scan = self.client.scan(AEROSPIKE_NAMESPACE, DAILY_AGG_SET)

        records = []

        def daily_agg_handler(key, meta, record):
            if record:
                records.append((key, record))

        try:
            # we set fail_on_cluster_change to False to be safer
            scan.foreach(daily_agg_handler, {'fail_on_cluster_change': False})
        except Exception as e:
            logger.error(f"Error scanning dailyAggSet: {e}")
            return

        logger.debug(f"flush_daily_aggregate: found {len(records)} records")

        for (key, rec) in records:
            date_str = key[2] if len(key) > 2 else "unknown_date"
            counts_map = rec.get("counts", {})
            logger.debug(f"flush_daily_aggregate: date_str={date_str}, counts_map={counts_map}")
            try:
                flush_fn(date_str, counts_map)
            except Exception as flush_err:
                logger.exception(f"Error in flush_fn for date_str={date_str}: {flush_err}")

            # remove record to avoid re-processing
            logger.debug(f"flush_daily_aggregate: removing key={key}")
            try:
                self.client.remove(key)
            except Exception as remove_err:
                logger.error(f"Error removing daily agg key={key}: {remove_err}")
