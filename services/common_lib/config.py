import os
import json

class Settings:
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://householdiq_user:householdiq_pass@localhost:5432/householdiq_db")
    AEROSPIKE_HOST = os.getenv("AEROSPIKE_HOST", "localhost")
    AEROSPIKE_PORT = int(os.getenv("AEROSPIKE_PORT", "3000"))
    RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j_pass")

    DEBUG = (os.getenv("DEBUG", "false").lower() == "true")
    GLOBAL_SALT = os.getenv("GLOBAL_SALT", "SUPER_SECURE_SALT")

    BRIDGING_CONFIDENCE_THRESHOLD = float(os.getenv("BRIDGING_CONFIDENCE_THRESHOLD", "0.7"))
    DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "30"))
    PRIVACY_MIN_THRESHOLD = int(os.getenv("PRIVACY_MIN_THRESHOLD", "10"))
    PRIVACY_NOISE_EPSILON = float(os.getenv("PRIVACY_NOISE_EPSILON", "1.0"))
    USE_NEO4J_BRIDGING = (os.getenv("USE_NEO4J_BRIDGING", "true").lower() == "true")
    PRUNE_NEO4J_ENABLED = (os.getenv("PRUNE_NEO4J_ENABLED", "true").lower() == "true")

    _sr = os.getenv("SAMPLING_RATES", '{"impression":10000,"click":3000,"conversion":500}')
    SAMPLING_RATES = json.loads(_sr)

    DP_MODE_ENABLED = (os.getenv("DP_MODE_ENABLED", "false").lower() == "true")

settings = Settings()
