from celery.schedules import crontab
from services.common_lib.tasks import celery_app, batch_fuzzy_bridging
from services.common_lib.config import settings
from services.common_lib.database import SessionLocal
from services.common_lib.logging_config import logger
from services.common_lib.daily_aggregates import flush_daily_aggregate
from services.common_lib.aerospike_cache import AerospikeCache
from neo4j import GraphDatabase
from services.common_lib.ml_bridging_threshold import MLBridgingThresholdManager
import asyncio

def setup_periodic_tasks(app):
    # flush daily aggregates every hour
    if not hasattr(app.conf, 'beat_schedule'):
        app.conf.beat_schedule = {}
    
    app.conf.beat_schedule.update({
        'flush-daily-agg': {
            'task': 'services.common_lib.tasks_beat.flush_daily_agg_task',
            'schedule': 3600.0
        },
        # run fuzzy bridging batch every 10s
        'batch-fuzzy-bridging': {
            'task': 'services.common_lib.tasks.batch_fuzzy_bridging',
            'schedule': 10.0
        }
    })
    
    if settings.PRUNE_NEO4J_ENABLED:
        app.conf.beat_schedule['prune-neo4j-daily'] = {
            'task': 'services.common_lib.tasks_beat.prune_neo4j_task',
            'schedule': crontab(hour=3, minute=0)
        }
    # weekly ML bridging threshold retrain
    app.conf.beat_schedule['retrain-bridging-ml-weekly'] = {
        'task': 'services.common_lib.tasks_beat.retrain_bridging_ml',
        'schedule': crontab(day_of_week='sun', hour=1, minute=0)
    }

@celery_app.task(name="services.common_lib.tasks_beat.flush_daily_agg_task")
def flush_daily_agg_task():
    logger.info("Flushing daily aggregates from Aerospike to Postgres.")
    db = SessionLocal()
    try:
        aero = AerospikeCache()
        asyncio.run(flush_daily_aggregate(aero, db))
    except Exception as e:
        logger.error(f"Error flushing daily aggregates: {e}")
    finally:
        db.close()

@celery_app.task(name="services.common_lib.tasks_beat.prune_neo4j_task")
def prune_neo4j_task():
    logger.info("Pruning old Neo4j event nodes.")
    try:
        driver = GraphDatabase.driver(settings.NEO4J_URI, auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD))
        with driver.session() as session:
            session.run(f"""
                MATCH (e:Event)
                WHERE e.createdAt < (timestamp() - {settings.DATA_RETENTION_DAYS}*86400000)
                DETACH DELETE e
            """)
        driver.close()
    except Exception as e:
        logger.error(f"Error pruning Neo4j: {e}")

@celery_app.task(name="services.common_lib.tasks_beat.retrain_bridging_ml")
def retrain_bridging_ml():
    logger.info("Retraining ML bridging threshold (weekly).")
    db = SessionLocal()
    try:
        ml_mgr = MLBridgingThresholdManager(db)
        ml_mgr.retrain_model()
    except Exception as e:
        logger.error(f"Error in retrain_bridging_ml: {e}")
    finally:
        db.close()
