from services.common_lib.tasks import celery_app, settings
from services.common_lib.tasks_beat import setup_periodic_tasks

# Configure the broker URL
broker_url = f"amqp://aggregator:aggregator@{settings.RABBITMQ_HOST}:5672//"
celery_app.conf.broker_url = broker_url
celery_app.conf.update(timezone="UTC")

# Configure task routes
celery_app.conf.task_routes = {
    'services.common_lib.tasks.batch_fuzzy_bridging': {'queue': 'bridging'},
    'services.common_lib.tasks.bridging_fuzzy': {'queue': 'bridging'},
    'services.common_lib.tasks.queue_fuzzy_bridging': {'queue': 'bridging'},
    'services.common_lib.tasks.short_circuit_deterministic': {'queue': 'bridging'},
    'services.common_lib.tasks_beat.flush_daily_agg_task': {'queue': 'celery'},
    'services.common_lib.tasks_beat.prune_neo4j_task': {'queue': 'celery'},
    'services.common_lib.tasks_beat.retrain_bridging_ml': {'queue': 'celery'}
}

# Set up periodic tasks
setup_periodic_tasks(celery_app)

# Print the beat schedule for debugging
print("Beat schedule:", celery_app.conf.beat_schedule)
