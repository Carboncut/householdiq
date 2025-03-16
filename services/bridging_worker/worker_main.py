from celery import Celery
from services.common_lib.config import settings
from services.common_lib.tasks_beat import setup_periodic_tasks
from services.common_lib import tasks

celery_app = Celery(
    "worker_main",
    broker=f"amqp://aggregator:aggregator@{settings.RABBITMQ_HOST}:5672//",
    backend=None
)
celery_app.conf.update(timezone="UTC")

setup_periodic_tasks(celery_app)
