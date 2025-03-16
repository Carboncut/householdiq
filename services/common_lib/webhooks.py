import requests
from services.common_lib.logging_config import logger
from services.common_lib.database import SessionLocal
from services.common_lib.models import WebhookSubscription

class WebhookManager:
    def __init__(self):
        pass

    def trigger_webhook(self, event_type: str, data: dict):
        db = SessionLocal()
        try:
            subs = db.query(WebhookSubscription).filter_by(event_type=event_type, active=True).all()
            for s in subs:
                try:
                    resp = requests.post(s.callback_url, json=data, timeout=3)
                    logger.info(f"Webhook to {s.callback_url} returned {resp.status_code}")
                except Exception as e:
                    logger.error(f"Error calling webhook {s.callback_url}: {e}")
        finally:
            db.close()
