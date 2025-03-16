from sqlalchemy.orm import Session
from services.common_lib.logging_config import logger
from services.common_lib.models import DataSharingAgreement, AnonymizedEvent, DailyAggregate

def get_data_sharing_agreement(db: Session, initiator_id: int, recipient_id: int):
    return (db.query(DataSharingAgreement)
             .filter(DataSharingAgreement.partner_id_initiator == initiator_id,
                     DataSharingAgreement.partner_id_recipient == recipient_id)
             .first())

def store_data_sharing(db: Session, event, partner_id_initiator: int, recipient_id: int):
    """
    If there's a data-sharing agreement, replicate data or share aggregates.
    """
    agreement = get_data_sharing_agreement(db, partner_id_initiator, recipient_id)
    if not agreement:
        logger.debug(f"No data sharing agreement between {partner_id_initiator} and {recipient_id}.")
        return
    if agreement.allow_aggregated_data_sharing:
        share_aggregated_data(db, partner_id_initiator, recipient_id)
    else:
        share_anonymized_data_if_k_met(db, partner_id_initiator, recipient_id, agreement.min_k_anonymity)

def share_aggregated_data(db: Session, partner_id_initiator: int, recipient_id: int):
    rows = db.query(DailyAggregate).filter(DailyAggregate.partner_id == partner_id_initiator).all()
    logger.info(f"Sharing aggregated data from partner={partner_id_initiator} to {recipient_id}: {len(rows)} rows")

def share_anonymized_data_if_k_met(db: Session, partner_id_initiator: int, recipient_id: int, min_k_anonymity: int):
    count_anon = db.query(AnonymizedEvent).filter(AnonymizedEvent.partner_id == partner_id_initiator).count()
    if count_anon >= min_k_anonymity:
        logger.info(f"Sharing anonymized data from partner={partner_id_initiator} to {recipient_id}, total={count_anon}")
    else:
        logger.info(f"Insufficient anonymized data (k={min_k_anonymity}), have={count_anon}. Skipping.")

def handle_data_sharing(db: Session, event, partner_id_recipient: int):
    store_data_sharing(db, event, event.partner_id, partner_id_recipient)
