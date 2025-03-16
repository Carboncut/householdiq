import pytest
from services.common_lib.database import SessionLocal
from services.common_lib.models import DataSharingAgreement, Partner, AnonymizedEvent, DailyAggregate
from services.common_lib.data_sharing import store_data_sharing

@pytest.fixture
def db_session():
    db = SessionLocal()
    yield db
    db.close()

def test_data_sharing_no_agreement(db_session):
    """
    If no data sharing agreement, nothing happens.
    """
    partner1 = Partner(name="PartnerA", salt="saltA")
    db_session.add(partner1)
    db_session.commit()
    db_session.refresh(partner1)

    class DummyEvent:
        partner_id = partner1.id

    store_data_sharing(db_session, DummyEvent(), partner1.id, 999999)  # no agreement => skip
    # no crash => test pass

def test_data_sharing_aggregated(db_session):
    """
    If agreement allow_aggregated_data_sharing=True, we share daily aggregates flow.
    """
    pA = Partner(name="PartnerAggA", salt="saltX")
    pB = Partner(name="PartnerAggB", salt="saltY")
    db_session.add(pA)
    db_session.add(pB)
    db_session.commit()

    # create a data sharing agreement
    dsa = DataSharingAgreement(
        partner_id_initiator=pA.id,
        partner_id_recipient=pB.id,
        allow_aggregated_data_sharing=True,
        min_k_anonymity=10
    )
    db_session.add(dsa)
    db_session.commit()

    # create some daily aggregates
    daily = DailyAggregate(
        date_str="2025-05-01",
        partner_id=pA.id,
        device_type="CTV",
        event_type="impression",
        count=42
    )
    db_session.add(daily)
    db_session.commit()

    class DummyEvent2:
        partner_id = pA.id

    store_data_sharing(db_session, DummyEvent2(), pA.id, pB.id)
    # We expect no crash, logs show "Sharing aggregated data..."
    # Optionally, we could check logs or advanced hooking.

def test_data_sharing_anonymized(db_session):
    """
    If agreement does not allow aggregated, but min_k_anonymity is met, we share anonymized data.
    """
    pA = Partner(name="PartnerAnonA", salt="salt1")
    pB = Partner(name="PartnerAnonB", salt="salt2")
    db_session.add(pA)
    db_session.add(pB)
    db_session.commit()

    # create data sharing agreement
    dsa = DataSharingAgreement(
        partner_id_initiator=pA.id,
        partner_id_recipient=pB.id,
        allow_aggregated_data_sharing=False,
        min_k_anonymity=2
    )
    db_session.add(dsa)
    db_session.commit()

    # add anonymized events
    ae1 = AnonymizedEvent(event_id=111, hashed_device_sig="xyz", hashed_user_sig="abc", partner_id=pA.id)
    ae2 = AnonymizedEvent(event_id=222, hashed_device_sig="xxx", hashed_user_sig="bbb", partner_id=pA.id)
    db_session.add(ae1)
    db_session.add(ae2)
    db_session.commit()

    class DummyEvent3:
        partner_id = pA.id

    store_data_sharing(db_session, DummyEvent3(), pA.id, pB.id)
    # No error => sharing anonymized data log. 
