import pytest
from fastapi.testclient import TestClient
from services.ingestion.app_ingestion import app
from services.common_lib.database import SessionLocal
from services.common_lib.models import FrequencyCapping
from services.common_lib.real_time_capping import RealTimeCappingManager

client = TestClient(app)

@pytest.fixture
def db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def test_check_frequency_cap(db_session):
    capping_manager = RealTimeCappingManager()
    hh_id = "testHousehold123"

    # Initially not in DB => new row with default daily_impressions=0, cap_limit=5
    result = capping_manager.check_cap(db_session, hh_id)
    assert result["can_serve"] is True
    assert result["daily_impressions"] == 0
    assert result["cap_limit"] == 5

def test_increment_frequency_cap(db_session):
    capping_manager = RealTimeCappingManager()
    hh_id = "testHouseholdXYZ"

    for i in range(6):
        res = capping_manager.increment_impression(db_session, hh_id)

    # After 6 increments, daily_impressions=6, cap_limit=5 => can_serve => False
    assert res["daily_impressions"] == 6
    assert res["cap_limit"] == 5
    assert res["can_serve"] is False

def test_consent_revocation(db_session):
    capping_manager = RealTimeCappingManager()
    ephemeral_id = "ephem999"
    # Just test that it doesn't raise an error
    capping_manager.revoke_consent(db_session, ephemeral_id)

    # Optionally verify it in DB if we want
    from services.common_lib.models import ConsentRevocation
    rev = db_session.query(ConsentRevocation).filter_by(ephem_id=ephemeral_id).first()
    assert rev is not None
