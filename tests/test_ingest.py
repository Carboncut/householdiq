import uuid
import pytest
from fastapi.testclient import TestClient
from services.ingestion.app_ingestion import app
from services.common_lib.database import SessionLocal
from services.common_lib.models import Partner

client = TestClient(app)

@pytest.fixture
def db_session():
    db = SessionLocal()
    yield db
    db.close()

@pytest.fixture
def partner_fixture(db_session):
    p = Partner(name=f"Partner-{uuid.uuid4()}", salt="test_salt")
    db_session.add(p)
    db_session.commit()
    db_session.refresh(p)
    return p

def test_ingest_deterministic(db_session, partner_fixture):
    payload = {
        "partner_id": partner_fixture.id,
        "device_data": "deviceABC",
        "partial_keys": {
            "hashedEmail": "testhashedemail123",
            "deviceType": "Laptop"
        },
        "event_type": "impression",
        "campaign_id": "detTest",
        "consent_flags": {
            "cross_device_bridging": True,
            "targeting_segments": True
        }
    }
    resp = client.post("/v1/ingest", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["ephem_id"] == "deviceABC"

def test_ingest_child(db_session, partner_fixture):
    payload = {
        "partner_id": partner_fixture.id,
        "device_data": "childDevice111",
        "partial_keys": {
            "isChild": "true"
        },
        "event_type": "impression",
        "campaign_id": "kidAds",
        "consent_flags": {
            "cross_device_bridging": True,
            "targeting_segments": True
        }
    }
    r = client.post("/v1/ingest", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["bridging_skipped_reason"] == "CHILD_FLAG"
