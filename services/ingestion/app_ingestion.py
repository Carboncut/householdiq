import os
import asyncio
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict
from sqlalchemy.orm import Session

from services.common_lib.config import settings
from services.common_lib.session_utils import get_db
from services.common_lib.models import (
    Partner, ConsentFlags, EphemeralEvent, AnonymizedEvent
)
from services.common_lib.aerospike_cache import AerospikeCache
from services.common_lib.logging_config import logger
from services.common_lib.privacy_frameworks import (
    parse_tcf_string, parse_us_privacy_string, user_allows_bridging
)
from services.common_lib.sampling import should_sample_event
from services.common_lib.real_time_capping import RealTimeCappingManager
from services.ingestion.tasks import short_circuit_deterministic, queue_fuzzy_bridging

ASYNC_INGESTION = (os.getenv("ASYNC_INGESTION", "false").lower() == "true")

app = FastAPI(title="HouseholdIQ Ingestion Service", debug=settings.DEBUG)
aero_client = AerospikeCache()
rtc_manager = RealTimeCappingManager()

# ----- Pydantic Schemas -----

class ConsentFlagsRequest(BaseModel):
    cross_device_bridging: bool
    targeting_segments: bool

class PrivacySignals(BaseModel):
    tcf_string: Optional[str] = None
    us_privacy_string: Optional[str] = None
   

class IngestRequest(BaseModel):
    partner_id: int
    device_data: str
    partial_keys: Dict[str, str]
    event_type: str
    campaign_id: Optional[str] = None
    consent_flags: ConsentFlagsRequest
    privacy_signals: Optional[PrivacySignals] = None

class IngestResponse(BaseModel):
    id: int
    ephem_id: str
    timestamp: str
    event_type: str
    campaign_id: Optional[str]
    household_id: Optional[str] = None
    bridging_skip_reason: Optional[str] = None
    bridging_token: Optional[str] = None

class CappingIncrementRequest(BaseModel):
    household_id: str

class CappingIncrementResponse(BaseModel):
    household_id: str
    can_serve: bool
    daily_impressions: int
    cap_limit: int

# ----- INGEST ENDPOINT -----

@app.post("/v1/ingest", response_model=IngestResponse)
async def ingest_event(payload: IngestRequest, db: Session = Depends(get_db)):
    if not ASYNC_INGESTION:
        return _ingest_blocking(payload, db)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _ingest_blocking, payload, db)

def _ingest_blocking(payload: IngestRequest, db: Session) -> IngestResponse:
    # 1) Validate partner
    partner = db.query(Partner).filter(Partner.id == payload.partner_id).first()
    if not partner:
        logger.error(f"Invalid partner_id={payload.partner_id}")
        raise HTTPException(status_code=400, detail="Invalid partner_id")

    # 2) Parse privacy signals
    tcf_data, us_privacy_data= {}, {}
    if payload.privacy_signals:
        if payload.privacy_signals.tcf_string:
            tcf_data = parse_tcf_string(payload.privacy_signals.tcf_string)
        if payload.privacy_signals.us_privacy_string:
            us_privacy_data = parse_us_privacy_string(payload.privacy_signals.us_privacy_string)
       

    bridging_allowed = user_allows_bridging(tcf_data, us_privacy_data)
    final_cross_device = (payload.consent_flags.cross_device_bridging and bridging_allowed)

    cf = ConsentFlags(
        cross_device_bridging=final_cross_device,
        targeting_segments=payload.consent_flags.targeting_segments
    )
    db.add(cf)
    db.commit()
    db.refresh(cf)

    # 3) Create ephemeral event
    partial_keys = payload.partial_keys if final_cross_device else {}
    is_child_flag = (partial_keys.get("isChild", "").lower() == "true")
    device_child_flag = (partial_keys.get("device_child_flag", "").lower() == "true")

    new_event = EphemeralEvent(
        ephem_id=payload.device_data,
        partial_keys=partial_keys,
        event_type=payload.event_type,
        campaign_id=payload.campaign_id,
        partner_id=partner.id,
        consent_flags_id=cf.id,
        privacy_tcf_string=(payload.privacy_signals.tcf_string if payload.privacy_signals else None),
        privacy_us_string=(payload.privacy_signals.us_privacy_string if payload.privacy_signals else None),
        is_child=is_child_flag,
        device_child_flag=device_child_flag
    )
    db.add(new_event)
    db.commit()
    db.refresh(new_event)

    bridging_skip_reason = None
    bridging_token = None

    # 4) Possibly bridging
    if final_cross_device:
        if is_child_flag or device_child_flag:
            bridging_skip_reason = "CHILD_FLAG"
        else:
            hashed_email = partial_keys.get("hashedEmail", "").lower()
            if hashed_email:
                bridging_token = short_circuit_deterministic(new_event.id, hashed_email)
            else:
                queue_fuzzy_bridging.delay(new_event.id)
    else:
        bridging_skip_reason = "NO_CONSENT_OR_FLAGS"

    logger.info(f"Ingested event ID={new_event.id}, bridging={final_cross_device}, skip={bridging_skip_reason}")

    # 5) Possibly sample (anonymous)
    from services.common_lib.sampling import should_sample_event
    if should_sample_event(new_event.event_type):
        hashed_device_sig = (partial_keys.get("hashedIP", "") + partial_keys.get("deviceType", ""))
        anonym_evt = AnonymizedEvent(
            event_id=new_event.id,
            hashed_device_sig=hashed_device_sig,
            hashed_user_sig=partial_keys.get("hashedEmail", ""),
            event_day=str(new_event.timestamp.date()),
            event_type=new_event.event_type,
            partner_id=partner.id
        )
        db.add(anonym_evt)
        db.commit()

    # 6) Possibly store daily aggregates
    date_str = new_event.timestamp.strftime("%Y-%m-%d")
    dev_type = partial_keys.get("deviceType", "unknown") if final_cross_device else "unknown"
    aero_client.buffer_increment_daily_aggregate(date_str, partner.id, dev_type, new_event.event_type)

    # 7) Return bridging knowledge
    household_id = aero_client.get_household_ref(new_event.ephem_id)
    return IngestResponse(
        id=new_event.id,
        ephem_id=new_event.ephem_id,
        timestamp=str(new_event.timestamp),
        event_type=new_event.event_type,
        campaign_id=new_event.campaign_id,
        household_id=household_id,
        bridging_skip_reason=bridging_skip_reason,
        bridging_token=bridging_token
    )

# ----- REAL-TIME CAPPING: increment route -----

@app.post("/v1/capping/increment", response_model=CappingIncrementResponse)
def capping_increment(body: CappingIncrementRequest, db: Session = Depends(get_db)):
    """
    Write-based capping endpoint to increment daily impressions
    """
    result = rtc_manager.increment_impression(db, body.household_id)
    return CappingIncrementResponse(
        household_id=body.household_id,
        can_serve=result["can_serve"],
        daily_impressions=result["daily_impressions"],
        cap_limit=result["cap_limit"]
    )

