import os
import asyncio
from fastapi import FastAPI, Depends, HTTPException, Query
from pydantic import BaseModel, validator, Field
from typing import Optional, Dict, List, Any, Literal, Tuple
from sqlalchemy.orm import Session, sessionmaker, joinedload
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from datetime import datetime

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
from services.common_lib.tasks import queue_fuzzy_bridging, short_circuit_deterministic
from services.common_lib.daily_aggregates import buffer_increment_daily_aggregate
from services.common_lib.privacy import parse_privacy_signals

# Connection pooling setup
engine = create_engine(
    settings.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Thread pool for async operations
executor = ThreadPoolExecutor(max_workers=10)

# Semaphore for controlling concurrency
request_semaphore = asyncio.Semaphore(10)

@asynccontextmanager
async def get_db_session():
    """Async context manager for database sessions."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

ASYNC_INGESTION = (os.getenv("ASYNC_INGESTION", "false").lower() == "true")
BATCH_SIZE = 100  # Number of events to process in a batch

app = FastAPI(title="HouseholdIQ Ingestion Service", debug=settings.DEBUG)
aero_client = None
rtc_manager = RealTimeCappingManager()

# Event buffer for batch processing
event_buffer: List[Dict] = []

# ----- Pydantic Schemas -----

class ConsentFlagsRequest(BaseModel):
    cross_device_bridging: bool
    targeting_segments: bool

class PrivacySignals(BaseModel):
    tcf_string: Optional[str] = None
    us_privacy_string: Optional[str] = None

class EventType(str, Enum):
    IMPRESSION = "impression"
    CLICK = "click"
    CONVERSION = "conversion"

class PartialKeys(BaseModel):
    hashedEmail: Optional[str] = None
    deviceType: str
    isChild: bool = False
    deviceChildFlag: bool = False
    hashedIP: Optional[str] = None

    # Validate the deviceType
    @validator("deviceType")
    def validate_device_type(cls, v):
        valid_types = ["mobile", "desktop", "tablet"]
        if v.lower() not in valid_types:
            raise ValueError(f"Invalid device type. Must be one of: {valid_types}")
        return v.lower()

    def dict(self, *args, **kwargs):
        """
        Override dict method to ensure consistent key format and JSON serialization.
        We keep the original field names that come in via the API.
        """
        d = super().dict(*args, **kwargs)
        return {
            "hashedEmail": d.get("hashedEmail"),
            "deviceType": d.get("deviceType"),
            "isChild": bool(d.get("isChild", False)),
            "deviceChildFlag": bool(d.get("deviceChildFlag", False)),
            "hashedIP": d.get("hashedIP")
        }

class IngestRequest(BaseModel):
    partner_id: int
    device_data: str
    partial_keys: PartialKeys
    event_type: Literal["impression", "click", "conversion"]
    campaign_id: Optional[str] = None
    consent_flags: ConsentFlagsRequest
    privacy_signals: Optional[PrivacySignals] = None

    @validator("partner_id")
    def validate_partner_id(cls, v):
        if v <= 0:
            raise ValueError("Invalid partner_id")
        return v

    @property
    def hashed_email(self) -> Optional[str]:
        return self.partial_keys.hashedEmail if self.partial_keys else None

    @property
    def device_type(self) -> str:
        return self.partial_keys.deviceType if self.partial_keys else ""

class IngestResponse(BaseModel):
    id: int
    ephem_id: str
    timestamp: str
    event_type: Literal["impression", "click", "conversion"]
    campaign_id: Optional[str] = None
    household_id: Optional[str] = None
    bridging_skip_reason: Optional[str] = None
    bridging_token: Optional[str] = None

    @validator("event_type")
    def validate_event_type(cls, v):
        return v.lower()

    @validator("bridging_skip_reason")
    def validate_bridging_skip_reason(cls, v):
        if v is not None and v not in ["NO_CONSENT_OR_FLAGS", "CHILD_FLAG"]:
            raise ValueError("Invalid bridging_skip_reason")
        return v

class CappingIncrementRequest(BaseModel):
    household_id: str

class CappingIncrementResponse(BaseModel):
    household_id: str
    can_serve: bool
    daily_impressions: int
    cap_limit: int

# ----- INGEST ENDPOINT -----

def init_aerospike():
    """Initialize the Aerospike client (singleton)."""
    global aero_client
    if aero_client is None:
        from services.common_lib.aerospike_cache import AerospikeCache
        aero_client = AerospikeCache()
    return aero_client

def get_aerospike() -> AerospikeCache:
    """Get Aerospike cache instance."""
    return init_aerospike()

@app.post("/v1/ingest", response_model=IngestResponse)
async def ingest_event(
    request: IngestRequest,
    db: Session = Depends(get_db),
    aero_cache: AerospikeCache = Depends(get_aerospike)
) -> IngestResponse:
    """
    Ingest an event with proper validation, bridging logic,
    anonymized event creation, and daily aggregates tracking.
    """
    try:
        # 1) Create consent flags
        consent_flags = _create_consent_flags(
            db,
            request.consent_flags.cross_device_bridging,
            request.consent_flags.targeting_segments
        )

        # 2) Create ephemeral event
        event = _create_ephemeral_event(
            db,
            request,
            request.partner_id,
            consent_flags.id,
            request.consent_flags.cross_device_bridging
        )

        # 3) Parse privacy signals
        tcf_data, us_privacy_data = _parse_privacy_signals(request.privacy_signals)
        bridging_allowed = user_allows_bridging(tcf_data, us_privacy_data)
        final_cross_device = (request.consent_flags.cross_device_bridging and bridging_allowed)

        # 4) Handle bridging
        bridging_result, bridging_token = await _handle_bridging(event)
        bridging_skip_reason = None
        if bridging_result == "NO_CONSENT_OR_FLAGS":
            bridging_skip_reason = "NO_CONSENT_OR_FLAGS"
        elif bridging_result == "CHILD_FLAG":
            bridging_skip_reason = "CHILD_FLAG"

        # 5) Create anonymized event
        _create_anonymized_event(db, event, request.partial_keys.dict())

        # 6) Buffer daily aggregate
        await _buffer_daily_aggregate(
            aero_cache,
            str(event.timestamp.date()),
            event.partner_id,
            event.device_type,
            event.event_type
        )

        # 7) Return response
        return IngestResponse(
            id=event.id,
            ephem_id=event.ephem_id,
            timestamp=str(event.timestamp),
            event_type=event.event_type,
            campaign_id=event.campaign_id,
            bridging_token=bridging_token,
            bridging_skip_reason=bridging_skip_reason
        )
    except HTTPException as e:
        logger.error(f"HTTP error: {str(e)}")
        raise
    except Exception as e:
        logger.exception(f"Error processing event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

def _queue_tasks(event_id: int, hashed_email: Optional[str] = None) -> Dict[str, Any]:
    """Queue background tasks for bridging."""
    try:
        # Queue fuzzy bridging
        queue_fuzzy_bridging.apply_async(args=[event_id], ignore_result=True)

        if hashed_email:
            token = f"{event_id}_{hashed_email}"
            short_circuit_deterministic.apply_async(
                args=[event_id, hashed_email],
                task_id=token,
                ignore_result=True
            )
            return {"bridging_token": token}

        return {"event_id": event_id}
    except Exception as e:
        logger.exception(f"Error queueing tasks: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Task queueing error: {str(e)}")

async def _ingest_async(payload: IngestRequest) -> IngestResponse:
    """Example of asynchronous ingestion logic with concurrent bridging tasks."""
    async with get_db_session() as db:
        try:
            # 1) Aerospike client
            aero_cache = init_aerospike()

            # 2) Validate partner
            partner = await asyncio.get_event_loop().run_in_executor(
                executor, _get_partner, db, payload.partner_id
            )
            if not partner:
                logger.error(f"Invalid partner_id={payload.partner_id}")
                raise HTTPException(status_code=400, detail="Invalid partner_id")

            # 3) Parse privacy signals
            tcf_data, us_privacy_data = await asyncio.get_event_loop().run_in_executor(
                executor, _parse_privacy_signals, payload.privacy_signals
            )
            bridging_allowed = user_allows_bridging(tcf_data, us_privacy_data)
            final_cross_device = (payload.consent_flags.cross_device_bridging and bridging_allowed)

            # 4) Create consent flags
            cf = await asyncio.get_event_loop().run_in_executor(
                executor, _create_consent_flags, db,
                final_cross_device, payload.consent_flags.targeting_segments
            )

            # 5) Create ephemeral event
            new_event = await asyncio.get_event_loop().run_in_executor(
                executor, _create_ephemeral_event,
                db, payload, partner.id, cf.id, final_cross_device
            )

            # 6) Handle bridging
            bridging_result, bridging_token = await _handle_bridging(new_event)

            # 7) Optional sampling -> anonymized event
            if should_sample_event(new_event.event_type):
                await asyncio.get_event_loop().run_in_executor(
                    executor, _create_anonymized_event,
                    db, new_event, payload.partial_keys.dict()
                )

            # 8) Buffer daily aggregates
            await asyncio.get_event_loop().run_in_executor(
                executor, _buffer_daily_aggregate,
                aero_cache,
                str(datetime.now().strftime('%Y-%m-%d')),
                partner.id,
                payload.device_type,
                new_event.event_type
            )

            # 9) Retrieve household ref
            household_id = await aero_cache.get_household_ref(new_event.ephem_id)

            # 10) Queue bridging tasks
            tasks = _queue_tasks(new_event.id, payload.hashed_email)

            # 11) Build response
            bridging_skip_reason = None
            if bridging_result == "NO_CONSENT_OR_FLAGS":
                bridging_skip_reason = "NO_CONSENT_OR_FLAGS"
            elif bridging_result == "CHILD_FLAG":
                bridging_skip_reason = "CHILD_FLAG"

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
        except HTTPException:
            raise
        except Exception as e:
            logger.exception(f"Error in async ingestion: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

# ----- Helper Functions -----

def _get_partner(db: Session, partner_id: int) -> Optional[Partner]:
    """Fetch partner object, with a short-circuit for test partners."""
    try:
        if settings.DEBUG and 1 <= partner_id <= 10:
            return Partner(id=partner_id, name=f"Test Partner {partner_id}")

        partner = db.query(Partner).filter(Partner.id == partner_id).first()
        if not partner:
            raise HTTPException(status_code=400, detail="Invalid partner_id")
        return partner
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting partner: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

def _parse_privacy_signals(privacy_signals: Optional[PrivacySignals]) -> tuple:
    """
    Parse TCF and US Privacy signals. In debug mode, we allow invalid strings.
    """
    try:
        tcf_data, us_privacy_data = {}, {}
        if privacy_signals:
            if privacy_signals.tcf_string:
                try:
                    tcf_data = parse_tcf_string(privacy_signals.tcf_string)
                except:
                    if settings.DEBUG:
                        tcf_data = {"test": True}
                    else:
                        raise
            if privacy_signals.us_privacy_string:
                try:
                    us_privacy_data = parse_us_privacy_string(privacy_signals.us_privacy_string)
                except:
                    if settings.DEBUG:
                        us_privacy_data = {"test": True}
                    else:
                        raise
        return tcf_data, us_privacy_data
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error parsing privacy signals: {str(e)}")
        if settings.DEBUG:
            return {}, {}
        raise HTTPException(status_code=400, detail=f"Invalid privacy signals: {str(e)}")

def _create_consent_flags(db: Session, cross_device: bool, targeting: bool) -> ConsentFlags:
    """Create and store a ConsentFlags row."""
    try:
        cf = ConsentFlags(
            cross_device_bridging=cross_device,
            targeting_segments=targeting
        )
        db.add(cf)
        db.commit()
        db.refresh(cf)
        return cf
    except Exception as e:
        db.rollback()
        logger.exception(f"Error creating consent flags: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

def _create_ephemeral_event(
    db: Session,
    request: IngestRequest,
    partner_id: int,
    consent_flags_id: int,
    cross_device: bool
) -> EphemeralEvent:
    """Create EphemeralEvent (the raw event row) from the request."""
    try:
        # Convert partial_keys to a dict
        partial_keys_dict = request.partial_keys.dict()

        ev = EphemeralEvent(
            ephem_id=request.device_data,
            partial_keys=partial_keys_dict,
            event_type=request.event_type,
            campaign_id=request.campaign_id,
            partner_id=partner_id,
            consent_flags_id=consent_flags_id,
            privacy_tcf_string=(
                request.privacy_signals.tcf_string if request.privacy_signals else None
            ),
            privacy_us_string=(
                request.privacy_signals.us_privacy_string if request.privacy_signals else None
            ),
            timestamp=datetime.now()
        )
        db.add(ev)
        db.commit()
        db.refresh(ev)

        # Eager-load consent flags
        ev = db.query(EphemeralEvent).options(
            joinedload(EphemeralEvent.consent_flags)
        ).filter(EphemeralEvent.id == ev.id).first()

        logger.info(f"Created event with ID {ev.id}")
        logger.info(f"Partial keys: {ev.partial_keys}")
        return ev

    except Exception as e:
        db.rollback()
        logger.exception(f"Error creating ephemeral event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

async def _handle_bridging(event: EphemeralEvent) -> Tuple[str, Optional[str]]:
    """Handle child flags and bridging consent for this event."""
    try:
        logger.info(f"Handling bridging for event {event.id}")
        logger.info(f"Partial keys: {event.partial_keys}")

        is_child = event.partial_keys.get('isChild', False)
        device_child_flag = event.partial_keys.get('deviceChildFlag', False)

        if is_child or device_child_flag:
            logger.info(f"Child flag detected: is_child={is_child}, device_child_flag={device_child_flag}")
            return "CHILD_FLAG", None

        if not event.consent_flags:
            logger.info("No consent flags found")
            return "NO_CONSENT_OR_FLAGS", None

        if not event.consent_flags.cross_device_bridging:
            logger.info("No consent for cross-device bridging")
            return "NO_CONSENT_OR_FLAGS", None

        hashed_email = event.partial_keys.get("hashedEmail")
        if not hashed_email:
            logger.info("No hashed email found")
            return "NO_HASHED_EMAIL", None

        # Queue bridging tasks
        tasks = _queue_tasks(event.id, hashed_email)
        logger.info(f"Bridging tasks queued for event {event.id}")
        return "BRIDGING_QUEUED", tasks.get("bridging_token")

    except Exception as e:
        logger.error(f"Error in bridging handler: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to process device bridging"
        )

def _create_anonymized_event(
    db: Session,
    event: EphemeralEvent,
    partial_keys: Dict
):
    """Create an AnonymizedEvent for sampling or reporting."""
    try:
        hashed_device_sig = (
            partial_keys.get("hashedIP", "")
            + partial_keys.get("deviceType", "")
        )
        anonym_evt = AnonymizedEvent(
            event_id=event.id,
            hashed_device_sig=hashed_device_sig,
            hashed_user_sig=partial_keys.get("hashedEmail", ""),
            event_day=str(event.timestamp.date()),
            event_type=event.event_type,
            partner_id=event.partner_id
        )
        db.add(anonym_evt)
        db.commit()
    except Exception as e:
        db.rollback()
        logger.exception(f"Error creating anonymized event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

async def _buffer_daily_aggregate(
    aero_cache: AerospikeCache,
    date_str: str,
    partner_id: int,
    device_type: str,
    event_type: str
):
    """
    Buffer daily aggregate increment with proper async handling.
    If Aerospike fails, we raise a 500 with a 'Failed to buffer daily aggregate' detail.
    """
    # Log the arguments to see what we're passing
    logger.info(
        f"Buffering daily aggregate: date={date_str}, "
        f"partner={partner_id}, device={device_type}, event={event_type}"
    )
    try:
        await aero_cache.buffer_increment_daily_aggregate(
            date_str,
            partner_id,
            device_type,
            event_type
        )
        logger.debug(f"Successfully buffered daily aggregate for {date_str}|{partner_id}|{device_type}|{event_type}")
    except Exception as e:
        logger.exception("Error buffering daily aggregate")
        raise HTTPException(
            status_code=500,
            detail="Failed to buffer daily aggregate"
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

def daily_agg_handler(key, meta, record):
    """Handler for daily aggregation records."""
    records = []
    try:
        if record and isinstance(record, dict):
            records.append((key, record))
    except Exception as e:
        logger.error(f"Error in daily_agg_handler: {e}")
    return records
