import os
from fastapi import FastAPI, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session

from services.common_lib.logging_config import logger
from services.common_lib.session_utils import get_db
from services.common_lib.aerospike_cache import AerospikeCache
from services.common_lib.models import FrequencyCapping
from services.common_lib.real_time_capping import RealTimeCappingManager

app = FastAPI(title="HouseholdIQ Lookup Service", debug=False)
aero_client = AerospikeCache()
rtc_manager = RealTimeCappingManager()

@app.get("/v1/lookup")
def lookup_ephemeral(ephem_id: str = Query(...)):
    """
    Very fast ephemeral -> household ID read
    """
    household_id = aero_client.get_household_ref(ephem_id)
    if not household_id:
        return {"household_id": None, "status": "not_found"}
    # optionally get confidence score
    score = aero_client.get_household_average_score(household_id)
    return {
        "household_id": household_id,
        "confidence_score": score,
        "status": "matched"
    }

class CappingCheckRequest(BaseModel):
    household_id: str

class CappingCheckResponse(BaseModel):
    household_id: str
    can_serve: bool
    daily_impressions: int
    cap_limit: int

@app.post("/v1/capping/check", response_model=CappingCheckResponse)
def capping_check(body: CappingCheckRequest, db: Session = Depends(get_db)):
    """
    High-speed read-based capping check
    """
    result = rtc_manager.check_cap(db, body.household_id)
    return CappingCheckResponse(
        household_id=body.household_id,
        can_serve=result["can_serve"],
        daily_impressions=result["daily_impressions"],
        cap_limit=result["cap_limit"]
    )
