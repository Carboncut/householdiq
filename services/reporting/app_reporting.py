from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional
from sqlalchemy.orm import Session
from services.common_lib.logging_config import logger
from services.common_lib.session_utils import get_db
from services.common_lib.models import (
    DailyAggregate, DataSharingAgreement, EphemeralEvent
)
from services.common_lib.data_sharing import store_data_sharing, get_data_sharing_agreement
from services.common_lib.plugin_manager import PluginManager
from services.common_lib.models import PluginRegistry
from services.common_lib.database import SessionLocal
from services.common_lib.privacy import apply_differential_privacy
from services.common_lib.config import settings

app = FastAPI(title="HouseholdIQ Reporting Service", debug=False)

class AggregatesQuery(BaseModel):
    start_date: str
    end_date: str

class AggregatesResponse(BaseModel):
    data: Dict[str, float]

@app.post("/v1/reporting/daily", response_model=AggregatesResponse)
def get_daily_aggregates(body: AggregatesQuery, db: Session = Depends(get_db)):
    """
    Return daily aggregates for a date range
    Possibly apply differential privacy if DP_MODE_ENABLED is True
    """
    rows = db.query(DailyAggregate).filter(
        DailyAggregate.date_str >= body.start_date,
        DailyAggregate.date_str <= body.end_date
    ).all()
    totals = {}
    for r in rows:
        key = f"{r.date_str}|{r.partner_id}|{r.device_type}|{r.event_type}"
        totals[key] = totals.get(key, 0) + r.count

    if settings.DP_MODE_ENABLED:
        # apply noise
        for k in totals:
            totals[k] = float(apply_differential_privacy(totals[k]))

    return AggregatesResponse(data=totals)

class DataSharingRequest(BaseModel):
    initiator_id: int
    recipient_id: int
    details: Optional[str] = None

class DataSharingResponse(BaseModel):
    success: bool
    message: str

@app.post("/v1/data_sharing/agreements", response_model=DataSharingResponse)
def create_data_sharing_agreement(body: DataSharingRequest, db: Session = Depends(get_db)):
    existing = get_data_sharing_agreement(db, body.initiator_id, body.recipient_id)
    if existing:
        existing.agreement_details = body.details or ""
        db.commit()
        return DataSharingResponse(success=True, message="Updated existing agreement.")
    else:
        new_ag = DataSharingAgreement(
            partner_id_initiator=body.initiator_id,
            partner_id_recipient=body.recipient_id,
            agreement_details=body.details or ""
        )
        db.add(new_ag)
        db.commit()
        return DataSharingResponse(success=True, message="Created new agreement.")

@app.post("/v1/data_sharing/export", response_model=DataSharingResponse)
def export_data_sharing(body: DataSharingRequest, db: Session = Depends(get_db)):
    """
    Trigger data export
    """
    event_stub = EphemeralEvent()
    event_stub.partner_id = body.initiator_id
    try:
        store_data_sharing(db, event_stub, body.initiator_id, body.recipient_id)
        return DataSharingResponse(success=True, message="Data export triggered.")
    except Exception as e:
        return DataSharingResponse(success=False, message=str(e))

# Plugin manager endpoints
class PluginListResponse(BaseModel):
    plugins: list

@app.get("/v1/plugins/list", response_model=PluginListResponse)
def list_plugins(db: Session = Depends(get_db)):
    rows = db.query(PluginRegistry).all()
    plugin_data = []
    for r in rows:
        plugin_data.append({
            "plugin_name": r.plugin_name,
            "plugin_path": r.plugin_path,
            "enabled": r.enabled
        })
    return PluginListResponse(plugins=plugin_data)

class PluginActionResponse(BaseModel):
    success: bool
    message: str

@app.post("/v1/plugins/enable", response_model=PluginActionResponse)
def enable_plugin(plugin_name: str, db: Session = Depends(get_db)):
    row = db.query(PluginRegistry).filter(PluginRegistry.plugin_name == plugin_name).first()
    if not row:
        return PluginActionResponse(success=False, message="Plugin not found.")
    row.enabled = True
    db.commit()
    return PluginActionResponse(success=True, message=f"Plugin {plugin_name} enabled.")

@app.post("/v1/plugins/disable", response_model=PluginActionResponse)
def disable_plugin(plugin_name: str, db: Session = Depends(get_db)):
    row = db.query(PluginRegistry).filter(PluginRegistry.plugin_name == plugin_name).first()
    if not row:
        return PluginActionResponse(success=False, message="Plugin not found.")
    row.enabled = False
    db.commit()
    return PluginActionResponse(success=True, message=f"Plugin {plugin_name} disabled.")
