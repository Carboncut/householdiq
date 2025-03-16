from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session
from services.common_lib.logging_config import logger
from services.common_lib.session_utils import get_db
from services.common_lib.models import Partner

app = FastAPI(title="HouseholdIQ Customer Service", debug=False)

class CreateCustomerRequest(BaseModel):
    name: str
    salt: Optional[str] = "default_salt"
    namespace: Optional[str] = None

class CreateCustomerResponse(BaseModel):
    id: int
    name: str
    salt: str
    namespace: Optional[str]

@app.post("/v1/customers/create", response_model=CreateCustomerResponse)
def create_customer(body: CreateCustomerRequest, db: Session = Depends(get_db)):
    """
    Creates a new partner or 'customer' record in the aggregator
    """
    existing = db.query(Partner).filter(Partner.name == body.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Partner name already exists")

    partner = Partner(name=body.name, salt=body.salt)
    db.add(partner)
    db.commit()
    db.refresh(partner)

    if body.namespace:
        partner.namespace = body.namespace
        db.commit()
        db.refresh(partner)

    return CreateCustomerResponse(
        id=partner.id,
        name=partner.name,
        salt=partner.salt,
        namespace=partner.namespace
    )

class UpdateCustomerRequest(BaseModel):
    salt: Optional[str]
    namespace: Optional[str]

@app.post("/v1/customers/update/{partner_id}", response_model=CreateCustomerResponse)
def update_customer(partner_id: int, body: UpdateCustomerRequest, db: Session = Depends(get_db)):
    """
    Updates existing partner's salt, namespace
    """
    partner = db.query(Partner).filter(Partner.id == partner_id).first()
    if not partner:
        raise HTTPException(status_code=404, detail="Partner not found")

    if body.salt is not None:
        partner.salt = body.salt
    if body.namespace is not None:
        partner.namespace = body.namespace

    db.commit()
    db.refresh(partner)
    return CreateCustomerResponse(
        id=partner.id,
        name=partner.name,
        salt=partner.salt,
        namespace=partner.namespace
    )

@app.get("/v1/customers/{partner_id}", response_model=CreateCustomerResponse)
def get_customer(partner_id: int, db: Session = Depends(get_db)):
    """
    Retrieve partner details
    """
    partner = db.query(Partner).filter(Partner.id == partner_id).first()
    if not partner:
        raise HTTPException(status_code=404, detail="Partner not found")

    return CreateCustomerResponse(
        id=partner.id,
        name=partner.name,
        salt=partner.salt,
        namespace=partner.namespace
    )
