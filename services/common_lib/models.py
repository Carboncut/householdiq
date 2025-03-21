from typing import Any, Optional
from sqlalchemy import (
    Column, String, Float, DateTime, Integer, Boolean, JSON,
    ForeignKey, Text, func
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime
import json

Base = declarative_base()

class Partner(Base):
    __tablename__ = "partners"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    salt = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    namespace = Column(String, nullable=True)

class ConsentFlags(Base):
    __tablename__ = "consent_flags"
    id = Column(Integer, primary_key=True)
    cross_device_bridging = Column(Boolean, default=True)
    targeting_segments = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class EphemeralEvent(Base):
    __tablename__ = "ephemeral_events"

    id = Column(Integer, primary_key=True)
    ephem_id = Column(String, nullable=False)
    partial_keys = Column(JSON, nullable=False)
    event_type = Column(String, nullable=False)
    campaign_id = Column(String, nullable=True)
    partner_id = Column(Integer, ForeignKey("partners.id"), nullable=False)
    consent_flags_id = Column(Integer, ForeignKey("consent_flags.id"), nullable=False)
    privacy_tcf_string = Column(String, nullable=True)
    privacy_us_string = Column(String, nullable=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.now)

    # Relationships
    consent_flags = relationship("ConsentFlags", backref="ephemeral_events")

    def __init__(self, **kwargs: Any):
        """
        Custom constructor so we can handle 'partial_keys' explicitly
        while still letting SQLAlchemy map all other columns.
        """
        partial_keys = kwargs.pop('partial_keys', None)
        super().__init__(**kwargs)

        # If partial_keys was a string, parse as JSON
        if isinstance(partial_keys, str):
            try:
                partial_keys = json.loads(partial_keys)
            except json.JSONDecodeError:
                partial_keys = {}

        self.partial_keys = partial_keys if partial_keys is not None else {}

    def get_partial_key(self, name: str) -> Any:
        """Retrieve a value from partial_keys by name."""
        if not self.partial_keys:
            return None
        return self.partial_keys.get(name)

    def set_partial_key(self, name: str, value: Any) -> None:
        """Set a value in partial_keys by name."""
        if not self.partial_keys:
            self.partial_keys = {}
        self.partial_keys[name] = value

    @property
    def device_type(self) -> Optional[str]:
        """
        A read-only property to match any code that does `event.device_type`.
        Returns `partial_keys["deviceType"]` or None if missing.
        """
        return self.get_partial_key("deviceType")

    @property
    def hashed_email(self) -> Optional[str]:
        """Returns `partial_keys["hashedEmail"]`."""
        return self.get_partial_key("hashedEmail")

    @property
    def hashed_ip(self) -> Optional[str]:
        """Returns `partial_keys["hashedIP"]`."""
        return self.get_partial_key("hashedIP")

    @property
    def is_child(self) -> bool:
        """
        Returns True/False based on partial_keys["isChild"]. 
        Accepts either a boolean or a string like "true"/"false".
        """
        raw_val = self.get_partial_key("isChild")
        if isinstance(raw_val, bool):
            return raw_val
        if isinstance(raw_val, str) and raw_val.lower() == "true":
            return True
        return False

class BridgingReference(Base):
    __tablename__ = "bridging_references"
    id = Column(Integer, primary_key=True)
    household_ephem_id = Column(String, nullable=True)
    linked_ephem_ids = Column(ARRAY(String), default=[])
    confidence_score = Column(Float, default=0.0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expiry_timestamp = Column(DateTime(timezone=True), nullable=True)

class DataSharingAgreement(Base):
    __tablename__ = "data_sharing_agreements"
    id = Column(Integer, primary_key=True)
    partner_id_initiator = Column(Integer, ForeignKey("partners.id"), nullable=False)
    partner_id_recipient = Column(Integer, ForeignKey("partners.id"), nullable=False)
    agreement_details = Column(Text, nullable=True)
    start_date = Column(DateTime(timezone=True), nullable=True)
    end_date = Column(DateTime(timezone=True), nullable=True)
    allow_aggregated_data_sharing = Column(Boolean, default=True)
    min_k_anonymity = Column(Integer, default=50)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class AnonymizedEvent(Base):
    __tablename__ = "anonymized_events"
    id = Column(Integer, primary_key=True)
    event_id = Column(Integer, nullable=False)
    hashed_device_sig = Column(String, nullable=True)
    hashed_user_sig = Column(String, nullable=True)
    event_day = Column(String, nullable=True)
    event_type = Column(String, nullable=True)
    partner_id = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class DailyAggregate(Base):
    __tablename__ = "daily_aggregates"
    id = Column(Integer, primary_key=True)
    date_str = Column(String, nullable=False)
    partner_id = Column(Integer, nullable=False)
    device_type = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    count = Column(Integer, default=0)
    last_updated = Column(DateTime(timezone=True), server_default=func.now())

class BridgingConfig(Base):
    __tablename__ = "bridging_config"
    id = Column(Integer, primary_key=True)
    threshold = Column(Float, nullable=True)
    partial_key_weights = Column(JSON, nullable=True)
    last_updated = Column(DateTime(timezone=True), server_default=func.now())
    time_decay_factor = Column(Float, nullable=True)

class FrequencyCapping(Base):
    __tablename__ = "frequency_capping"
    id = Column(Integer, primary_key=True)
    household_id = Column(String, nullable=False)
    daily_impressions = Column(Integer, default=0)
    cap_limit = Column(Integer, default=5)
    updated_at = Column(DateTime(timezone=True), server_default=func.now())

class ConsentRevocation(Base):
    __tablename__ = "consent_revocations"
    id = Column(Integer, primary_key=True)
    ephem_id = Column(String, nullable=False)
    revoked_at = Column(DateTime(timezone=True), server_default=func.now())

class MLBridgingThreshold(Base):
    __tablename__ = "ml_bridging_thresholds"
    id = Column(Integer, primary_key=True)
    model_version = Column(String, nullable=False)
    threshold_value = Column(Float, default=0.7)
    last_trained = Column(DateTime(timezone=True), server_default=func.now())

class AttributionJourney(Base):
    __tablename__ = "attribution_journeys"
    id = Column(Integer, primary_key=True)
    household_id = Column(String, nullable=False)
    conversion_time = Column(DateTime(timezone=True), nullable=True)
    touch_points = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class LookalikeSegment(Base):
    __tablename__ = "lookalike_segments"
    id = Column(Integer, primary_key=True)
    seed_segment = Column(String, nullable=False)
    matched_households = Column(ARRAY(String), default=[])
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class PluginRegistry(Base):
    __tablename__ = "plugin_registry"
    id = Column(Integer, primary_key=True)
    plugin_name = Column(String, nullable=False)
    plugin_path = Column(String, nullable=False)
    enabled = Column(Boolean, default=True)

class WebhookSubscription(Base):
    __tablename__ = "webhook_subscriptions"
    id = Column(Integer, primary_key=True)
    subscriber_name = Column(String, nullable=False)
    callback_url = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
