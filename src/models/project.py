"""Pydantic models for large load interconnection projects."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class ProjectCategory(str, Enum):
    DATA_CENTER = "data_center"
    INDUSTRIAL = "industrial"
    CRYPTO_MINING = "crypto_mining"
    EV_CHARGING = "ev_charging"
    HYDROGEN = "hydrogen"
    OTHER = "other"
    UNKNOWN = "unknown"


class ProjectStatus(str, Enum):
    ACTIVE = "active"
    WITHDRAWN = "withdrawn"
    COMPLETED = "completed"
    SUSPENDED = "suspended"
    UNKNOWN = "unknown"


class ConfidenceLevel(str, Enum):
    HIGH = "high"      # Official queue row with explicit MW + date + POI
    MEDIUM = "medium"  # Official doc with MW/date but partial location/POI
    LOW = "low"        # Inferred category or incomplete date; never invent MW


class SourceField(BaseModel):
    """Per-field provenance tracking."""
    value: Any = None
    source_url: Optional[str] = None
    source_name: Optional[str] = None
    extracted_at: Optional[datetime] = None
    confidence: ConfidenceLevel = ConfidenceLevel.MEDIUM

    model_config = {"arbitrary_types_allowed": True}


class Project(BaseModel):
    """
    Represents a single large load interconnection project (≥100 MW).
    Follows the standard schema across all ISOs.
    """
    # --- Identity ---
    id: Optional[str] = None  # Computed deterministic hash
    iso: str = Field(..., description="ISO/RTO (NYISO, PJM, MISO, SPP, CAISO, ISO-NE, ERCOT)")
    queue_id: Optional[str] = Field(None, description="ISO queue ID / project number if available")
    project_name: Optional[str] = Field(None, description="Project name as provided by source")

    # --- Type / Category ---
    category: ProjectCategory = Field(ProjectCategory.UNKNOWN, description="Project type")
    status: ProjectStatus = Field(ProjectStatus.UNKNOWN, description="Project status")

    # --- Capacity ---
    mw_requested: Optional[float] = Field(None, description="Requested/nameplate MW (original definition from source)")
    mw_adjusted: Optional[float] = Field(None, description="Adjusted MW after any capacity revisions")
    mw_in_service: Optional[float] = Field(None, description="Expected final in-service MW")
    mw_definition: Optional[str] = Field(None, description="Which MW definition is used (nameplate/requested/adjusted)")

    # --- Dates ---
    in_service_date: Optional[date] = Field(None, description="Target in-service / energization / COD date")
    in_service_date_type: Optional[str] = Field(None, description="What the date means (energization/COD/RIS/etc.)")
    queue_date: Optional[date] = Field(None, description="Date project entered queue")

    # --- Location ---
    state: Optional[str] = Field(None, description="2-letter state abbreviation")
    county: Optional[str] = Field(None, description="County name")
    city: Optional[str] = Field(None, description="City/municipality")
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)

    # --- Point of Interconnection ---
    utility: Optional[str] = Field(None, description="Utility / Transmission Service Provider")
    substation: Optional[str] = Field(None, description="Interconnection substation")
    voltage_kv: Optional[float] = Field(None, description="Interconnection voltage in kV")
    poi_text: Optional[str] = Field(None, description="Full POI / transmission line text as in source")
    transmission_owner: Optional[str] = Field(None, description="Transmission owner")

    # --- Source Provenance ---
    source_url: Optional[str] = Field(None, description="Primary source URL")
    source_name: Optional[str] = Field(None, description="Source display name (e.g. 'NYISO Interconnection Queue')")
    source_iso: Optional[str] = Field(None, description="ISO/RTO that published this source")
    additional_sources: list[str] = Field(default_factory=list, description="Additional source URLs")
    confidence: ConfidenceLevel = Field(ConfidenceLevel.MEDIUM)
    field_provenance: dict[str, dict] = Field(
        default_factory=dict,
        description="Per-field provenance: {field_name: {source_url, source_name, extracted_at}}"
    )

    # --- Metadata ---
    last_checked: Optional[datetime] = Field(None, description="Last time source was checked")
    first_seen: Optional[datetime] = Field(None, description="First time this project was seen")
    last_updated: Optional[datetime] = Field(None, description="Last time project data changed")
    raw_data: Optional[dict] = Field(None, description="Raw source row for debugging")
    notes: Optional[str] = Field(None, description="Any notes or caveats")

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("state")
    @classmethod
    def normalize_state(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return v.strip().upper()[:2]

    @field_validator("mw_requested", "mw_adjusted", "mw_in_service")
    @classmethod
    def validate_mw(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v < 0:
            raise ValueError("MW must be non-negative")
        return v

    @model_validator(mode="after")
    def compute_id(self) -> "Project":
        if self.id is None:
            self.id = self._compute_id()
        return self

    def _compute_id(self) -> str:
        """Deterministic ID based on ISO + queue_id if available, else hash of key fields."""
        if self.iso and self.queue_id:
            key = f"{self.iso}::{self.queue_id}"
        else:
            parts = [
                self.iso or "",
                (self.project_name or "").lower().strip(),
                self.state or "",
                self.county or "",
                str(round(self.mw_requested or 0, 0)),
            ]
            key = "::".join(parts)
        return hashlib.md5(key.encode()).hexdigest()[:16]

    @property
    def mw_display(self) -> Optional[float]:
        """Best available MW figure for display."""
        return self.mw_in_service or self.mw_adjusted or self.mw_requested

    @property
    def location_display(self) -> str:
        parts = [p for p in [self.city, self.county, self.state] if p]
        return ", ".join(parts) if parts else "Unknown"

    def to_row(self) -> dict:
        """Flat dict for DataFrame display."""
        return {
            "id": self.id,
            "iso": self.iso,
            "queue_id": self.queue_id or "",
            "project_name": self.project_name or "Unknown",
            "category": self.category.value,
            "status": self.status.value,
            "mw": self.mw_display,
            "mw_requested": self.mw_requested,
            "mw_adjusted": self.mw_adjusted,
            "mw_definition": self.mw_definition or "",
            "in_service_date": self.in_service_date.isoformat() if self.in_service_date else "",
            "in_service_date_type": self.in_service_date_type or "",
            "state": self.state or "",
            "county": self.county or "",
            "city": self.city or "",
            "latitude": self.latitude,
            "longitude": self.longitude,
            "utility": self.utility or "",
            "substation": self.substation or "",
            "voltage_kv": self.voltage_kv,
            "poi_text": self.poi_text or "",
            "confidence": self.confidence.value,
            "source_name": self.source_name or "",
            "source_url": self.source_url or "",
            "last_checked": self.last_checked.isoformat() if self.last_checked else "",
        }
