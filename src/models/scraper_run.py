"""Model for tracking scraper execution history."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ScraperStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"      # Fetched but some parsing issues
    FAILED = "failed"
    SKIPPED = "skipped"      # Content unchanged (ETag/hash match)
    RATE_LIMITED = "rate_limited"


class ScraperRun(BaseModel):
    """Record of a single scraper execution."""
    run_id: Optional[str] = None
    source_key: str              # Config key (e.g., "nyiso", "pjm_load_forecast")
    source_name: str
    iso: str
    started_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    status: ScraperStatus = ScraperStatus.FAILED
    projects_found: int = 0
    projects_new: int = 0
    projects_updated: int = 0
    projects_removed: int = 0
    filings_found: int = 0
    error_message: Optional[str] = None
    url: Optional[str] = None
    content_hash: Optional[str] = None   # Hash of downloaded content
    bytes_downloaded: Optional[int] = None
    fields_produced: list[str] = Field(default_factory=list)
    log_lines: list[str] = Field(default_factory=list)

    model_config = {"arbitrary_types_allowed": True}

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.finished_at and self.started_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None
