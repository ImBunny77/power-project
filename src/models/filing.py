"""Models for FERC dockets and filing documents."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class FilingDocument(BaseModel):
    """A single document within a FERC docket or ISO filing."""
    doc_id: Optional[str] = None
    docket_id: str
    title: str
    filed_date: Optional[datetime] = None
    filer: Optional[str] = None
    doc_type: Optional[str] = None  # "order", "comments", "filing", "reply", etc.
    url: str
    pdf_parsed: bool = False
    has_project_table: bool = False
    extracted_text_snippet: Optional[str] = None  # First 500 chars of extracted text
    keywords_found: list[str] = Field(default_factory=list)
    retrieved_at: Optional[datetime] = None

    model_config = {"arbitrary_types_allowed": True}


class FercDocket(BaseModel):
    """A FERC rulemaking or other docket."""
    docket_id: str  # e.g. "RM26-4"
    name: str
    url: str
    description: Optional[str] = None
    documents: list[FilingDocument] = Field(default_factory=list)
    last_fetched: Optional[datetime] = None
    total_docs: int = 0
    keywords: list[str] = Field(default_factory=list)

    model_config = {"arbitrary_types_allowed": True}


class Filing(BaseModel):
    """Generic filing tracker entry (can be FERC, ISO, or state PUC)."""
    filing_id: Optional[str] = None
    docket_id: Optional[str] = None
    source: str  # "FERC", "NYISO", "PJM", etc.
    title: str
    filed_date: Optional[datetime] = None
    filer: Optional[str] = None
    url: str
    summary: Optional[str] = None
    keywords_found: list[str] = Field(default_factory=list)
    has_project_data: bool = False
    retrieved_at: Optional[datetime] = None

    model_config = {"arbitrary_types_allowed": True}
