"""PJM Load Forecast + Large Load scraper."""
from __future__ import annotations

import io
import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.models.project import (
    ConfidenceLevel, Project, ProjectCategory, ProjectStatus
)
from src.models.scraper_run import ScraperRun, ScraperStatus
from src.scrapers.base import BaseScraper
from src.utils.downloader import download_file
from src.utils.pdf_parser import extract_pdf_tables, extract_pdf_text, find_tables_with_keyword

logger = logging.getLogger(__name__)

PJM_DOMAINS = {"pjm.com", "www.pjm.com"}

LARGE_LOAD_SEARCH_PAGES = [
    "https://www.pjm.com/planning/services-requests/large-load-interconnection",
    "https://www.pjm.com/committees-groups/subcommittees/las",
    "https://www.pjm.com/planning/load-forecast",
    "https://www.pjm.com/-/media/DotCom/committees-groups/subcommittees/las/postings/",
]

LOAD_KEYWORDS = ["large load", "data center", "co-located", "provisional load", "load adjustment"]


class PJMScraper(BaseScraper):
    """Scrapes PJM load forecast XLSX and large load adjustment PDFs."""

    source_key = "pjm"
    source_name = "PJM Load Forecast & Large Load Adjustments"
    iso = "PJM"

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []

        # 1) Load forecast XLSX
        xlsx_url = self.config.get(
            "url",
            "https://www.pjm.com/-/media/DotCom/planning/res-adeq/load-forecast/2025-load-report-tables.xlsx"
        )
        self._log(f"Downloading PJM load forecast XLSX from {xlsx_url}")
        xlsx_result = download_file(xlsx_url)
        if xlsx_result.success and xlsx_result.content:
            run.content_hash = xlsx_result.content_hash
            run.bytes_downloaded = (xlsx_result.bytes_downloaded or 0)
            xlsx_projects = self._parse_load_forecast(xlsx_result.content, xlsx_url)
            self._log(f"PJM XLSX: {len(xlsx_projects)} projects")
            projects.extend(xlsx_projects)

        # 2) Large load adjustment PDF
        pdf_url = self.config.get(
            "pdf_url",
            "https://www.pjm.com/-/media/DotCom/committees-groups/subcommittees/las/postings/load-adjustment-request-implementation.pdf"
        )
        self._log(f"Downloading PJM large load adjustment PDF from {pdf_url}")
        pdf_result = download_file(pdf_url)
        if pdf_result.success and pdf_result.content:
            run.bytes_downloaded = (run.bytes_downloaded or 0) + (pdf_result.bytes_downloaded or 0)
            pdf_projects = self._parse_large_load_pdf(pdf_result.content, pdf_url)
            self._log(f"PJM adjustment PDF: {len(pdf_projects)} projects")
            projects.extend(pdf_projects)

        # 3) Document discovery
        disc_projects, disc_docs = self._discover_pjm_documents()
        self._log(f"PJM discovery: {len(disc_projects)} additional projects, {len(disc_docs)} docs")
        projects.extend(disc_projects)
        run.filings_found = len(disc_docs)

        run.projects_found = len(projects)
        run.fields_produced = [
            "project_name", "mw_requested", "state", "in_service_date",
            "utility", "confidence"
        ]

        # Save discovered filing docs to DB if available
        if self.db and disc_docs:
            for doc in disc_docs:
                try:
                    self.db.upsert_filing_document(doc)
                except Exception as e:
                    self._log(f"Failed to save filing doc: {e}")

        status = ScraperStatus.SUCCESS if projects else ScraperStatus.PARTIAL
        return projects, self._finish_run(status)

    def _parse_load_forecast(self, content: bytes, source_url: str) -> list[Project]:
        """Parse PJM load forecast XLSX for large load data."""
        projects = []
        now = datetime.utcnow()
        try:
            xl = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
            self._log(f"PJM XLSX sheets: {xl.sheet_names}")

            for sheet_name in xl.sheet_names:
                sheet_lower = sheet_name.lower()
                if any(kw in sheet_lower for kw in ["large load", "data center", "adjustment", "load request"]):
                    try:
                        df = xl.parse(sheet_name)
                        sheet_projects = self._parse_large_load_sheet(df, source_url, sheet_name)
                        projects.extend(sheet_projects)
                        self._log(f"Sheet '{sheet_name}': {len(sheet_projects)} projects")
                    except Exception as e:
                        self._log(f"Sheet '{sheet_name}' parse error: {e}")

            # Also scan all sheets for MW data
            if not projects:
                for sheet_name in xl.sheet_names[:5]:  # Limit to first 5
                    try:
                        df = xl.parse(sheet_name)
                        sheet_projects = self._parse_large_load_sheet(df, source_url, sheet_name)
                        if sheet_projects:
                            projects.extend(sheet_projects)
                            self._log(f"Found {len(sheet_projects)} projects in sheet '{sheet_name}'")
                    except Exception as e:
                        self._log(f"Sheet scan error '{sheet_name}': {e}")

        except Exception as e:
            self._log(f"PJM XLSX parse error: {e}")
        return projects

    def _parse_large_load_sheet(self, df: pd.DataFrame, source_url: str, sheet_name: str) -> list[Project]:
        """Extract large load projects from a DataFrame sheet."""
        projects = []
        now = datetime.utcnow()

        # Look for MW column
        mw_col = None
        name_col = None
        state_col = None
        date_col = None
        zone_col = None

        cols_lower = {str(c).lower(): str(c) for c in df.columns}
        for col_lower, col_orig in cols_lower.items():
            if "mw" in col_lower and "winter" not in col_lower:
                mw_col = col_orig
            if any(kw in col_lower for kw in ["name", "project", "customer", "applicant"]):
                name_col = col_orig
            if "state" in col_lower:
                state_col = col_orig
            if any(kw in col_lower for kw in ["date", "in-service", "cod", "commercial"]):
                date_col = col_orig
            if any(kw in col_lower for kw in ["zone", "lda", "area"]):
                zone_col = col_orig

        if not mw_col:
            return projects

        for _, row in df.iterrows():
            try:
                mw = self.parse_mw(row.get(mw_col))
                if mw is None or mw < 100:
                    continue

                name = str(row.get(name_col, "")).strip() if name_col else None
                if name in ("nan", "None", ""):
                    name = None

                state_raw = str(row.get(state_col, "")).strip() if state_col else None
                state = None
                if state_raw and state_raw not in ("nan", "None", ""):
                    state = state_raw[:2].upper()

                zone = str(row.get(zone_col, "")).strip() if zone_col else None
                if zone in ("nan", "None", ""):
                    zone = None

                in_service_date = self.parse_date(row.get(date_col) if date_col else None)

                project = Project(
                    iso="PJM",
                    project_name=name or f"PJM Large Load ({sheet_name})",
                    category=self.classify_category(name or ""),
                    status=ProjectStatus.ACTIVE,
                    mw_requested=mw,
                    mw_definition=f"MW from PJM load forecast sheet '{sheet_name}'",
                    in_service_date=in_service_date,
                    in_service_date_type="Forecast in-service date (PJM)",
                    state=state,
                    utility=zone,
                    source_url=source_url,
                    source_name=f"PJM Load Forecast - {sheet_name}",
                    source_iso="PJM",
                    confidence=ConfidenceLevel.MEDIUM,
                    last_checked=now,
                    notes=f"From PJM load forecast XLSX, sheet: {sheet_name}",
                )
                projects.append(project)
            except Exception:
                continue

        return projects

    def _parse_large_load_pdf(self, content: bytes, source_url: str) -> list[Project]:
        """Parse PJM large load adjustment request PDF."""
        projects = []
        now = datetime.utcnow()

        try:
            # Try table extraction first
            tables = find_tables_with_keyword(
                content,
                ["large load", "mw", "data center", "in-service", "customer"]
            )

            for page_num, df in tables:
                self._log(f"PDF page {page_num}: found table with {len(df)} rows")
                sheet_projects = self._parse_large_load_sheet(
                    df, source_url, f"PDF page {page_num}"
                )
                projects.extend(sheet_projects)

            # If no tables, try text extraction
            if not projects:
                text = extract_pdf_text(content, max_pages=30)
                if text:
                    projects = self._parse_pdf_text_for_loads(text, source_url)

        except Exception as e:
            self._log(f"PJM PDF parse error: {e}")

        return projects

    def _parse_pdf_text_for_loads(self, text: str, source_url: str) -> list[Project]:
        """Best-effort parse of unstructured PDF text for load project data."""
        projects = []
        now = datetime.utcnow()

        # Look for patterns like "X MW", "X,XXX MW"
        mw_pattern = re.compile(
            r"([A-Za-z0-9\s,\.\-]+?)\s*[:\|]\s*(\d[\d,\.]*)\s*MW",
            re.IGNORECASE
        )

        for match in mw_pattern.finditer(text):
            try:
                name_raw = match.group(1).strip()
                mw_raw = match.group(2).replace(",", "")
                mw = float(mw_raw)

                if mw < 100:
                    continue
                if len(name_raw) < 3 or len(name_raw) > 100:
                    continue

                project = Project(
                    iso="PJM",
                    project_name=name_raw,
                    category=self.classify_category(name_raw),
                    status=ProjectStatus.ACTIVE,
                    mw_requested=mw,
                    mw_definition="MW extracted from PDF text",
                    source_url=source_url,
                    source_name="PJM Large Load Adjustment PDF",
                    source_iso="PJM",
                    confidence=ConfidenceLevel.LOW,
                    last_checked=now,
                    notes="Extracted from unstructured PDF text; verify manually",
                )
                projects.append(project)
            except Exception:
                continue

        return projects

    def _discover_pjm_documents(self) -> tuple[list[Project], list[dict]]:
        """Crawl PJM site to discover large load documents."""
        projects = []
        docs = []

        for page_url in LARGE_LOAD_SEARCH_PAGES[:2]:
            try:
                result = download_file(page_url, timeout=10)
                if not result.success or not result.content:
                    continue

                soup = BeautifulSoup(result.text, "html.parser")
                links = soup.find_all("a", href=True)

                for link in links:
                    href = link.get("href", "")
                    text = link.get_text().strip()

                    # Only follow PJM domain links
                    full_url = urljoin(page_url, href)
                    parsed = urlparse(full_url)
                    if parsed.netloc not in PJM_DOMAINS and parsed.netloc != "":
                        continue

                    # Look for relevant documents
                    if not any(kw in text.lower() for kw in LOAD_KEYWORDS):
                        if not any(kw in href.lower() for kw in ["large-load", "load-adjustment", "data-center"]):
                            continue

                    # Check if it's a PDF or XLSX
                    is_doc = any(href.lower().endswith(ext) for ext in [".pdf", ".xlsx", ".xls"])
                    if not is_doc and "media" not in href.lower():
                        continue

                    doc = {
                        "doc_id": f"pjm_{abs(hash(full_url)) % 1000000}",
                        "docket_id": "PJM-LARGE-LOAD",
                        "title": text or href.split("/")[-1],
                        "url": full_url,
                        "pdf_parsed": False,
                        "keywords_found": [kw for kw in LOAD_KEYWORDS if kw in text.lower() or kw in href.lower()],
                        "retrieved_at": datetime.utcnow().isoformat(),
                    }
                    docs.append(doc)

                    # Try to parse if PDF
                    if href.lower().endswith(".pdf"):
                        try:
                            pdf_result = download_file(full_url, timeout=45)
                            if pdf_result.success and pdf_result.content:
                                pdf_projects = self._parse_large_load_pdf(
                                    pdf_result.content, full_url
                                )
                                projects.extend(pdf_projects)
                                doc["pdf_parsed"] = True
                                doc["has_project_table"] = len(pdf_projects) > 0
                        except Exception as e:
                            self._log(f"Could not parse discovered PDF {full_url}: {e}")

            except Exception as e:
                self._log(f"Discovery error for {page_url}: {e}")

        return projects, docs
