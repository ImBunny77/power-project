"""CAISO Large Loads initiative scraper."""
from __future__ import annotations

import logging
import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.models.project import (
    ConfidenceLevel, Project, ProjectCategory, ProjectStatus
)
from src.models.scraper_run import ScraperRun, ScraperStatus
from src.scrapers.base import BaseScraper
from src.utils.downloader import download_file
from src.utils.pdf_parser import extract_pdf_tables, extract_pdf_text

logger = logging.getLogger(__name__)

CAISO_BASE = "https://www.caiso.com"
CAISO_LARGE_LOAD_URL = "https://www.caiso.com/generation-transmission/load/large-load"


class CAISOScraper(BaseScraper):
    """Scrapes CAISO Large Loads initiative page and linked PDFs."""

    source_key = "caiso"
    source_name = "CAISO Large Loads Initiative"
    iso = "CAISO"

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []
        docs = []

        url = self.config.get("url", CAISO_LARGE_LOAD_URL)
        self._log(f"Fetching CAISO Large Loads page: {url}")

        result = download_file(url, timeout=30)
        if not result.success:
            self._log(f"Download failed: {result.error}")
            return [], self._finish_run(ScraperStatus.FAILED, result.error)

        run.content_hash = result.content_hash
        run.bytes_downloaded = result.bytes_downloaded or 0

        try:
            page_projects, page_docs = self._parse_page(result.text or "", url)
            projects.extend(page_projects)
            docs.extend(page_docs)

            # Save docs to DB
            if self.db:
                for doc in docs:
                    try:
                        self.db.upsert_filing_document(doc)
                    except Exception as e:
                        self._log(f"Failed to save doc: {e}")

            run.projects_found = len(projects)
            run.filings_found = len(docs)
            run.fields_produced = ["project_name", "mw_requested", "state", "confidence", "source_url"]
            self._log(f"CAISO: {len(projects)} projects, {len(docs)} documents indexed")

        except Exception as e:
            logger.exception(f"CAISO parse error: {e}")
            run.error_message = str(e)
            return projects, self._finish_run(ScraperStatus.PARTIAL, str(e))

        return projects, self._finish_run(ScraperStatus.SUCCESS)

    def _parse_page(self, html: str, page_url: str) -> tuple[list[Project], list[dict]]:
        """Parse CAISO large loads page and follow PDF links."""
        projects = []
        docs = []
        now = datetime.utcnow()

        soup = BeautifulSoup(html, "html.parser")

        # Collect all links on page
        links = soup.find_all("a", href=True)
        for link in links:
            href = link.get("href", "")
            text = link.get_text().strip()
            full_url = urljoin(page_url, href) if not href.startswith("http") else href

            # Only follow CAISO domain
            if "caiso.com" not in full_url:
                continue

            lower_text = text.lower()
            lower_href = href.lower()
            is_relevant = any(kw in lower_text or kw in lower_href for kw in [
                "large load", "issue paper", "stakeholder", "initiative",
                "data center", "load study"
            ])

            if href.lower().endswith(".pdf") and (is_relevant or "large" in lower_href):
                doc = {
                    "doc_id": f"caiso_{abs(hash(full_url)) % 1000000}",
                    "docket_id": "CAISO-LARGE-LOADS",
                    "title": text or href.split("/")[-1],
                    "url": full_url,
                    "pdf_parsed": False,
                    "has_project_table": False,
                    "keywords_found": [],
                    "retrieved_at": now.isoformat(),
                }
                docs.append(doc)

                # Try to extract project data from PDF
                try:
                    pdf_result = download_file(full_url, timeout=45)
                    if pdf_result.success and pdf_result.content:
                        pdf_projects = self._parse_pdf(pdf_result.content, full_url)
                        projects.extend(pdf_projects)
                        doc["pdf_parsed"] = True
                        doc["has_project_table"] = len(pdf_projects) > 0
                        # Get text snippet
                        text_snippet = extract_pdf_text(pdf_result.content, max_pages=2)
                        doc["extracted_text_snippet"] = text_snippet[:500] if text_snippet else None
                        kw_found = [kw for kw in ["large load", "mw", "data center", "in-service"]
                                    if kw in (text_snippet or "").lower()]
                        doc["keywords_found"] = kw_found
                except Exception as e:
                    self._log(f"PDF parse error for {full_url}: {e}")

        # Also look for inline tables on the page
        tables = soup.find_all("table")
        for table in tables:
            table_text = table.get_text().lower()
            if any(kw in table_text for kw in ["mw", "load", "data center"]):
                inline_projects = self._parse_html_table(table, page_url)
                projects.extend(inline_projects)

        return projects, docs

    def _parse_pdf(self, content: bytes, source_url: str) -> list[Project]:
        """Extract project data from CAISO PDF."""
        projects = []
        now = datetime.utcnow()

        tables = extract_pdf_tables(content)
        for df in tables:
            # Look for MW column
            cols_lower = {str(c).lower(): str(c) for c in df.columns}
            mw_col = None
            for col_lower, col_orig in cols_lower.items():
                if "mw" in col_lower:
                    mw_col = col_orig
                    break

            if not mw_col:
                continue

            for _, row in df.iterrows():
                try:
                    mw = self.parse_mw(row.get(mw_col))
                    if mw is None or mw < 100:
                        continue

                    # Try to find name/description column
                    name = None
                    for col_lower, col_orig in cols_lower.items():
                        if any(kw in col_lower for kw in ["name", "project", "customer", "entity"]):
                            val = str(row.get(col_orig, "")).strip()
                            if val and val not in ("nan", "None", ""):
                                name = val
                                break

                    project = Project(
                        iso="CAISO",
                        project_name=name or f"CAISO Large Load",
                        category=self.classify_category(name or ""),
                        status=ProjectStatus.ACTIVE,
                        mw_requested=mw,
                        mw_definition="MW from CAISO large loads PDF",
                        state="CA",
                        source_url=source_url,
                        source_name="CAISO Large Loads Initiative PDF",
                        source_iso="CAISO",
                        confidence=ConfidenceLevel.LOW,
                        last_checked=now,
                        notes="Extracted from CAISO large loads initiative PDF",
                    )
                    projects.append(project)
                except Exception:
                    continue

        return projects

    def _parse_html_table(self, table, source_url: str) -> list[Project]:
        """Parse inline HTML table for project data."""
        import pandas as pd
        projects = []
        now = datetime.utcnow()

        try:
            dfs = pd.read_html(str(table))
            for df in dfs:
                cols_lower = {str(c).lower(): str(c) for c in df.columns}
                mw_col = next((v for k, v in cols_lower.items() if "mw" in k), None)
                if not mw_col:
                    continue
                for _, row in df.iterrows():
                    mw = self.parse_mw(row.get(mw_col))
                    if mw and mw >= 100:
                        project = Project(
                            iso="CAISO",
                            project_name=f"CAISO Large Load ({mw:.0f} MW)",
                            category=ProjectCategory.UNKNOWN,
                            status=ProjectStatus.ACTIVE,
                            mw_requested=mw,
                            state="CA",
                            source_url=source_url,
                            source_name="CAISO Large Loads Page",
                            source_iso="CAISO",
                            confidence=ConfidenceLevel.LOW,
                            last_checked=now,
                        )
                        projects.append(project)
        except Exception:
            pass
        return projects
