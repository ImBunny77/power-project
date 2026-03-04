"""FERC docket/filings tracker and scraper."""
from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.models.project import (
    ConfidenceLevel, Project, ProjectStatus
)
from src.models.scraper_run import ScraperRun, ScraperStatus
from src.scrapers.base import BaseScraper
from src.utils.downloader import download_file
from src.utils.pdf_parser import extract_pdf_tables, extract_pdf_text, find_tables_with_keyword

logger = logging.getLogger(__name__)

FERC_SEARCH_BASE = "https://elibrary.ferc.gov/eLibrary/search"
FERC_BASE = "https://www.ferc.gov"

LARGE_LOAD_KEYWORDS = [
    "data center", "large load", "co-located load", "collocated",
    "provisional load", "HILL", "hyperscale", "interconnection queue",
    "load growth", "electrification"
]


class FERCFilingsScraper(BaseScraper):
    """Tracks FERC dockets and extracts filing documents and project data."""

    source_key = "ferc_filings"
    source_name = "FERC Filings Tracker"
    iso = "FERC"

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []
        all_docs = []

        dockets = self.config.get("ferc_dockets", [])
        if not dockets:
            # Use defaults if not passed
            dockets = [
                {
                    "id": "RM26-4",
                    "name": "Interconnection of Large Loads",
                    "url": "https://www.ferc.gov/rm26-4",
                    "enabled": True,
                },
                {
                    "id": "CHAIRMAN_LETTER",
                    "name": "Chairman Rosner Letter on Large Load Forecasting",
                    "url": "https://www.ferc.gov/news-events/news/chairman-rosners-letter-rtosisos-large-load-forecasting",
                    "enabled": True,
                }
            ]

        for docket_cfg in dockets:
            if not docket_cfg.get("enabled", True):
                continue

            docket_id = docket_cfg["id"]
            docket_url = docket_cfg["url"]
            self._log(f"Fetching FERC docket {docket_id}: {docket_url}")

            try:
                docket_projects, docket_docs = self._fetch_docket(
                    docket_id, docket_cfg["name"], docket_url,
                    docket_cfg.get("search_keywords", LARGE_LOAD_KEYWORDS)
                )
                projects.extend(docket_projects)
                all_docs.extend(docket_docs)
                self._log(f"Docket {docket_id}: {len(docket_docs)} docs, {len(docket_projects)} projects")

                # Save/update docket metadata
                if self.db:
                    self.db.upsert_ferc_docket({
                        "docket_id": docket_id,
                        "name": docket_cfg["name"],
                        "url": docket_url,
                        "last_fetched": datetime.utcnow().isoformat(),
                        "total_docs": len(docket_docs),
                        "keywords": docket_cfg.get("search_keywords", LARGE_LOAD_KEYWORDS),
                    })

            except Exception as e:
                self._log(f"Error fetching docket {docket_id}: {e}")

        # Save documents
        if self.db:
            for doc in all_docs:
                try:
                    self.db.upsert_filing_document(doc)
                except Exception as e:
                    self._log(f"Failed to save filing doc: {e}")

        run.projects_found = len(projects)
        run.filings_found = len(all_docs)
        run.fields_produced = ["project_name", "mw_requested", "confidence", "source_url"]

        status = ScraperStatus.SUCCESS if all_docs else ScraperStatus.PARTIAL
        return projects, self._finish_run(status)

    def _fetch_docket(
        self,
        docket_id: str,
        docket_name: str,
        url: str,
        keywords: list[str],
    ) -> tuple[list[Project], list[dict]]:
        """Fetch a FERC docket page and discover/parse documents."""
        projects = []
        docs = []
        now = datetime.utcnow()

        result = download_file(url, timeout=30)
        if not result.success:
            self._log(f"Failed to fetch {url}: {result.error}")
            return [], []

        html = result.text or ""
        soup = BeautifulSoup(html, "html.parser")

        # Try to find document list links
        links = soup.find_all("a", href=True)

        for link in links:
            href = link.get("href", "")
            text = link.get_text().strip()
            full_url = urljoin(url, href) if not href.startswith("http") else href

            # Must be FERC domain or eLibrary
            parsed = urlparse(full_url)
            if not any(domain in parsed.netloc for domain in ["ferc.gov", "ferc.us"]):
                continue

            lower_text = text.lower()
            lower_href = href.lower()

            # Skip navigation/non-document links
            if len(text) < 5:
                continue

            # Look for document links
            is_doc = (
                href.lower().endswith((".pdf", ".xlsx", ".xls", ".docx")) or
                "elibrary" in lower_href or
                "document" in lower_href or
                any(kw in lower_text for kw in keywords)
            )

            if not is_doc:
                continue

            doc_id = f"ferc_{docket_id}_{hashlib.md5(full_url.encode()).hexdigest()[:8]}"

            # Try to extract date from text
            filed_date = self._extract_date_from_text(text)

            # Identify filer from link context (look at nearby text)
            parent_text = ""
            if link.parent:
                parent_text = link.parent.get_text()

            doc = {
                "doc_id": doc_id,
                "docket_id": docket_id,
                "title": text[:500] if text else href.split("/")[-1],
                "filed_date": filed_date.isoformat() if filed_date else None,
                "url": full_url,
                "pdf_parsed": False,
                "has_project_table": False,
                "keywords_found": [kw for kw in keywords if kw.lower() in lower_text],
                "retrieved_at": now.isoformat(),
            }
            docs.append(doc)

            # Parse PDFs for project tables
            if href.lower().endswith(".pdf"):
                try:
                    pdf_result = download_file(full_url, timeout=45)
                    if pdf_result.success and pdf_result.content:
                        pdf_projects = self._parse_filing_pdf(
                            pdf_result.content, full_url, docket_id, keywords
                        )
                        projects.extend(pdf_projects)
                        doc["pdf_parsed"] = True
                        doc["has_project_table"] = len(pdf_projects) > 0

                        # Text snippet
                        snippet = extract_pdf_text(pdf_result.content, max_pages=1)
                        doc["extracted_text_snippet"] = snippet[:500] if snippet else None
                        # Update keywords found
                        full_text = extract_pdf_text(pdf_result.content, max_pages=5).lower()
                        doc["keywords_found"] = [kw for kw in keywords if kw.lower() in full_text]
                except Exception as e:
                    self._log(f"Could not parse PDF {full_url}: {e}")

        # Also try FERC eLibrary search for this docket
        elib_docs = self._search_elibrary(docket_id, keywords)
        for edoc in elib_docs:
            if not any(d["doc_id"] == edoc["doc_id"] for d in docs):
                docs.append(edoc)

        return projects, docs

    def _search_elibrary(self, docket_id: str, keywords: list[str]) -> list[dict]:
        """Search FERC eLibrary for docket documents."""
        docs = []
        now = datetime.utcnow()

        # FERC eLibrary search URL format
        search_url = (
            f"https://elibrary.ferc.gov/eLibrary/search?docket={docket_id}"
            f"&searchType=quick&search=&dateRange=custom"
            f"&fromDate=01/01/2024&toDate=12/31/2030"
        )

        try:
            result = download_file(search_url, timeout=20)
            if not result.success or not result.text:
                return docs

            soup = BeautifulSoup(result.text, "html.parser")

            # Parse result rows
            rows = soup.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue

                link = row.find("a", href=True)
                if not link:
                    continue

                href = link.get("href", "")
                text = link.get_text().strip()
                full_url = urljoin("https://elibrary.ferc.gov", href)

                # Extract date from first cell
                date_text = cells[0].get_text().strip() if cells else ""
                filed_date = self._extract_date_from_text(date_text)

                # Filer from second cell
                filer = cells[1].get_text().strip() if len(cells) > 1 else None

                doc_id = f"ferc_elib_{docket_id}_{hashlib.md5(full_url.encode()).hexdigest()[:8]}"

                doc = {
                    "doc_id": doc_id,
                    "docket_id": docket_id,
                    "title": text or "FERC Filing",
                    "filed_date": filed_date.isoformat() if filed_date else None,
                    "filer": filer,
                    "url": full_url,
                    "pdf_parsed": False,
                    "has_project_table": False,
                    "keywords_found": [],
                    "retrieved_at": now.isoformat(),
                }
                docs.append(doc)

        except Exception as e:
            self._log(f"eLibrary search error for {docket_id}: {e}")

        return docs

    def _parse_filing_pdf(
        self,
        content: bytes,
        source_url: str,
        docket_id: str,
        keywords: list[str],
    ) -> list[Project]:
        """Extract project data from a FERC filing PDF."""
        projects = []
        now = datetime.utcnow()

        # Look for tables with keyword matches
        tables = find_tables_with_keyword(content, ["mw", "project", "load"])
        for page_num, df in tables:
            cols_lower = {str(c).lower(): c for c in df.columns}
            mw_col = next((v for k, v in cols_lower.items() if "mw" in k), None)
            if not mw_col:
                continue

            for _, row in df.iterrows():
                mw = self.parse_mw(row.get(mw_col))
                if not mw or mw < 100:
                    continue

                name_col = next((v for k, v in cols_lower.items()
                                 if any(kw in k for kw in ["name", "project", "customer", "entity"])), None)
                name = str(row.get(name_col, "")).strip() if name_col else None
                if not name or name in ("nan", "None", ""):
                    name = None

                state_col = next((v for k, v in cols_lower.items() if "state" in k), None)
                state = None
                if state_col:
                    state_raw = str(row.get(state_col, "")).strip()
                    if len(state_raw) == 2:
                        state = state_raw.upper()

                iso_col = next((v for k, v in cols_lower.items()
                                if any(kw in k for kw in ["iso", "rto", "region"])), None)
                iso = "FERC"
                if iso_col:
                    iso_val = str(row.get(iso_col, "")).strip().upper()
                    if iso_val in ("NYISO", "PJM", "MISO", "SPP", "CAISO", "ISO-NE", "ERCOT"):
                        iso = iso_val

                projects.append(Project(
                    iso=iso,
                    project_name=name or f"FERC Filing Load Project ({mw:.0f} MW)",
                    category=self.classify_category(name or ""),
                    status=ProjectStatus.ACTIVE,
                    mw_requested=mw,
                    mw_definition="MW extracted from FERC filing PDF",
                    state=state,
                    source_url=source_url,
                    source_name=f"FERC Docket {docket_id}",
                    source_iso="FERC",
                    confidence=ConfidenceLevel.LOW,
                    last_checked=now,
                    notes=f"Extracted from FERC {docket_id} filing, PDF page {page_num}",
                ))

        return projects

    @staticmethod
    def _extract_date_from_text(text: str) -> Optional[datetime]:
        """Extract a date from text like '01/15/2025' or 'January 15, 2025'."""
        if not text:
            return None
        patterns = [
            r"(\d{1,2}/\d{1,2}/\d{4})",
            r"(\d{4}-\d{2}-\d{2})",
            r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4})",
        ]
        formats = ["%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b. %d, %Y"]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                for fmt in formats:
                    try:
                        return datetime.strptime(date_str, fmt)
                    except ValueError:
                        continue
        return None
