"""MISO Large Loads program scraper."""
from __future__ import annotations

import logging
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

MISO_LARGE_LOAD_URLS = [
    "https://www.misoenergy.org/engage/committees/large-loads/",
    "https://www.misoenergy.org/planning/resource-adequacy-betterment/load-forecast/",
]

MISO_STATES = ["IL", "IN", "IA", "MI", "MN", "MO", "MT", "ND", "SD", "WI", "KY", "MS", "AR", "TX", "LA"]


class MISOScraper(BaseScraper):
    """Scrapes MISO large loads committee page and meeting materials."""

    source_key = "miso"
    source_name = "MISO Large Loads Program"
    iso = "MISO"

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []
        docs = []

        urls = [self.config.get("url", MISO_LARGE_LOAD_URLS[0])] + MISO_LARGE_LOAD_URLS[1:]

        for url in urls:
            self._log(f"Fetching MISO page: {url}")
            result = download_file(url, timeout=30)
            if not result.success:
                self._log(f"Failed: {result.error}")
                continue

            run.bytes_downloaded = (run.bytes_downloaded or 0) + (result.bytes_downloaded or 0)

            try:
                page_projects, page_docs = self._parse_page(result.text or "", url)
                projects.extend(page_projects)
                docs.extend(page_docs)
                self._log(f"MISO {url}: {len(page_projects)} projects, {len(page_docs)} docs")
            except Exception as e:
                self._log(f"MISO parse error {url}: {e}")

        if self.db:
            for doc in docs:
                try:
                    self.db.upsert_filing_document(doc)
                except Exception:
                    pass

        run.projects_found = len(projects)
        run.filings_found = len(docs)
        run.fields_produced = ["project_name", "mw_requested", "state", "confidence", "source_url"]

        status = ScraperStatus.PARTIAL if not projects else ScraperStatus.SUCCESS
        return projects, self._finish_run(status)

    def _parse_page(self, html: str, page_url: str) -> tuple[list[Project], list[dict]]:
        projects = []
        docs = []
        now = datetime.utcnow()

        soup = BeautifulSoup(html, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = link.get_text().strip()
            full_url = urljoin(page_url, href) if not href.startswith("http") else href

            if "misoenergy.org" not in full_url:
                continue

            lower = (text + href).lower()
            is_relevant = any(kw in lower for kw in [
                "large load", "data center", "load forecast", "queue",
                "market participant", "meeting material", "presentation"
            ])

            if href.lower().endswith((".pdf", ".xlsx", ".xls")) and is_relevant:
                doc = {
                    "doc_id": f"miso_{abs(hash(full_url)) % 1000000}",
                    "docket_id": "MISO-LARGE-LOADS",
                    "title": text or href.split("/")[-1],
                    "url": full_url,
                    "pdf_parsed": False,
                    "has_project_table": False,
                    "keywords_found": [kw for kw in ["large load", "data center", "mw"]
                                       if kw in lower],
                    "retrieved_at": now.isoformat(),
                }
                docs.append(doc)

                try:
                    dl = download_file(full_url, timeout=45)
                    if dl.success and dl.content:
                        if href.lower().endswith(".pdf"):
                            file_projects = self._parse_pdf(dl.content, full_url)
                        else:
                            file_projects = self._parse_xlsx(dl.content, full_url)
                        projects.extend(file_projects)
                        doc["pdf_parsed"] = True
                        doc["has_project_table"] = len(file_projects) > 0
                except Exception as e:
                    self._log(f"Could not parse {full_url}: {e}")

        # Check for inline tables
        for table in soup.find_all("table"):
            table_text = table.get_text().lower()
            if "mw" in table_text:
                inline = self._parse_html_table(table, page_url)
                projects.extend(inline)

        return projects, docs

    def _parse_pdf(self, content: bytes, source_url: str) -> list[Project]:
        projects = []
        now = datetime.utcnow()
        tables = extract_pdf_tables(content)
        for df in tables:
            cols_lower = {str(c).lower(): c for c in df.columns}
            mw_col = next((v for k, v in cols_lower.items() if "mw" in k), None)
            if not mw_col:
                continue
            for _, row in df.iterrows():
                mw = self.parse_mw(row.get(mw_col))
                if mw and mw >= 100:
                    name_col = next((v for k, v in cols_lower.items()
                                     if any(kw in k for kw in ["name", "project", "customer"])), None)
                    name = str(row.get(name_col, "")).strip() if name_col else None
                    if not name or name in ("nan", "None", ""):
                        name = None
                    state_col = next((v for k, v in cols_lower.items() if "state" in k), None)
                    state = str(row.get(state_col, "")).strip()[:2].upper() if state_col else None
                    if state and state not in MISO_STATES:
                        state = None
                    projects.append(Project(
                        iso="MISO",
                        project_name=name or f"MISO Large Load ({mw:.0f} MW)",
                        category=self.classify_category(name or ""),
                        status=ProjectStatus.ACTIVE,
                        mw_requested=mw,
                        mw_definition="MW from MISO meeting material PDF",
                        state=state,
                        source_url=source_url,
                        source_name="MISO Large Loads Program",
                        source_iso="MISO",
                        confidence=ConfidenceLevel.LOW,
                        last_checked=now,
                    ))
        return projects

    def _parse_xlsx(self, content: bytes, source_url: str) -> list[Project]:
        import io
        import pandas as pd
        projects = []
        now = datetime.utcnow()
        try:
            xl = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
            for sheet in xl.sheet_names[:3]:
                df = xl.parse(sheet)
                cols_lower = {str(c).lower(): c for c in df.columns}
                mw_col = next((v for k, v in cols_lower.items() if "mw" in k), None)
                if not mw_col:
                    continue
                for _, row in df.iterrows():
                    mw = self.parse_mw(row.get(mw_col))
                    if mw and mw >= 100:
                        projects.append(Project(
                            iso="MISO",
                            project_name=f"MISO Large Load ({mw:.0f} MW)",
                            mw_requested=mw,
                            status=ProjectStatus.ACTIVE,
                            source_url=source_url,
                            source_name="MISO Large Load Spreadsheet",
                            source_iso="MISO",
                            confidence=ConfidenceLevel.MEDIUM,
                            last_checked=now,
                        ))
        except Exception as e:
            self._log(f"MISO XLSX error: {e}")
        return projects

    def _parse_html_table(self, table, source_url: str) -> list[Project]:
        import pandas as pd
        projects = []
        now = datetime.utcnow()
        try:
            dfs = pd.read_html(str(table))
            for df in dfs:
                cols_lower = {str(c).lower(): c for c in df.columns}
                mw_col = next((v for k, v in cols_lower.items() if "mw" in k), None)
                if not mw_col:
                    continue
                for _, row in df.iterrows():
                    mw = self.parse_mw(row.get(mw_col))
                    if mw and mw >= 100:
                        projects.append(Project(
                            iso="MISO",
                            project_name=f"MISO Large Load ({mw:.0f} MW)",
                            mw_requested=mw,
                            status=ProjectStatus.ACTIVE,
                            source_url=source_url,
                            source_name="MISO Large Loads Page",
                            source_iso="MISO",
                            confidence=ConfidenceLevel.LOW,
                            last_checked=now,
                        ))
        except Exception:
            pass
        return projects
