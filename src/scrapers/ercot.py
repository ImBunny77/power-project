"""ERCOT Large Load Integration scraper."""
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
from src.utils.pdf_parser import extract_pdf_tables

logger = logging.getLogger(__name__)

ERCOT_URLS = [
    "https://www.ercot.com/services/rq/large-load-integration",
    "https://www.ercot.com/gridinfo/load/load_hist",
    "https://www.ercot.com/misapp/GetReports.do?reportTypeId=13051",
]


class ERCOTScraper(BaseScraper):
    """Scrapes ERCOT large load integration page and any posted data."""

    source_key = "ercot"
    source_name = "ERCOT Large Load Integration"
    iso = "ERCOT"

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []
        docs = []

        urls = [self.config.get("url", ERCOT_URLS[0])] + ERCOT_URLS[1:]

        for url in urls:
            self._log(f"Fetching ERCOT page: {url}")
            result = download_file(url, timeout=30)
            if not result.success:
                self._log(f"Failed to fetch {url}: {result.error}")
                continue

            run.bytes_downloaded = (run.bytes_downloaded or 0) + (result.bytes_downloaded or 0)

            try:
                page_projects, page_docs = self._parse_page(result.text or "", url)
                projects.extend(page_projects)
                docs.extend(page_docs)
                self._log(f"ERCOT {url}: {len(page_projects)} projects, {len(page_docs)} docs")
            except Exception as e:
                self._log(f"ERCOT parse error {url}: {e}")

        if self.db:
            for doc in docs:
                try:
                    self.db.upsert_filing_document(doc)
                except Exception:
                    pass

        run.projects_found = len(projects)
        run.filings_found = len(docs)
        run.fields_produced = ["project_name", "mw_requested", "state", "source_url"]

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

            if "ercot.com" not in full_url:
                continue

            lower = (text + href).lower()
            is_relevant = any(kw in lower for kw in [
                "large load", "lli", "new large load", "interconnection",
                "data center", "industrial", "queue"
            ])

            if href.lower().endswith((".pdf", ".xlsx", ".xls", ".csv")) and is_relevant:
                doc = {
                    "doc_id": f"ercot_{abs(hash(full_url)) % 1000000}",
                    "docket_id": "ERCOT-LARGE-LOAD",
                    "title": text or href.split("/")[-1],
                    "url": full_url,
                    "pdf_parsed": False,
                    "has_project_table": False,
                    "keywords_found": [kw for kw in ["large load", "lli", "mw", "data center"]
                                       if kw in lower],
                    "retrieved_at": now.isoformat(),
                }
                docs.append(doc)

                try:
                    dl = download_file(full_url, timeout=45)
                    if dl.success and dl.content:
                        if href.lower().endswith(".pdf"):
                            file_projects = self._parse_pdf(dl.content, full_url)
                        elif href.lower().endswith(".csv"):
                            file_projects = self._parse_csv(dl.content, full_url)
                        else:
                            file_projects = self._parse_xlsx(dl.content, full_url)
                        projects.extend(file_projects)
                        doc["pdf_parsed"] = True
                        doc["has_project_table"] = len(file_projects) > 0
                except Exception as e:
                    self._log(f"Could not parse {full_url}: {e}")

        # Look for inline tables with MW data
        for table in soup.find_all("table"):
            table_text = table.get_text().lower()
            if "mw" in table_text and ("load" in table_text or "demand" in table_text):
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
                                     if any(kw in k for kw in ["name", "project", "customer", "entity"])), None)
                    name = str(row.get(name_col, "")).strip() if name_col else None
                    if not name or name in ("nan", "None", ""):
                        name = None
                    projects.append(Project(
                        iso="ERCOT",
                        project_name=name or f"ERCOT Large Load ({mw:.0f} MW)",
                        category=self.classify_category(name or ""),
                        status=ProjectStatus.ACTIVE,
                        mw_requested=mw,
                        state="TX",
                        source_url=source_url,
                        source_name="ERCOT Large Load Integration",
                        source_iso="ERCOT",
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
                            iso="ERCOT",
                            project_name=f"ERCOT Large Load ({mw:.0f} MW)",
                            mw_requested=mw,
                            status=ProjectStatus.ACTIVE,
                            state="TX",
                            source_url=source_url,
                            source_name="ERCOT Large Load Spreadsheet",
                            source_iso="ERCOT",
                            confidence=ConfidenceLevel.MEDIUM,
                            last_checked=now,
                        ))
        except Exception as e:
            self._log(f"ERCOT XLSX error: {e}")
        return projects

    def _parse_csv(self, content: bytes, source_url: str) -> list[Project]:
        import io
        import pandas as pd
        projects = []
        now = datetime.utcnow()
        try:
            df = pd.read_csv(io.BytesIO(content))
            cols_lower = {str(c).lower(): c for c in df.columns}
            mw_col = next((v for k, v in cols_lower.items() if "mw" in k), None)
            if mw_col:
                for _, row in df.iterrows():
                    mw = self.parse_mw(row.get(mw_col))
                    if mw and mw >= 100:
                        projects.append(Project(
                            iso="ERCOT",
                            project_name=f"ERCOT Large Load ({mw:.0f} MW)",
                            mw_requested=mw,
                            status=ProjectStatus.ACTIVE,
                            state="TX",
                            source_url=source_url,
                            source_name="ERCOT Large Load CSV",
                            source_iso="ERCOT",
                            confidence=ConfidenceLevel.MEDIUM,
                            last_checked=now,
                        ))
        except Exception as e:
            self._log(f"ERCOT CSV error: {e}")
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
                            iso="ERCOT",
                            project_name=f"ERCOT Large Load ({mw:.0f} MW)",
                            mw_requested=mw,
                            status=ProjectStatus.ACTIVE,
                            state="TX",
                            source_url=source_url,
                            source_name="ERCOT Large Loads Page",
                            source_iso="ERCOT",
                            confidence=ConfidenceLevel.LOW,
                            last_checked=now,
                        ))
        except Exception:
            pass
        return projects
