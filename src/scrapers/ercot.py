"""ERCOT New Large Load (NLL) Integration scraper."""
from __future__ import annotations

import io
import json
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from src.models.project import ConfidenceLevel, Project, ProjectStatus
from src.models.scraper_run import ScraperRun, ScraperStatus
from src.scrapers.base import BaseScraper
from src.utils.downloader import download_file

logger = logging.getLogger(__name__)

# ERCOT New Large Load (NLL) status report — JSON file list API
NLL_JSON_URL = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=13051&lang=EN"
NLL_HTML_URL = "https://www.ercot.com/misapp/GetReports.do?reportTypeId=13051"
NLL_PAGE_URL = "https://www.ercot.com/services/rq/large-load-integration"

COLUMN_MAP = {
    "queue_id":     ["LLI ID", "Project ID", "ID", "Request ID", "NLL ID"],
    "project_name": ["Customer Name", "Requestor", "Entity", "Name", "Project Name"],
    "mw":           ["Load (MW)", "MW Request", "MW", "Requested MW", "Capacity (MW)",
                     "Load MW", "Peak Load (MW)"],
    "county":       ["County", "Location County"],
    "substation":   ["POI", "Substation Name", "Point of Interconnection", "Substation", "Station"],
    "utility":      ["Transmission Owner", "TO", "Zone", "Utility"],
    "in_service":   ["Requested In-Service Date", "Commercial Operation Date", "COD",
                     "In Service Date", "Proposed In-Service Date"],
    "queue_date":   ["Application Date", "Date Received", "Received Date", "Queue Date"],
    "voltage":      ["Voltage (kV)", "kV", "Voltage"],
    "status":       ["Status", "Project Status", "Request Status"],
}


def _find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols_lower = {str(c).lower().strip(): c for c in df.columns}
    for cand in candidates:
        cl = cand.lower()
        if cl in cols_lower:
            return cols_lower[cl]
        for col_lower, col_orig in cols_lower.items():
            if cl in col_lower or col_lower in cl:
                return col_orig
    return None


def _clean(row, col) -> Optional[str]:
    if not col:
        return None
    v = str(row.get(col, "")).strip()
    return None if v in ("", "nan", "None", "NaT") else v


class ERCOTScraper(BaseScraper):
    source_key = "ercot"
    source_name = "ERCOT New Large Load Integration"
    iso = "ERCOT"

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []

        content = None
        file_url = self.config.get("queue_url", "")

        if file_url:
            r = download_file(file_url, timeout=20)
            if r.success and r.content:
                content = r.content
                run.bytes_downloaded = r.bytes_downloaded or 0

        # Try JSON file list to get latest NLL report
        if not content:
            self._log(f"Fetching ERCOT NLL file list: {NLL_JSON_URL}")
            r = download_file(NLL_JSON_URL, timeout=15)
            if r.success and r.content:
                try:
                    data = json.loads(r.text or "{}")
                    # Find the most recent file URL in the JSON
                    file_url = self._extract_latest_url(data)
                    if file_url:
                        self._log(f"Latest NLL file: {file_url}")
                        r2 = download_file(file_url, timeout=20)
                        if r2.success and r2.content:
                            content = r2.content
                            run.bytes_downloaded = r2.bytes_downloaded or 0
                except Exception as e:
                    self._log(f"JSON parse error: {e}")

        # Fall back to HTML page scraping
        if not content:
            self._log(f"Fetching ERCOT NLL HTML: {NLL_HTML_URL}")
            r = download_file(NLL_HTML_URL, timeout=15)
            if r.success and r.content:
                soup = BeautifulSoup(r.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = str(a["href"])
                    if any(ext in href.lower() for ext in [".xlsx", ".csv", ".xls"]):
                        full = urljoin(NLL_HTML_URL, href)
                        r2 = download_file(full, timeout=20)
                        if r2.success and r2.content:
                            content = r2.content
                            file_url = full
                            run.bytes_downloaded = r2.bytes_downloaded or 0
                            break

        if not content:
            self._log("ERCOT NLL download failed — no file found")
            return [], self._finish_run(ScraperStatus.FAILED, "Could not download ERCOT NLL file")

        self._log(f"Downloaded {run.bytes_downloaded:,} bytes from {file_url}")

        try:
            projects = self._parse_nll_file(content, file_url or NLL_PAGE_URL)
            run.projects_found = len(projects)
            run.fields_produced = ["queue_id", "project_name", "mw_requested",
                                   "county", "substation", "in_service_date", "queue_date", "confidence"]
            self._log(f"Found {len(projects)} ERCOT large load projects >=100 MW")
            status = ScraperStatus.SUCCESS if projects else ScraperStatus.PARTIAL
        except Exception as e:
            logger.exception(f"ERCOT parse error: {e}")
            run.error_message = str(e)
            status = ScraperStatus.FAILED

        return projects, self._finish_run(status)

    def _extract_latest_url(self, data: dict) -> Optional[str]:
        """Extract the most recent file URL from ERCOT's JSON response."""
        try:
            docs = (data.get("ListDocsByRptTypeRes", {})
                       .get("SimpleDocList", {})
                       .get("SimpleDoc", []))
            if isinstance(docs, dict):
                docs = [docs]
            if docs:
                latest = docs[0]
                url = latest.get("DownloadURL") or latest.get("DocURL") or latest.get("URL")
                if url:
                    return url if url.startswith("http") else urljoin(NLL_HTML_URL, url)
        except Exception:
            pass
        return None

    def _parse_nll_file(self, content: bytes, source_url: str) -> list[Project]:
        # Try XLSX first
        try:
            return self._parse_xlsx(content, source_url)
        except Exception:
            pass
        # Try CSV
        try:
            return self._parse_csv(content, source_url)
        except Exception as e:
            raise ValueError(f"Could not parse ERCOT file as XLSX or CSV: {e}")

    def _parse_xlsx(self, content: bytes, source_url: str) -> list[Project]:
        xl = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
        self._log(f"ERCOT XLSX sheets: {xl.sheet_names}")
        for sheet in xl.sheet_names:
            try:
                df = xl.parse(sheet, header=0)
                if _find_col(df, COLUMN_MAP["mw"]) or _find_col(df, COLUMN_MAP["project_name"]):
                    return self._rows_to_projects(df, source_url)
            except Exception:
                continue
        return []

    def _parse_csv(self, content: bytes, source_url: str) -> list[Project]:
        for enc in ["utf-8", "latin-1", "cp1252"]:
            try:
                df = pd.read_csv(io.BytesIO(content), encoding=enc)
                return self._rows_to_projects(df, source_url)
            except Exception:
                continue
        return []

    def _rows_to_projects(self, df: pd.DataFrame, source_url: str) -> list[Project]:
        projects = []
        now = datetime.utcnow()

        mw_col     = _find_col(df, COLUMN_MAP["mw"])
        queue_col  = _find_col(df, COLUMN_MAP["queue_id"])
        name_col   = _find_col(df, COLUMN_MAP["project_name"])
        county_col = _find_col(df, COLUMN_MAP["county"])
        sub_col    = _find_col(df, COLUMN_MAP["substation"])
        util_col   = _find_col(df, COLUMN_MAP["utility"])
        date_col   = _find_col(df, COLUMN_MAP["in_service"])
        qdate_col  = _find_col(df, COLUMN_MAP["queue_date"])
        status_col = _find_col(df, COLUMN_MAP["status"])

        self._log(f"ERCOT cols: mw={mw_col} name={name_col} county={county_col}")
        if not mw_col:
            return projects

        for _, row in df.iterrows():
            try:
                mw = self.parse_mw(row.get(mw_col))
                if mw is None or mw < 100:
                    continue

                queue_id = _clean(row, queue_col)
                project_name = _clean(row, name_col) or f"ERCOT Load {queue_id or '?'}"
                county = _clean(row, county_col)
                substation = _clean(row, sub_col)
                utility = _clean(row, util_col)
                in_service = self.parse_date(row.get(date_col) if date_col else None)
                queue_date = self.parse_date(row.get(qdate_col) if qdate_col else None)

                status_raw = (_clean(row, status_col) or "").lower()
                if "withdraw" in status_raw or "cancel" in status_raw:
                    proj_status = ProjectStatus.WITHDRAWN
                elif "complet" in status_raw or "oper" in status_raw:
                    proj_status = ProjectStatus.COMPLETED
                else:
                    proj_status = ProjectStatus.ACTIVE

                projects.append(Project(
                    iso="ERCOT",
                    queue_id=queue_id,
                    project_name=project_name,
                    category=self.classify_category(project_name),
                    status=proj_status,
                    mw_requested=mw,
                    mw_definition="Load MW from ERCOT NLL Integration Status",
                    in_service_date=in_service,
                    queue_date=queue_date,
                    state="TX",
                    county=county,
                    substation=substation,
                    poi_text=substation,
                    utility=utility,
                    source_url=source_url,
                    source_name="ERCOT New Large Load Integration",
                    source_iso="ERCOT",
                    confidence=ConfidenceLevel.HIGH if substation else ConfidenceLevel.MEDIUM,
                    last_checked=now,
                ))
            except Exception:
                continue
        return projects
