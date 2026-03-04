"""SPP Generator Interconnection Queue scraper."""
from __future__ import annotations

import io
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

QUEUE_PAGE = "https://www.spp.org/engineering/generation-interconnection/generator-interconnection-status-report/"
QUEUE_PORTAL = "https://opsportal.spp.org/Studies/GISR"

COLUMN_MAP = {
    "queue_id":     ["GI_ID", "Transmission Queue#", "Queue #", "Request ID", "GI ID", "Queue ID"],
    "project_name": ["Project Name", "Name", "Applicant", "Customer", "Entity"],
    "fuel_type":    ["Fuel Type", "Resource Type", "Technology", "Type", "Fuel"],
    "mw":           ["MW Requested", "Summer MW", "Capacity (MW)", "MW", "MW Request",
                     "Net MW", "Capacity MW"],
    "state":        ["State"],
    "county":       ["County", "Location"],
    "substation":   ["POI Substation", "Transmission Substation", "Substation",
                     "Point of Interconnection", "POI", "Station"],
    "utility":      ["Transmission Owner", "TO", "Zone", "Utility"],
    "in_service":   ["Proposed In-Service Date", "COD", "Commercial Operation Date",
                     "In Service Date", "Proposed COD"],
    "queue_date":   ["Application Date", "Queue Date", "Received Date", "Date Submitted"],
    "voltage":      ["Voltage (kV)", "kV", "Voltage", "POI Voltage"],
    "status":       ["Status", "Queue Status", "Project Status"],
}

LOAD_TYPES = {"load", "demand response", "demand", "dr", "demand resource", "l"}


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


class SPPScraper(BaseScraper):
    source_key = "spp"
    source_name = "SPP Generator Interconnection Queue"
    iso = "SPP"

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []

        content = None
        xlsx_url = self.config.get("queue_url", "")

        if xlsx_url:
            r = download_file(xlsx_url, timeout=25)
            if r.success and r.content:
                content = r.content
                run.bytes_downloaded = r.bytes_downloaded or 0

        # Parse the status report page to find the XLSX link
        if not content:
            self._log(f"Fetching SPP queue status report page: {QUEUE_PAGE}")
            page = download_file(QUEUE_PAGE, timeout=15)
            if page.success and page.content:
                soup = BeautifulSoup(page.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = str(a["href"])
                    if ".xlsx" in href.lower() or ".xls" in href.lower():
                        if any(kw in href.lower() for kw in ["interconnect", "queue", "gi", "gisr", "status"]):
                            full = urljoin(QUEUE_PAGE, href)
                            r = download_file(full, timeout=25)
                            if r.success and r.content:
                                content = r.content
                                xlsx_url = full
                                run.bytes_downloaded = r.bytes_downloaded or 0
                                break
                if not content:
                    # Try any XLSX link on the page
                    for a in soup.find_all("a", href=True):
                        href = str(a["href"])
                        if ".xlsx" in href.lower():
                            full = urljoin(QUEUE_PAGE, href)
                            r = download_file(full, timeout=25)
                            if r.success and r.content:
                                content = r.content
                                xlsx_url = full
                                run.bytes_downloaded = r.bytes_downloaded or 0
                                break

        # Try the OPS portal
        if not content:
            self._log(f"Trying SPP OPS portal: {QUEUE_PORTAL}")
            r = download_file(QUEUE_PORTAL, timeout=15)
            if r.success and r.content:
                soup = BeautifulSoup(r.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = str(a["href"])
                    if ".xlsx" in href.lower() or ".csv" in href.lower():
                        full = urljoin(QUEUE_PORTAL, href)
                        r2 = download_file(full, timeout=25)
                        if r2.success and r2.content:
                            content = r2.content
                            xlsx_url = full
                            run.bytes_downloaded = r2.bytes_downloaded or 0
                            break

        if not content:
            msg = "SPP queue blocks automated access (connection reset). Download manually from spp.org and upload via Sources tab."
            self._log(msg)
            return [], self._finish_run(ScraperStatus.PARTIAL, msg)

        self._log(f"Downloaded {run.bytes_downloaded:,} bytes from {xlsx_url}")

        try:
            projects = self._parse_queue_file(content, xlsx_url or QUEUE_PAGE)
            run.projects_found = len(projects)
            run.fields_produced = ["queue_id", "project_name", "mw_requested", "state",
                                   "county", "substation", "in_service_date", "queue_date", "confidence"]
            self._log(f"Found {len(projects)} SPP load projects >=100 MW")
            status = ScraperStatus.SUCCESS if projects else ScraperStatus.PARTIAL
        except Exception as e:
            logger.exception(f"SPP parse error: {e}")
            run.error_message = str(e)
            status = ScraperStatus.FAILED

        return projects, self._finish_run(status)

    def _parse_queue_file(self, content: bytes, source_url: str) -> list[Project]:
        try:
            return self._parse_xlsx(content, source_url)
        except Exception:
            pass
        try:
            return self._parse_csv(content, source_url)
        except Exception as e:
            raise ValueError(f"Could not parse SPP file: {e}")

    def _parse_xlsx(self, content: bytes, source_url: str) -> list[Project]:
        xl = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
        self._log(f"SPP XLSX sheets: {xl.sheet_names}")
        for sheet in xl.sheet_names:
            sl = sheet.lower()
            if any(kw in sl for kw in ["active", "queue", "all", "gi", "request"]):
                try:
                    df = xl.parse(sheet, header=0)
                    if _find_col(df, COLUMN_MAP["mw"]):
                        ps = self._rows_to_projects(df, source_url)
                        if ps or len(df) > 50:
                            return ps
                except Exception:
                    continue
        for sheet in xl.sheet_names:
            try:
                df = xl.parse(sheet, header=0)
                if _find_col(df, COLUMN_MAP["mw"]):
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

        type_col   = _find_col(df, COLUMN_MAP["fuel_type"])
        mw_col     = _find_col(df, COLUMN_MAP["mw"])
        queue_col  = _find_col(df, COLUMN_MAP["queue_id"])
        name_col   = _find_col(df, COLUMN_MAP["project_name"])
        state_col  = _find_col(df, COLUMN_MAP["state"])
        county_col = _find_col(df, COLUMN_MAP["county"])
        sub_col    = _find_col(df, COLUMN_MAP["substation"])
        util_col   = _find_col(df, COLUMN_MAP["utility"])
        date_col   = _find_col(df, COLUMN_MAP["in_service"])
        qdate_col  = _find_col(df, COLUMN_MAP["queue_date"])
        status_col = _find_col(df, COLUMN_MAP["status"])

        self._log(f"SPP cols: mw={mw_col} type={type_col} name={name_col} state={state_col}")
        if not mw_col:
            return projects

        for _, row in df.iterrows():
            try:
                type_val = str(row.get(type_col, "") if type_col else "").strip().lower()

                mw = self.parse_mw(row.get(mw_col))
                if mw is None or mw < 100:
                    continue

                queue_id = _clean(row, queue_col)
                project_name = _clean(row, name_col) or f"SPP Load {queue_id or '?'}"
                state_raw = _clean(row, state_col) or ""
                state = state_raw[:2].upper() if state_raw else None
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
                    iso="SPP",
                    queue_id=queue_id,
                    project_name=project_name,
                    category=self.classify_category(project_name),
                    status=proj_status,
                    mw_requested=mw,
                    mw_definition="MW from SPP GI Queue",
                    in_service_date=in_service,
                    queue_date=queue_date,
                    state=state,
                    county=county,
                    substation=substation,
                    poi_text=substation,
                    utility=utility,
                    source_url=source_url,
                    source_name="SPP Generator Interconnection Queue",
                    source_iso="SPP",
                    confidence=ConfidenceLevel.HIGH if substation else ConfidenceLevel.MEDIUM,
                    last_checked=now,
                ))
            except Exception:
                continue
        return projects
