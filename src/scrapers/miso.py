"""MISO Generator Interconnection Queue scraper."""
from __future__ import annotations

import io
import logging
import re
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

GI_QUEUE_PAGE = "https://www.misoenergy.org/planning/generator-interconnection/GI_Queue/"
# MISO sometimes has a direct API endpoint; fall back to page parsing
GI_QUEUE_API  = "https://www.misoenergy.org/api/giqueue/getprojectfile"

COLUMN_MAP = {
    "queue_id":     ["Queue#", "Queue Number", "Queue ID", "Request ID", "Queue Pos"],
    "project_name": ["Project Name", "Name", "Customer", "Applicant", "Entity"],
    "fuel_type":    ["Fuel Type", "Type", "Resource Type", "Technology", "Fuel"],
    "mw":           ["Summer Capacity (MW)", "Capacity (MW)", "MW", "Summer MW",
                     "Capacity", "Net Summer MW"],
    "state":        ["State", "Interconnection State"],
    "county":       ["County", "Location"],
    "substation":   ["Substation", "Point of Interconnection", "POI", "Station"],
    "utility":      ["Transmission Owner", "TO", "Utility", "Zone"],
    "in_service":   ["Commercial Operation Date", "In Service Date", "COD",
                     "Proposed COD", "Anticipated COD"],
    "queue_date":   ["Queue Date", "Date Entered Queue", "Application Date", "Received Date"],
    "voltage":      ["Voltage (kV)", "kV", "Voltage"],
    "status":       ["Status", "Queue Status", "Study Phase"],
}

LOAD_TYPES = {"load", "demand", "dr", "demand resource", "l", "demand response"}


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


class MISOScraper(BaseScraper):
    source_key = "miso"
    source_name = "MISO Generator Interconnection Queue"
    iso = "MISO"

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []

        xlsx_url = self.config.get("queue_url", "")
        content = None

        # 1) Try API endpoint
        if not xlsx_url:
            self._log("Trying MISO GI queue API endpoint...")
            r = download_file(GI_QUEUE_API, timeout=20)
            if r.success and r.content and b"PK" in r.content[:4]:
                content = r.content
                xlsx_url = GI_QUEUE_API
                run.bytes_downloaded = r.bytes_downloaded or 0

        # 2) Parse GI Queue page to find the XLSX link
        if not content:
            self._log(f"Fetching MISO GI Queue page: {GI_QUEUE_PAGE}")
            page = download_file(GI_QUEUE_PAGE, timeout=15)
            if page.success and page.content:
                soup = BeautifulSoup(page.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = str(a["href"])
                    if ".xlsx" in href.lower() or "giqueue" in href.lower() or "gi-queue" in href.lower():
                        full = urljoin(GI_QUEUE_PAGE, href)
                        r = download_file(full, timeout=30)
                        if r.success and r.content:
                            content = r.content
                            xlsx_url = full
                            run.bytes_downloaded = r.bytes_downloaded or 0
                            break
                if not content:
                    # also try cdn pattern
                    for a in soup.find_all("a", href=True):
                        href = str(a["href"])
                        if "cdn.misoenergy.org" in href or "GI" in href:
                            r = download_file(href, timeout=30)
                            if r.success and r.content:
                                content = r.content
                                xlsx_url = href
                                run.bytes_downloaded = r.bytes_downloaded or 0
                                break

        if not content:
            self._log("MISO GI Queue download failed")
            return [], self._finish_run(ScraperStatus.FAILED, "Could not download MISO GI Queue XLSX")

        run.content_hash = None
        self._log(f"Downloaded {run.bytes_downloaded:,} bytes from {xlsx_url}")

        try:
            projects = self._parse_queue_xlsx(content, xlsx_url)
            run.projects_found = len(projects)
            run.fields_produced = ["queue_id", "project_name", "mw_requested", "state",
                                   "county", "substation", "in_service_date", "queue_date", "confidence"]
            self._log(f"Found {len(projects)} MISO load projects >=100 MW")
            status = ScraperStatus.SUCCESS if projects else ScraperStatus.PARTIAL
        except Exception as e:
            logger.exception(f"MISO parse error: {e}")
            run.error_message = str(e)
            status = ScraperStatus.FAILED

        return projects, self._finish_run(status)

    def _parse_queue_xlsx(self, content: bytes, source_url: str) -> list[Project]:
        projects = []
        xl = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
        self._log(f"MISO XLSX sheets: {xl.sheet_names}")

        for sheet in xl.sheet_names:
            sl = sheet.lower()
            if any(kw in sl for kw in ["active", "queue", "gi", "all", "project"]):
                try:
                    df = xl.parse(sheet, header=0)
                    if len(df) > 5 and _find_col(df, COLUMN_MAP["mw"]):
                        self._log(f"Using sheet '{sheet}' ({len(df)} rows)")
                        ps = self._rows_to_projects(df, source_url)
                        if ps:
                            projects.extend(ps)
                            break
                except Exception as e:
                    self._log(f"Sheet '{sheet}' error: {e}")

        if not projects:
            for sheet in xl.sheet_names:
                try:
                    df = xl.parse(sheet, header=0)
                    if _find_col(df, COLUMN_MAP["mw"]):
                        ps = self._rows_to_projects(df, source_url)
                        if ps:
                            projects.extend(ps)
                            break
                except Exception:
                    continue
        return projects

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

        self._log(f"MISO cols: mw={mw_col} type={type_col} name={name_col} state={state_col}")
        if not mw_col:
            return projects

        for _, row in df.iterrows():
            try:
                type_val = str(row.get(type_col, "") if type_col else "").strip().lower()
                if type_col and type_val not in ("", "nan", "none"):
                    if not any(lv in type_val for lv in LOAD_TYPES):
                        continue

                mw = self.parse_mw(row.get(mw_col))
                if mw is None or mw < 100:
                    continue

                queue_id = _clean(row, queue_col)
                project_name = _clean(row, name_col) or f"MISO Load {queue_id or '?'}"
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
                    iso="MISO",
                    queue_id=queue_id,
                    project_name=project_name,
                    category=self.classify_category(project_name),
                    status=proj_status,
                    mw_requested=mw,
                    mw_definition="MW from MISO GI Queue",
                    in_service_date=in_service,
                    queue_date=queue_date,
                    state=state,
                    county=county,
                    substation=substation,
                    poi_text=substation,
                    utility=utility,
                    source_url=source_url,
                    source_name="MISO Generator Interconnection Queue",
                    source_iso="MISO",
                    confidence=ConfidenceLevel.HIGH if substation else ConfidenceLevel.MEDIUM,
                    last_checked=now,
                ))
            except Exception:
                continue
        return projects
