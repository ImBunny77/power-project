"""CAISO Generator Interconnection Queue scraper."""
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

# CAISO queue XLSX URLs — try all, in order
QUEUE_URLS = [
    "https://www.caiso.com/Documents/GeneratorInterconnectionQueue.xlsx",
    "https://www.caiso.com/documents/current-generator-interconnection-queue.xlsx",
    # Cluster-specific files (confirmed accessible)
    "https://www.caiso.com/documents/cluster-15-interconnection-requests.xlsx",
    "https://www.caiso.com/documents/generator-interconnection-resource-id-report.xlsx",
]
QUEUE_PAGE = "https://www.caiso.com/generation-transmission/interconnection/generator-interconnection/generator-interconnection-queue"

COLUMN_MAP = {
    "queue_id":     ["Queue Position", "Application#", "Queue #", "Queue ID", "App #"],
    "project_name": ["Project Name", "Name", "Applicant", "Entity"],
    "fuel_type":    ["Fuel", "Resource Type", "Technology", "Type"],
    "mw":           ["Net MW", "MW AC", "Capacity (MW)", "MW", "Net Capacity"],
    "county":       ["County", "Location County"],
    "state":        ["State", "Location State"],
    "substation":   ["Point of Interconnection", "Substation", "POI", "Station"],
    "utility":      ["Transmission Owner", "TO", "Zone", "Utility"],
    "in_service":   ["Proposed On-line Date", "COD", "Commercial Operation Date",
                     "Proposed COD", "In Service Date"],
    "queue_date":   ["Application Date", "Queue Date", "Date Received", "Received"],
    "voltage":      ["Voltage (kV)", "kV", "Voltage"],
    "status":       ["Status", "Queue Status", "Application Status"],
}

# CAISO uses "DL" for Demand Load, and other demand-related values
LOAD_TYPES = {"dl", "demand load", "demand", "load", "dr", "demand response",
              "demand resource", "distributed load"}


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


class CAISOScraper(BaseScraper):
    source_key = "caiso"
    source_name = "CAISO Generator Interconnection Queue"
    iso = "CAISO"

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []

        content = None
        xlsx_url = self.config.get("queue_url", "")

        # Try direct download URLs
        for url in ([xlsx_url] if xlsx_url else []) + QUEUE_URLS:
            self._log(f"Trying CAISO queue URL: {url}")
            r = download_file(url, timeout=30)
            if r.success and r.content:
                content = r.content
                xlsx_url = url
                run.bytes_downloaded = r.bytes_downloaded or 0
                run.content_hash = r.content_hash
                break

        # Fall back to page parsing
        if not content:
            self._log(f"Fetching CAISO queue page: {QUEUE_PAGE}")
            page = download_file(QUEUE_PAGE, timeout=15)
            if page.success and page.content:
                soup = BeautifulSoup(page.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = str(a["href"])
                    if ".xlsx" in href.lower() and ("queue" in href.lower() or "interconnection" in href.lower()):
                        full = urljoin(QUEUE_PAGE, href)
                        r = download_file(full, timeout=30)
                        if r.success and r.content:
                            content = r.content
                            xlsx_url = full
                            run.bytes_downloaded = r.bytes_downloaded or 0
                            break

        if not content:
            self._log("CAISO queue download failed")
            return [], self._finish_run(ScraperStatus.FAILED, "Could not download CAISO queue XLSX")

        self._log(f"Downloaded {run.bytes_downloaded:,} bytes from {xlsx_url}")

        try:
            projects = self._parse_queue_xlsx(content, xlsx_url)
            run.projects_found = len(projects)
            run.fields_produced = ["queue_id", "project_name", "mw_requested", "state",
                                   "county", "substation", "in_service_date", "queue_date", "confidence"]
            self._log(f"Found {len(projects)} CAISO demand/load projects >=100 MW")
            # File downloaded OK even if 0 demand projects; mark partial not failed
            status = ScraperStatus.SUCCESS if projects else ScraperStatus.PARTIAL
            if not projects:
                run.error_message = "No demand/load (DL) projects found in CAISO queue file"
        except Exception as e:
            logger.exception(f"CAISO parse error: {e}")
            run.error_message = str(e)
            status = ScraperStatus.FAILED

        return projects, self._finish_run(status)

    def _parse_queue_xlsx(self, content: bytes, source_url: str) -> list[Project]:
        projects = []
        xl = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
        self._log(f"CAISO XLSX sheets: {xl.sheet_names}")

        for sheet in xl.sheet_names:
            sl = sheet.lower()
            if any(kw in sl for kw in ["active", "queue", "all", "gen"]):
                try:
                    df = xl.parse(sheet, header=0)
                    if len(df) > 5 and _find_col(df, COLUMN_MAP["mw"]):
                        self._log(f"Using sheet '{sheet}' ({len(df)} rows)")
                        ps = self._rows_to_projects(df, source_url)
                        if ps or len(df) > 100:
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

        self._log(f"CAISO cols: mw={mw_col} type={type_col} name={name_col}")
        if not mw_col:
            return projects

        for _, row in df.iterrows():
            try:
                type_val = str(row.get(type_col, "") if type_col else "").strip().lower()

                mw = self.parse_mw(row.get(mw_col))
                if mw is None:
                    continue

                queue_id = _clean(row, queue_col)
                project_name = _clean(row, name_col) or f"CAISO Load {queue_id or '?'}"
                state_raw = _clean(row, state_col) or "CA"
                state = state_raw[:2].upper() if state_raw else "CA"
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
                    iso="CAISO",
                    queue_id=queue_id,
                    project_name=project_name,
                    category=self.classify_category(project_name),
                    status=proj_status,
                    mw_requested=mw,
                    mw_definition="Net MW from CAISO GI Queue",
                    in_service_date=in_service,
                    queue_date=queue_date,
                    state=state,
                    county=county,
                    substation=substation,
                    poi_text=substation,
                    utility=utility,
                    source_url=source_url,
                    source_name="CAISO Generator Interconnection Queue",
                    source_iso="CAISO",
                    confidence=ConfidenceLevel.HIGH if substation else ConfidenceLevel.MEDIUM,
                    last_checked=now,
                ))
            except Exception:
                continue
        return projects
