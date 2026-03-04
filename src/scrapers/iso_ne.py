"""ISO-NE Generator Interconnection Queue scraper."""
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

QUEUE_URL  = "https://www.iso-ne.com/static-assets/documents/100002/intercon_queue.xlsx"
QUEUE_PAGE = "https://www.iso-ne.com/interconnection/interconnection-request/interconnection-queue"

COLUMN_MAP = {
    "queue_id":     ["Queue Position", "Project Number", "Queue #", "Project No",
                     "Request Number", "Queue No"],
    "project_name": ["Project Name", "Name", "Applicant", "Entity", "Project"],
    "fuel_type":    ["Resource Type", "Fuel", "Technology", "Type", "Fuel Type"],
    "mw":           ["Capacity (MW)", "Net MW", "MW", "Summer MW", "Net Capacity (MW)",
                     "Proposed Capacity (MW)"],
    "state":        ["State"],
    "town":         ["Town", "Town/City", "Municipality", "County", "Location"],
    "substation":   ["Station", "Interconnection Substation", "Substation",
                     "Point of Interconnection", "POI"],
    "utility":      ["Transmission Owner", "TO", "Zone", "Utility"],
    "in_service":   ["Proposed In-Service Date", "COD", "Commercial Operation Date",
                     "In Service Date", "Proposed COD"],
    "queue_date":   ["Date of Application", "Queue Date", "Application Date",
                     "Received Date", "Date Received"],
    "voltage":      ["Voltage (kV)", "kV", "Voltage"],
    "status":       ["Status", "Queue Status", "Project Status"],
}

LOAD_TYPES = {"dr", "demand resource", "demand response", "load", "demand",
              "demand resource - behind the meter", "l"}


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


class ISONEScraper(BaseScraper):
    source_key = "iso_ne"
    source_name = "ISO-NE Generator Interconnection Queue"
    iso = "ISO-NE"

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []

        queue_url = self.config.get("queue_url", QUEUE_URL)
        self._log(f"Downloading ISO-NE queue XLSX from {queue_url}")

        result = download_file(queue_url, timeout=25)

        if not result.success or not result.content:
            self._log(f"Direct download failed ({result.error}), trying page discovery...")
            page = download_file(QUEUE_PAGE, timeout=15)
            if page.success and page.content:
                soup = BeautifulSoup(page.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = str(a["href"])
                    if ".xlsx" in href.lower() and "queue" in href.lower():
                        full = urljoin(QUEUE_PAGE, href)
                        result = download_file(full, timeout=25)
                        if result.success:
                            queue_url = full
                            break

        if not result.success or not result.content:
            self._log(f"ISO-NE queue download failed: {result.error}")
            return [], self._finish_run(ScraperStatus.FAILED, result.error)

        run.content_hash = result.content_hash
        run.bytes_downloaded = result.bytes_downloaded or 0
        self._log(f"Downloaded {run.bytes_downloaded:,} bytes (cache={result.from_cache})")

        try:
            projects = self._parse_queue_xlsx(result.content, queue_url)
            run.projects_found = len(projects)
            run.fields_produced = ["queue_id", "project_name", "mw_requested", "state",
                                   "county", "substation", "in_service_date", "queue_date", "confidence"]
            self._log(f"Found {len(projects)} ISO-NE demand/load projects >=100 MW")
            status = ScraperStatus.SUCCESS if projects else ScraperStatus.PARTIAL
        except Exception as e:
            logger.exception(f"ISO-NE parse error: {e}")
            run.error_message = str(e)
            status = ScraperStatus.FAILED

        return projects, self._finish_run(status)

    def _parse_queue_xlsx(self, content: bytes, source_url: str) -> list[Project]:
        xl = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
        self._log(f"ISO-NE XLSX sheets: {xl.sheet_names}")

        for sheet in xl.sheet_names:
            sl = sheet.lower()
            if any(kw in sl for kw in ["active", "queue", "all", "gen"]):
                try:
                    df = xl.parse(sheet, header=0)
                    if _find_col(df, COLUMN_MAP["mw"]):
                        ps = self._rows_to_projects(df, source_url)
                        if ps or len(df) > 50:
                            return ps
                except Exception as e:
                    self._log(f"Sheet '{sheet}' error: {e}")

        for sheet in xl.sheet_names:
            try:
                df = xl.parse(sheet, header=0)
                if _find_col(df, COLUMN_MAP["mw"]):
                    ps = self._rows_to_projects(df, source_url)
                    if ps:
                        return ps
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
        town_col   = _find_col(df, COLUMN_MAP["town"])
        sub_col    = _find_col(df, COLUMN_MAP["substation"])
        util_col   = _find_col(df, COLUMN_MAP["utility"])
        date_col   = _find_col(df, COLUMN_MAP["in_service"])
        qdate_col  = _find_col(df, COLUMN_MAP["queue_date"])
        status_col = _find_col(df, COLUMN_MAP["status"])

        self._log(f"ISO-NE cols: mw={mw_col} type={type_col} name={name_col} state={state_col}")
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
                project_name = _clean(row, name_col) or f"ISO-NE Load {queue_id or '?'}"
                state_raw = _clean(row, state_col) or ""
                state = state_raw[:2].upper() if state_raw else None
                # ISO-NE uses "town" not county; store in county field
                county = _clean(row, town_col)
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
                    iso="ISO-NE",
                    queue_id=queue_id,
                    project_name=project_name,
                    category=self.classify_category(project_name),
                    status=proj_status,
                    mw_requested=mw,
                    mw_definition="Capacity MW from ISO-NE interconnection queue",
                    in_service_date=in_service,
                    queue_date=queue_date,
                    state=state,
                    county=county,
                    substation=substation,
                    poi_text=substation,
                    utility=utility,
                    source_url=source_url,
                    source_name="ISO-NE Generator Interconnection Queue",
                    source_iso="ISO-NE",
                    confidence=ConfidenceLevel.HIGH if substation else ConfidenceLevel.MEDIUM,
                    last_checked=now,
                ))
            except Exception:
                continue
        return projects
