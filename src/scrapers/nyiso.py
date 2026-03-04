"""NYISO Interconnection Queue scraper."""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional

import pandas as pd

from src.models.project import (
    ConfidenceLevel, Project, ProjectCategory, ProjectStatus
)
from src.models.scraper_run import ScraperRun, ScraperStatus
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# NYISO column mappings (column headers may vary across versions)
# Format: {normalized_key: [possible_column_names]}
COLUMN_MAP = {
    "queue_num": ["Queue Pos.", "Queue Position", "Queue #", "Queue Number", "PTID"],
    "project_name": ["Project Name", "Applicant", "Name", "Entity Name",
                     "Developer/Interconnection Customer"],
    "type": ["Type/ Fuel", "Type", "Project Type", "Category", "Fuel"],
    "sp": ["SP (MW)", "SP", "Study Phase", "Interconnection Type"],  # value 'L'=load
    "mw_max": ["SP (MW)", "WP (MW)", "Max MW", "Summer MW", "Proposed Max MW", "Capacity (MW)", "MW"],
    "mw_winter": ["WP (MW)", "Winter MW", "Min MW"],
    "in_service_date": [
        "Proposed In-Service/Initial Backfeed Date", "Proposed In-Service Date",
        "In-Service Date", "Commercial Operation Date", "Proposed COD", "COD",
        "Proposed Sync Date",
    ],
    "county": ["County", "Town/County", "Location County"],
    "state": ["State", "Location State"],
    "utility": ["Utility", "Transmission Owner", "TO", "Affected Transmission Owner (ATO)"],
    "substation": ["Points of Interconnection", "Substation", "Point of Interconnection",
                   "POI", "Interconnection Substation"],
    "voltage": ["Voltage", "Voltage Level (kV)", "kV"],
    "transmission_line": ["Transmission Line", "Line", "T-Line", "POI Text"],
    "status": ["S", "Status", "Queue Status", "Project Status"],
    "date_entered": ["Date of IR", "Date Entered", "Application Date", "Received Date"],
}


def _find_col(df: pd.DataFrame, keys: list[str]) -> Optional[str]:
    """Find the first matching column name (case-insensitive)."""
    cols_lower = {c.lower(): c for c in df.columns}
    for key in keys:
        if key.lower() in cols_lower:
            return cols_lower[key.lower()]
        # Partial match
        for col_lower, col_orig in cols_lower.items():
            if key.lower() in col_lower:
                return col_orig
    return None


def _col(df: pd.DataFrame, key: str) -> Optional[str]:
    """Get column value from COLUMN_MAP key."""
    possible = COLUMN_MAP.get(key, [key])
    return _find_col(df, possible)


class NYISOScraper(BaseScraper):
    """Scrapes NYISO Interconnection Queue XLSX for large load projects."""

    source_key = "nyiso"
    source_name = "NYISO Interconnection Queue"
    iso = "NYISO"

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []

        url = self.config.get("url", "https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx")
        self._log(f"Downloading NYISO queue from {url}")

        result = self.download(url)
        if not result.success:
            self._log(f"Download failed: {result.error}")
            return [], self._finish_run(ScraperStatus.FAILED, result.error)

        run.content_hash = result.content_hash
        run.bytes_downloaded = result.bytes_downloaded
        self._log(f"Downloaded {result.bytes_downloaded} bytes (from_cache={result.from_cache})")

        try:
            projects = self._parse_xlsx(result.content, url)
            run.projects_found = len(projects)
            run.fields_produced = [
                "queue_id", "project_name", "mw_requested", "in_service_date",
                "state", "county", "utility", "substation", "voltage_kv", "poi_text", "status"
            ]
            self._log(f"Parsed {len(projects)} load projects ≥100 MW")
            status = ScraperStatus.SUCCESS if projects else ScraperStatus.PARTIAL
        except Exception as e:
            logger.exception(f"NYISO parsing error: {e}")
            self._log(f"Parsing error: {e}")
            status = ScraperStatus.FAILED
            run.error_message = str(e)

        return projects, self._finish_run(status)

    def _parse_xlsx(self, content: bytes, source_url: str) -> list[Project]:
        """Parse NYISO XLSX and return load projects ≥100 MW."""
        import io
        try:
            # NYISO queue often has multiple sheets; try to find the right one
            xl = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
            self._log(f"Sheets found: {xl.sheet_names}")

            target_df = None
            for sheet in xl.sheet_names:
                sheet_lower = sheet.lower()
                if any(kw in sheet_lower for kw in ["queue", "interconnection", "active", "load"]):
                    try:
                        # First try with header=0 (most NYISO files have header on row 0)
                        df = xl.parse(sheet, header=0)
                        if df is not None and len(df) > 0 and any(
                            "queue" in str(c).lower() or "project" in str(c).lower()
                            or "mw" in str(c).lower()
                            for c in df.columns
                        ):
                            target_df = df
                            self._log(f"Using sheet: {sheet} with {len(df)} rows")
                            break
                        # Fallback: try to find header row
                        df = xl.parse(sheet, header=None)
                        df = self._find_header_row(df)
                        if df is not None and len(df) > 0:
                            target_df = df
                            self._log(f"Using sheet: {sheet} with {len(df)} rows (header found)")
                            break
                    except Exception as e:
                        self._log(f"Could not parse sheet {sheet}: {e}")

            if target_df is None:
                # Try first sheet
                try:
                    df = xl.parse(xl.sheet_names[0], header=None)
                    target_df = self._find_header_row(df)
                    if target_df is not None:
                        self._log(f"Fallback: using first sheet with {len(target_df)} rows")
                except Exception as e:
                    raise ValueError(f"Could not parse any sheet: {e}")

            if target_df is None or target_df.empty:
                raise ValueError("No parseable data found in XLSX")

            return self._rows_to_projects(target_df, source_url)

        except Exception as e:
            logger.exception(f"NYISO XLSX parse error: {e}")
            raise

    def _find_header_row(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Find the header row in a DataFrame that may have metadata above it."""
        HEADER_KEYWORDS = ["queue", "project name", "status", "county", "mw", "voltage"]
        for i, row in df.iterrows():
            row_str = " ".join(str(v).lower() for v in row.values if pd.notna(v))
            matches = sum(1 for kw in HEADER_KEYWORDS if kw in row_str)
            if matches >= 2:
                # This row is the header
                new_df = df.iloc[int(str(i).split()[0]) if not isinstance(i, int) else i + 1:].copy()
                new_df.columns = df.iloc[int(str(i).split()[0]) if not isinstance(i, int) else i].values
                new_df = new_df.reset_index(drop=True)
                # Drop rows that are all NaN
                new_df = new_df.dropna(how="all")
                return new_df
        return None

    def _rows_to_projects(self, df: pd.DataFrame, source_url: str) -> list[Project]:
        """Convert DataFrame rows to Project objects, filtering for loads ≥100 MW."""
        projects = []
        now = datetime.utcnow()

        # Identify relevant columns
        type_col = _col(df, "type")
        sp_col = _col(df, "sp")
        mw_col = _col(df, "mw_max") or _col(df, "mw_winter")
        queue_col = _col(df, "queue_num")
        name_col = _col(df, "project_name")
        state_col = _col(df, "state")
        county_col = _col(df, "county")
        utility_col = _col(df, "utility")
        substation_col = _col(df, "substation")
        voltage_col = _col(df, "voltage")
        line_col = _col(df, "transmission_line")
        date_col = _col(df, "in_service_date")
        status_col = _col(df, "status")
        entered_col = _col(df, "date_entered")

        self._log(
            f"Columns mapped: mw={mw_col}, type={type_col}, sp={sp_col}, "
            f"name={name_col}, substation={substation_col}"
        )

        for _, row in df.iterrows():
            try:
                # Filter for load projects
                # NYISO uses "L" or "Load" in type/sp columns
                type_val = str(row.get(type_col, "") if type_col else "").strip()
                sp_val = str(row.get(sp_col, "") if sp_col else "").strip()

                # Include if type indicates load, or if no type column (include all)
                is_load = False
                if type_col:
                    is_load = type_val.upper() in ("L", "LOAD") or "load" in type_val.lower()
                elif sp_col:
                    is_load = sp_val.upper() in ("L", "LOAD") or "load" in sp_val.lower()
                else:
                    is_load = True  # No type filter available, include all

                if not is_load:
                    continue

                # Parse MW
                mw_raw = row.get(mw_col) if mw_col else None
                mw = self.parse_mw(mw_raw)
                if mw is None or mw < 100:
                    continue

                # Parse other fields
                queue_id = str(row.get(queue_col, "")).strip() if queue_col else None
                if queue_id in ("nan", "None", ""):
                    queue_id = None

                project_name = str(row.get(name_col, "")).strip() if name_col else None
                if project_name in ("nan", "None", ""):
                    project_name = None

                state = str(row.get(state_col, "")).strip() if state_col else "NY"
                if state in ("nan", "None", ""):
                    state = "NY"  # Default for NYISO

                county = str(row.get(county_col, "")).strip() if county_col else None
                if county in ("nan", "None", ""):
                    county = None

                utility = str(row.get(utility_col, "")).strip() if utility_col else None
                if utility in ("nan", "None", ""):
                    utility = None

                substation = str(row.get(substation_col, "")).strip() if substation_col else None
                if substation in ("nan", "None", ""):
                    substation = None

                voltage_raw = row.get(voltage_col) if voltage_col else None
                voltage_kv = self.parse_mw(voltage_raw)  # same parsing logic

                line_text = str(row.get(line_col, "")).strip() if line_col else None
                if line_text in ("nan", "None", ""):
                    line_text = None

                # POI text: combine substation + line
                poi_parts = [p for p in [substation, line_text] if p]
                poi_text = " / ".join(poi_parts) if poi_parts else None

                in_service_date = self.parse_date(row.get(date_col) if date_col else None)
                queue_date = self.parse_date(row.get(entered_col) if entered_col else None)

                # Status
                status_raw = str(row.get(status_col, "")).strip().lower() if status_col else ""
                if "withdrawn" in status_raw or "cancelled" in status_raw:
                    proj_status = ProjectStatus.WITHDRAWN
                elif "complete" in status_raw or "operational" in status_raw:
                    proj_status = ProjectStatus.COMPLETED
                elif "suspend" in status_raw:
                    proj_status = ProjectStatus.SUSPENDED
                else:
                    proj_status = ProjectStatus.ACTIVE

                # Category from name
                category = self.classify_category(project_name or "")

                # Confidence: NYISO queue rows with explicit MW + POI = high
                confidence = ConfidenceLevel.HIGH if substation else ConfidenceLevel.MEDIUM

                project = Project(
                    iso="NYISO",
                    queue_id=queue_id,
                    project_name=project_name or f"NYISO Load {queue_id or 'Unknown'}",
                    category=category,
                    status=proj_status,
                    mw_requested=mw,
                    mw_definition="nameplate/requested MW from NYISO queue",
                    in_service_date=in_service_date,
                    in_service_date_type="In-Service Date (NYISO)",
                    queue_date=queue_date,
                    state=state if len(state) <= 2 else "NY",
                    county=county,
                    utility=utility,
                    substation=substation,
                    voltage_kv=voltage_kv,
                    poi_text=poi_text,
                    source_url=source_url,
                    source_name=self.source_name,
                    source_iso="NYISO",
                    confidence=confidence,
                    last_checked=now,
                    raw_data={k: str(v) for k, v in row.items() if pd.notna(v)},
                )
                projects.append(project)

            except Exception as e:
                self._log(f"Row parse error: {e}")
                continue

        return projects
