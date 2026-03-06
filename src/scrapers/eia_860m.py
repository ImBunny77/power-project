"""
EIA-860M Scraper — Planned Generators
Uses EIA Form 860M (Monthly Electric Generator Inventory) to get
proposed generators for ALL ISOs, especially PJM, SPP, and ERCOT
which block automated access to their own queue data.

Data source: https://www.eia.gov/electricity/data/eia860m/
"""
from __future__ import annotations

import io
import logging
import re
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from src.models import ConfidenceLevel, Project, ProjectStatus
from src.scrapers.base import BaseScraper, ScraperRun, ScraperStatus

logger = logging.getLogger(__name__)

# State-to-ISO mapping (primary ISO for each state)
STATE_TO_ISO = {
    # PJM states
    "DE": "PJM", "IL": "PJM", "IN": "PJM", "KY": "PJM", "MD": "PJM",
    "MI": "PJM", "NJ": "PJM", "NC": "PJM", "OH": "PJM", "PA": "PJM",
    "TN": "PJM", "VA": "PJM", "WV": "PJM", "DC": "PJM",
    # ERCOT (Texas)
    "TX": "ERCOT",
    # SPP states
    "AR": "SPP", "KS": "SPP", "LA": "SPP", "MO": "SPP", "NE": "SPP",
    "NM": "SPP", "ND": "SPP", "OK": "SPP", "SD": "SPP",
    # MISO states (overlap with PJM for some)
    "IA": "MISO", "MN": "MISO", "MT": "MISO", "WI": "MISO", "MS": "MISO",
    # NYISO
    "NY": "NYISO",
    # ISO-NE
    "CT": "ISO-NE", "MA": "ISO-NE", "ME": "ISO-NE", "NH": "ISO-NE",
    "RI": "ISO-NE", "VT": "ISO-NE",
    # CAISO
    "CA": "CAISO",
    # Non-ISO states — assign to nearest/relevant
    "AL": "SPP", "AZ": "CAISO", "CO": "SPP", "FL": "PJM", "GA": "PJM",
    "HI": "CAISO", "ID": "CAISO", "OR": "CAISO", "SC": "PJM", "UT": "CAISO",
    "WA": "CAISO", "WY": "SPP", "NV": "CAISO",
}

# ISOs we want to fill data for (only populate ISOs that don't have their own scraper working)
TARGET_ISOS = {"PJM", "SPP", "ERCOT"}

EIA_860M_URL_TEMPLATE = "https://www.eia.gov/electricity/data/eia860m/xls/{month}_generator{year}.xlsx"
EIA_860M_ARCHIVE_TEMPLATE = "https://www.eia.gov/electricity/data/eia860m/archive/xls/{month}_generator{year}.xlsx"

MONTHS = ["january", "february", "march", "april", "may", "june",
          "july", "august", "september", "october", "november", "december"]


class EIA860MScraper(BaseScraper):
    source_key = "eia_860m"
    source_name = "EIA-860M Planned Generators (PJM/SPP/ERCOT)"
    iso = "EIA"  # Multi-ISO source

    def _find_latest_url(self) -> Optional[str]:
        """Find the most recent EIA-860M file URL."""
        now = datetime.utcnow()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        
        # Try current year first, then previous year
        for year in [now.year, now.year - 1]:
            months_to_try = list(range(now.month - 1, -1, -1)) if year == now.year else list(range(11, -1, -1))
            for month_idx in months_to_try:
                month_name = MONTHS[month_idx]
                # Try current URL pattern first, then archive
                for template in [EIA_860M_URL_TEMPLATE, EIA_860M_ARCHIVE_TEMPLATE]:
                    url = template.format(month=month_name, year=year)
                    try:
                        r = requests.head(url, headers=headers, timeout=10, allow_redirects=True)
                        if r.status_code == 200:
                            return url
                    except Exception:
                        continue
        return None

    def run(self) -> tuple[list[Project], ScraperRun]:
        run = self._new_run()
        projects = []

        self._log("Finding latest EIA-860M data file...")
        url = self._find_latest_url()
        if not url:
            msg = "Could not find EIA-860M file"
            self._log(msg)
            return [], self._finish_run(ScraperStatus.PARTIAL, msg)

        self._log(f"Downloading EIA-860M from {url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        
        try:
            r = requests.get(url, headers=headers, timeout=60)
            if r.status_code != 200 or r.content[:2] != b'PK':
                msg = f"EIA-860M download failed: status {r.status_code}"
                self._log(msg)
                return [], self._finish_run(ScraperStatus.PARTIAL, msg)
            
            run.bytes_downloaded = len(r.content)
            self._log(f"Downloaded {len(r.content):,} bytes")
            
            # Parse the Planned sheet
            df = pd.read_excel(io.BytesIO(r.content), sheet_name='Planned', header=2, engine='openpyxl')
            self._log(f"Planned sheet: {len(df)} generators")
            
            # Also parse Canceled/Postponed for historical data
            try:
                df_canceled = pd.read_excel(io.BytesIO(r.content), sheet_name='Canceled or Postponed', header=2, engine='openpyxl')
                self._log(f"Canceled/Postponed sheet: {len(df_canceled)} generators")
            except Exception:
                df_canceled = pd.DataFrame()
            
            projects = self._parse_generators(df, url, ProjectStatus.ACTIVE)
            if len(df_canceled) > 0:
                projects += self._parse_generators(df_canceled, url, ProjectStatus.WITHDRAWN)
            
            run.projects_found = len(projects)
            run.fields_produced = ["queue_id", "project_name", "mw_requested", "state",
                                   "county", "in_service_date", "confidence"]
            
            self._log(f"Found {len(projects)} planned generators for {', '.join(TARGET_ISOS)}")
            status = ScraperStatus.SUCCESS if projects else ScraperStatus.PARTIAL
            
        except Exception as e:
            logger.exception(f"EIA-860M parse error: {e}")
            run.error_message = str(e)
            status = ScraperStatus.FAILED

        return projects, self._finish_run(status)

    def _parse_generators(self, df: pd.DataFrame, source_url: str, 
                          default_status: ProjectStatus) -> list[Project]:
        """Parse generators from EIA-860M dataframe."""
        projects = []
        now = datetime.utcnow()
        
        # Column name mapping (flexible to handle slight naming variations)
        def _find_col(cols, keywords):
            for c in cols:
                cl = str(c).lower()
                if all(k in cl for k in keywords):
                    return c
            return None
        
        entity_id_col = _find_col(df.columns, ['entity', 'id'])
        entity_name_col = _find_col(df.columns, ['entity', 'name'])
        plant_id_col = _find_col(df.columns, ['plant', 'id'])
        plant_name_col = _find_col(df.columns, ['plant', 'name'])
        state_col = _find_col(df.columns, ['plant', 'state']) or _find_col(df.columns, ['state'])
        county_col = _find_col(df.columns, ['county'])
        nameplate_col = _find_col(df.columns, ['nameplate', 'capacity'])
        summer_col = _find_col(df.columns, ['summer'])
        winter_col = _find_col(df.columns, ['winter'])
        tech_col = _find_col(df.columns, ['technology'])
        sector_col = _find_col(df.columns, ['sector'])
        status_col = _find_col(df.columns, ['status'])
        gen_id_col = _find_col(df.columns, ['generator', 'id'])
        lat_col = _find_col(df.columns, ['latitude'])
        lon_col = _find_col(df.columns, ['longitude'])
        operating_month_col = _find_col(df.columns, ['operating', 'month']) or _find_col(df.columns, ['planned', 'month'])
        operating_year_col = _find_col(df.columns, ['operating', 'year']) or _find_col(df.columns, ['planned', 'year'])
        
        self._log(f"EIA cols: state={state_col}, mw={nameplate_col}, plant={plant_name_col}")
        
        for _, row in df.iterrows():
            try:
                state = str(row.get(state_col, "") if state_col else "").strip().upper()[:2]
                if not state or state not in STATE_TO_ISO:
                    continue
                
                iso = STATE_TO_ISO.get(state, "")
                if iso not in TARGET_ISOS:
                    continue  # Skip states covered by working scrapers
                
                # Parse MW
                mw = None
                for mc in [nameplate_col, summer_col, winter_col]:
                    if mc:
                        try:
                            mw = float(row[mc])
                            if mw > 0:
                                break
                        except (ValueError, TypeError):
                            continue
                
                if mw is None or mw <= 0:
                    continue
                
                plant_id = str(row.get(plant_id_col, "") if plant_id_col else "").strip()
                gen_id = str(row.get(gen_id_col, "") if gen_id_col else "").strip()
                queue_id = f"EIA-{plant_id}-{gen_id}" if plant_id else None
                
                plant_name = str(row.get(plant_name_col, "") if plant_name_col else "").strip()
                entity_name = str(row.get(entity_name_col, "") if entity_name_col else "").strip()
                project_name = plant_name or entity_name or f"EIA Generator {queue_id or '?'}"
                
                county = str(row.get(county_col, "") if county_col else "").strip() or None
                tech = str(row.get(tech_col, "") if tech_col else "").strip()
                
                # Parse in-service date from operating month/year
                in_service = None
                if operating_year_col:
                    try:
                        year = int(float(row[operating_year_col]))
                        month = 1
                        if operating_month_col:
                            try:
                                month = int(float(row[operating_month_col]))
                            except (ValueError, TypeError):
                                pass
                        in_service = datetime(year, max(1, min(12, month)), 1)
                    except (ValueError, TypeError):
                        pass
                
                # Parse status
                proj_status = default_status
                if status_col:
                    status_raw = str(row.get(status_col, "")).lower()
                    if "cancel" in status_raw or "postpone" in status_raw:
                        proj_status = ProjectStatus.WITHDRAWN
                    elif "operating" in status_raw or "exist" in status_raw:
                        proj_status = ProjectStatus.COMPLETED
                
                projects.append(Project(
                    iso=iso,
                    queue_id=queue_id,
                    project_name=project_name,
                    category=self.classify_category(project_name + " " + tech),
                    status=proj_status,
                    mw_requested=mw,
                    mw_definition="Nameplate Capacity MW from EIA-860M",
                    in_service_date=in_service,
                    state=state,
                    county=county,
                    source_url=source_url,
                    source_name=f"EIA-860M Monthly Generator Inventory ({iso})",
                    source_iso=iso,
                    confidence=ConfidenceLevel.HIGH,
                    last_checked=now,
                    latitude=self._safe_float(row.get(lat_col)) if lat_col else None,
                    longitude=self._safe_float(row.get(lon_col)) if lon_col else None,
                ))
            except Exception:
                continue
        
        return projects
    
    @staticmethod
    def _safe_float(val) -> Optional[float]:
        try:
            f = float(val)
            return f if -180 <= f <= 180 else None
        except (ValueError, TypeError):
            return None
