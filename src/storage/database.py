"""SQLite storage layer for the Power Project tracker."""
from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Generator

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 3

CREATE_STATEMENTS = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    iso TEXT NOT NULL,
    queue_id TEXT,
    project_name TEXT,
    category TEXT DEFAULT 'unknown',
    status TEXT DEFAULT 'unknown',
    mw_requested REAL,
    mw_adjusted REAL,
    mw_in_service REAL,
    mw_definition TEXT,
    in_service_date TEXT,
    in_service_date_type TEXT,
    queue_date TEXT,
    state TEXT,
    county TEXT,
    city TEXT,
    latitude REAL,
    longitude REAL,
    utility TEXT,
    substation TEXT,
    voltage_kv REAL,
    poi_text TEXT,
    transmission_owner TEXT,
    source_url TEXT,
    source_name TEXT,
    source_iso TEXT,
    additional_sources TEXT DEFAULT '[]',
    confidence TEXT DEFAULT 'medium',
    field_provenance TEXT DEFAULT '{}',
    last_checked TEXT,
    first_seen TEXT,
    last_updated TEXT,
    raw_data TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS project_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL,
    changed_at TEXT NOT NULL,
    change_type TEXT NOT NULL,  -- 'created', 'updated', 'removed'
    changed_fields TEXT,        -- JSON list of changed field names
    old_values TEXT,            -- JSON dict of old values
    new_values TEXT,            -- JSON dict of new values
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS changelog (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    change_type TEXT NOT NULL,  -- 'new', 'updated', 'removed'
    project_id TEXT,
    project_name TEXT,
    iso TEXT,
    summary TEXT,
    details TEXT               -- JSON
);

CREATE TABLE IF NOT EXISTS scraper_runs (
    run_id TEXT PRIMARY KEY,
    source_key TEXT NOT NULL,
    source_name TEXT NOT NULL,
    iso TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT DEFAULT 'failed',
    projects_found INTEGER DEFAULT 0,
    projects_new INTEGER DEFAULT 0,
    projects_updated INTEGER DEFAULT 0,
    projects_removed INTEGER DEFAULT 0,
    filings_found INTEGER DEFAULT 0,
    error_message TEXT,
    url TEXT,
    content_hash TEXT,
    bytes_downloaded INTEGER,
    fields_produced TEXT DEFAULT '[]',
    log_lines TEXT DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS filing_documents (
    doc_id TEXT PRIMARY KEY,
    docket_id TEXT NOT NULL,
    title TEXT NOT NULL,
    filed_date TEXT,
    filer TEXT,
    doc_type TEXT,
    url TEXT NOT NULL,
    pdf_parsed INTEGER DEFAULT 0,
    has_project_table INTEGER DEFAULT 0,
    extracted_text_snippet TEXT,
    keywords_found TEXT DEFAULT '[]',
    retrieved_at TEXT
);

CREATE TABLE IF NOT EXISTS ferc_dockets (
    docket_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    description TEXT,
    last_fetched TEXT,
    total_docs INTEGER DEFAULT 0,
    keywords TEXT DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS geocode_cache (
    location_key TEXT PRIMARY KEY,
    latitude REAL,
    longitude REAL,
    geocoded_at TEXT,
    provider TEXT
);

CREATE INDEX IF NOT EXISTS idx_projects_iso ON projects(iso);
CREATE INDEX IF NOT EXISTS idx_projects_state ON projects(state);
CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);
CREATE INDEX IF NOT EXISTS idx_projects_mw ON projects(mw_requested);
CREATE INDEX IF NOT EXISTS idx_changelog_run ON changelog(run_id);
CREATE INDEX IF NOT EXISTS idx_scraper_runs_source ON scraper_runs(source_key);
CREATE INDEX IF NOT EXISTS idx_filing_docs_docket ON filing_documents(docket_id);
"""


class Database:
    """SQLite database wrapper for the Power Project tracker."""

    def __init__(self, db_path: str | Path = "data/power_project.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self):
        with self._conn() as conn:
            for stmt in CREATE_STATEMENTS.split(";"):
                stmt = stmt.strip()
                if stmt:
                    try:
                        conn.execute(stmt)
                    except sqlite3.OperationalError as e:
                        logger.debug(f"Schema stmt skipped: {e}")
            # Record schema version
            conn.execute(
                "INSERT OR IGNORE INTO schema_version(version, applied_at) VALUES (?, ?)",
                (SCHEMA_VERSION, datetime.utcnow().isoformat())
            )

    # ------------------------------------------------------------------ #
    # Projects                                                             #
    # ------------------------------------------------------------------ #

    def upsert_project(self, project_dict: dict) -> tuple[bool, bool]:
        """Insert or update a project. Returns (is_new, is_updated)."""
        now = datetime.utcnow().isoformat()

        with self._conn() as conn:
            existing = conn.execute(
                "SELECT * FROM projects WHERE id = ?", (project_dict["id"],)
            ).fetchone()

            # Serialize complex fields
            row = self._serialize_project(project_dict)

            if existing is None:
                row.setdefault("first_seen", now)
                row["last_updated"] = now
                cols = ", ".join(row.keys())
                placeholders = ", ".join("?" * len(row))
                conn.execute(
                    f"INSERT INTO projects ({cols}) VALUES ({placeholders})",
                    list(row.values())
                )
                return True, False
            else:
                # Detect changes
                changed_fields = []
                old_values = {}
                new_values = {}
                TRACK_FIELDS = [
                    "mw_requested", "mw_adjusted", "mw_in_service",
                    "in_service_date", "status", "substation", "poi_text",
                    "utility", "latitude", "longitude", "confidence"
                ]
                for field in TRACK_FIELDS:
                    old_val = existing[field] if existing[field] is not None else None
                    new_val = row.get(field)
                    if str(old_val) != str(new_val) and not (old_val is None and new_val is None):
                        changed_fields.append(field)
                        old_values[field] = old_val
                        new_values[field] = new_val

                if changed_fields:
                    row["last_updated"] = now
                    set_clause = ", ".join(f"{k} = ?" for k in row.keys())
                    conn.execute(
                        f"UPDATE projects SET {set_clause} WHERE id = ?",
                        list(row.values()) + [project_dict["id"]]
                    )
                    # Record history
                    conn.execute(
                        """INSERT INTO project_history
                           (project_id, changed_at, change_type, changed_fields, old_values, new_values)
                           VALUES (?, ?, 'updated', ?, ?, ?)""",
                        (
                            project_dict["id"], now,
                            json.dumps(changed_fields),
                            json.dumps(old_values),
                            json.dumps(new_values),
                        )
                    )
                    return False, True
                return False, False

    def _serialize_project(self, d: dict) -> dict:
        """Convert Python objects to SQLite-compatible types."""
        row = {}
        for k, v in d.items():
            if isinstance(v, (dict, list)):
                row[k] = json.dumps(v, default=str)
            elif isinstance(v, (datetime, date)):
                row[k] = v.isoformat()
            elif hasattr(v, "value") and not isinstance(v, (int, float, str, bool)):  # Enum
                row[k] = v.value
            elif v is None:
                row[k] = None
            else:
                row[k] = v
        return row

    def get_projects(
        self,
        iso: Optional[str] = None,
        state: Optional[str] = None,
        category: Optional[str] = None,
        status: Optional[str] = None,
        min_mw: float = 100,
        max_mw: Optional[float] = None,
        in_service_year: Optional[int] = None,
        search: Optional[str] = None,
        limit: int = 5000,
    ) -> list[dict]:
        """Query projects with optional filters."""
        conditions = ["COALESCE(mw_requested, mw_in_service, mw_adjusted, 0) >= ?"]
        params: list = [min_mw]

        if iso:
            conditions.append("iso = ?")
            params.append(iso)
        if state:
            conditions.append("state = ?")
            params.append(state.upper())
        if category:
            conditions.append("category = ?")
            params.append(category)
        if status:
            conditions.append("status = ?")
            params.append(status)
        if max_mw:
            conditions.append("COALESCE(mw_requested, mw_in_service, mw_adjusted, 0) <= ?")
            params.append(max_mw)
        if in_service_year:
            conditions.append("strftime('%Y', in_service_date) = ?")
            params.append(str(in_service_year))
        if search:
            conditions.append(
                "(LOWER(project_name) LIKE ? OR LOWER(county) LIKE ? OR LOWER(city) LIKE ? "
                "OR LOWER(poi_text) LIKE ? OR LOWER(substation) LIKE ?)"
            )
            term = f"%{search.lower()}%"
            params.extend([term, term, term, term, term])

        where = " AND ".join(conditions)
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM projects WHERE {where} ORDER BY mw_requested DESC LIMIT ?",
                params + [limit]
            ).fetchall()
        return [dict(r) for r in rows]

    def get_project_count(self, min_mw: float = 100) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM projects WHERE COALESCE(mw_requested, mw_in_service, mw_adjusted, 0) >= ?",
                (min_mw,)
            ).fetchone()
        return row[0] if row else 0

    def get_mw_by_iso(self, min_mw: float = 100) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT iso,
                   SUM(COALESCE(mw_in_service, mw_adjusted, mw_requested, 0)) as total_mw,
                   COUNT(*) as count
                   FROM projects
                   WHERE COALESCE(mw_requested, mw_in_service, mw_adjusted, 0) >= ?
                   GROUP BY iso ORDER BY total_mw DESC""",
                (min_mw,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_mw_by_state(self, min_mw: float = 100) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT state,
                   SUM(COALESCE(mw_in_service, mw_adjusted, mw_requested, 0)) as total_mw,
                   COUNT(*) as count
                   FROM projects
                   WHERE COALESCE(mw_requested, mw_in_service, mw_adjusted, 0) >= ?
                   AND state IS NOT NULL AND state != ''
                   GROUP BY state ORDER BY total_mw DESC""",
                (min_mw,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_mw_by_year(self, min_mw: float = 100) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT strftime('%Y', in_service_date) as year,
                   SUM(COALESCE(mw_in_service, mw_adjusted, mw_requested, 0)) as total_mw,
                   COUNT(*) as count
                   FROM projects
                   WHERE COALESCE(mw_requested, mw_in_service, mw_adjusted, 0) >= ?
                   AND in_service_date IS NOT NULL AND in_service_date != ''
                   GROUP BY year ORDER BY year ASC""",
                (min_mw,)
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_projects_removed(self, project_ids: list[str], run_id: str):
        """Mark projects as removed that weren't seen in latest refresh."""
        now = datetime.utcnow().isoformat()
        with self._conn() as conn:
            for pid in project_ids:
                conn.execute(
                    "UPDATE projects SET status = 'withdrawn', last_updated = ? WHERE id = ? AND status != 'withdrawn'",
                    (now, pid)
                )
                conn.execute(
                    """INSERT INTO project_history (project_id, changed_at, change_type, changed_fields, old_values, new_values)
                       VALUES (?, ?, 'removed', '["status"]', '{"status": "active"}', '{"status": "withdrawn"}')""",
                    (pid, now)
                )

    # ------------------------------------------------------------------ #
    # Changelog                                                            #
    # ------------------------------------------------------------------ #

    def add_changelog_entry(self, run_id: str, change_type: str, project_id: str,
                             project_name: str, iso: str, summary: str, details: dict = None):
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO changelog (run_id, recorded_at, change_type, project_id, project_name, iso, summary, details)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, datetime.utcnow().isoformat(), change_type,
                 project_id, project_name, iso, summary, json.dumps(details or {}))
            )

    def get_changelog(self, limit: int = 200, run_id: Optional[str] = None) -> list[dict]:
        with self._conn() as conn:
            if run_id:
                rows = conn.execute(
                    "SELECT * FROM changelog WHERE run_id = ? ORDER BY id DESC LIMIT ?",
                    (run_id, limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM changelog ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # Scraper Runs                                                         #
    # ------------------------------------------------------------------ #

    def save_scraper_run(self, run: dict):
        row = {}
        for k, v in run.items():
            if isinstance(v, list):
                row[k] = json.dumps(v)
            elif isinstance(v, datetime):
                row[k] = v.isoformat()
            elif hasattr(v, "value"):
                row[k] = v.value
            else:
                row[k] = v
        with self._conn() as conn:
            cols = ", ".join(row.keys())
            placeholders = ", ".join("?" * len(row))
            conn.execute(
                f"INSERT OR REPLACE INTO scraper_runs ({cols}) VALUES ({placeholders})",
                list(row.values())
            )

    def get_scraper_runs(self, limit: int = 50, source_key: Optional[str] = None) -> list[dict]:
        with self._conn() as conn:
            if source_key:
                rows = conn.execute(
                    "SELECT * FROM scraper_runs WHERE source_key = ? ORDER BY started_at DESC LIMIT ?",
                    (source_key, limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM scraper_runs ORDER BY started_at DESC LIMIT ?", (limit,)
                ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            for f in ["fields_produced", "log_lines"]:
                try:
                    d[f] = json.loads(d.get(f) or "[]")
                except Exception:
                    d[f] = []
            result.append(d)
        return result

    def get_latest_scraper_run_per_source(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT sr.* FROM scraper_runs sr
                   INNER JOIN (
                       SELECT source_key, MAX(started_at) as max_started
                       FROM scraper_runs GROUP BY source_key
                   ) latest ON sr.source_key = latest.source_key AND sr.started_at = latest.max_started
                   ORDER BY sr.source_key"""
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # Filing Documents                                                     #
    # ------------------------------------------------------------------ #

    def upsert_filing_document(self, doc: dict):
        row = {}
        for k, v in doc.items():
            if isinstance(v, list):
                row[k] = json.dumps(v)
            elif isinstance(v, datetime):
                row[k] = v.isoformat()
            else:
                row[k] = v
        with self._conn() as conn:
            cols = ", ".join(row.keys())
            placeholders = ", ".join("?" * len(row))
            conn.execute(
                f"INSERT OR REPLACE INTO filing_documents ({cols}) VALUES ({placeholders})",
                list(row.values())
            )

    def get_filing_documents(
        self,
        docket_id: Optional[str] = None,
        keyword: Optional[str] = None,
        limit: int = 200
    ) -> list[dict]:
        conditions = []
        params: list = []
        if docket_id:
            conditions.append("docket_id = ?")
            params.append(docket_id)
        if keyword:
            conditions.append(
                "(LOWER(title) LIKE ? OR LOWER(extracted_text_snippet) LIKE ? OR LOWER(keywords_found) LIKE ?)"
            )
            term = f"%{keyword.lower()}%"
            params.extend([term, term, term])

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM filing_documents{where} ORDER BY filed_date DESC LIMIT ?",
                params + [limit]
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["keywords_found"] = json.loads(d.get("keywords_found") or "[]")
            except Exception:
                d["keywords_found"] = []
            result.append(d)
        return result

    def upsert_ferc_docket(self, docket: dict):
        row = {}
        for k, v in docket.items():
            if isinstance(v, list):
                row[k] = json.dumps(v)
            elif isinstance(v, datetime):
                row[k] = v.isoformat()
            elif k == "documents":
                continue  # stored separately
            else:
                row[k] = v
        with self._conn() as conn:
            cols = ", ".join(row.keys())
            placeholders = ", ".join("?" * len(row))
            conn.execute(
                f"INSERT OR REPLACE INTO ferc_dockets ({cols}) VALUES ({placeholders})",
                list(row.values())
            )

    def get_ferc_dockets(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM ferc_dockets ORDER BY docket_id").fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # Geocoding cache                                                      #
    # ------------------------------------------------------------------ #

    def get_geocode(self, location_key: str) -> Optional[tuple[float, float]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT latitude, longitude FROM geocode_cache WHERE location_key = ?",
                (location_key,)
            ).fetchone()
        if row and row["latitude"] is not None:
            return (row["latitude"], row["longitude"])
        return None

    def save_geocode(self, location_key: str, lat: float, lon: float, provider: str = "nominatim"):
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO geocode_cache (location_key, latitude, longitude, geocoded_at, provider)
                   VALUES (?, ?, ?, ?, ?)""",
                (location_key, lat, lon, datetime.utcnow().isoformat(), provider)
            )

    def get_summary_stats(self, min_mw: float = 100) -> dict:
        """Get high-level summary statistics."""
        with self._conn() as conn:
            total = conn.execute(
                "SELECT COUNT(*), SUM(COALESCE(mw_in_service, mw_adjusted, mw_requested, 0)) FROM projects WHERE COALESCE(mw_requested, mw_in_service, mw_adjusted, 0) >= ?",
                (min_mw,)
            ).fetchone()
            latest_run = conn.execute(
                "SELECT MAX(started_at) FROM scraper_runs WHERE status IN ('success', 'partial')"
            ).fetchone()
            changelog_recent = conn.execute(
                """SELECT COUNT(*) FROM changelog cl
                   JOIN scraper_runs sr ON cl.run_id = sr.run_id
                   WHERE sr.started_at >= datetime('now', '-1 day')"""
            ).fetchone()

        return {
            "total_projects": total[0] or 0,
            "total_mw": round(total[1] or 0, 1),
            "last_refresh": latest_run[0] if latest_run else None,
            "changes_last_24h": changelog_recent[0] or 0,
        }
