"""Base scraper class with common utilities."""
from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from src.models.project import Project, ConfidenceLevel, ProjectCategory, ProjectStatus
from src.models.scraper_run import ScraperRun, ScraperStatus
from src.utils.downloader import download_file, DownloadResult

logger = logging.getLogger(__name__)

# Keywords that suggest data center / hyperscale load
DATA_CENTER_KEYWORDS = [
    "data center", "datacenter", "data centre", "hyperscale", "cloud",
    "aws", "amazon", "google", "microsoft", "meta", "apple", "oracle",
    "colocation", "colo", "server", "compute", "ai campus",
]

INDUSTRIAL_KEYWORDS = [
    "manufacturing", "industrial", "factory", "plant", "steel", "aluminum",
    "electrification", "process heat", "hydrogen", "electrolyzer",
    "semiconductor", "chip fab", "foundry", "battery", "gigafactory",
]

CRYPTO_KEYWORDS = [
    "bitcoin", "crypto", "mining", "blockchain", "digital currency",
]

EV_KEYWORDS = [
    "ev charging", "electric vehicle", "charging hub", "ev hub", "ev fleet",
]

HYDROGEN_KEYWORDS = [
    "hydrogen", "electrolyzer", "electrolysis", "green hydrogen",
]


class BaseScraper(ABC):
    """Abstract base class for all data source scrapers."""

    source_key: str = ""
    source_name: str = ""
    iso: str = ""

    def __init__(self, config: dict, db=None):
        self.config = config
        self.db = db
        self.logger = logging.getLogger(f"{__name__}.{self.source_key}")
        self._run: Optional[ScraperRun] = None

    def _new_run(self) -> ScraperRun:
        self._run = ScraperRun(
            run_id=str(uuid.uuid4()),
            source_key=self.source_key,
            source_name=self.source_name,
            iso=self.iso,
            started_at=datetime.utcnow(),
            url=self.config.get("url"),
        )
        return self._run

    def _finish_run(self, status: ScraperStatus, error: str = None) -> ScraperRun:
        if self._run:
            self._run.finished_at = datetime.utcnow()
            self._run.status = status
            if error:
                self._run.error_message = error
        return self._run

    def _log(self, msg: str):
        self.logger.info(msg)
        if self._run:
            self._run.log_lines.append(f"{datetime.utcnow().isoformat()}: {msg}")

    @abstractmethod
    def run(self) -> tuple[list[Project], ScraperRun]:
        """Run the scraper. Returns (projects, scraper_run)."""
        pass

    def download(self, url: str = None, force_refresh: bool = False) -> DownloadResult:
        """Download the source URL."""
        target_url = url or self.config.get("url", "")
        return download_file(target_url, force_refresh=force_refresh)

    @staticmethod
    def classify_category(text: str) -> ProjectCategory:
        """Classify project category from free text."""
        if not text:
            return ProjectCategory.UNKNOWN
        text_lower = text.lower()
        if any(kw in text_lower for kw in DATA_CENTER_KEYWORDS):
            return ProjectCategory.DATA_CENTER
        if any(kw in text_lower for kw in CRYPTO_KEYWORDS):
            return ProjectCategory.CRYPTO_MINING
        if any(kw in text_lower for kw in EV_KEYWORDS):
            return ProjectCategory.EV_CHARGING
        if any(kw in text_lower for kw in HYDROGEN_KEYWORDS):
            return ProjectCategory.HYDROGEN
        if any(kw in text_lower for kw in INDUSTRIAL_KEYWORDS):
            return ProjectCategory.INDUSTRIAL
        return ProjectCategory.UNKNOWN

    @staticmethod
    def parse_mw(value) -> Optional[float]:
        """Parse MW value from various formats."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            v = float(value)
            return v if v > 0 else None
        try:
            # Remove common non-numeric characters
            clean = str(value).replace(",", "").replace(" ", "").replace("MW", "").strip()
            v = float(clean)
            return v if v > 0 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def parse_date(value) -> Optional["date"]:
        """Parse date from various formats."""
        from datetime import date, datetime
        import re

        if value is None:
            return None
        if isinstance(value, (date, datetime)):
            return value.date() if isinstance(value, datetime) else value

        s = str(value).strip()
        if not s or s.lower() in ("nan", "none", "n/a", "", "tbd", "unknown"):
            return None

        # Try common formats
        formats = [
            "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d",
            "%B %d, %Y", "%b %d, %Y", "%d-%b-%Y", "%b-%Y",
            "%Y-%m", "%m/%Y", "%B %Y", "%b %Y",
            "%Y",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(s[:len(fmt.replace("%Y", "YYYY").replace("%m", "MM")
                                              .replace("%d", "DD").replace("%B", "MMMMMMMM")
                                              .replace("%b", "MMM"))], fmt).date()
            except Exception:
                pass

        # Regex for "Q1 2026", "1Q26", etc.
        q_match = re.match(r"[Qq]([1-4])\s*'?(\d{2,4})", s)
        if q_match:
            q, yr = int(q_match.group(1)), int(q_match.group(2))
            yr = yr + 2000 if yr < 100 else yr
            month = {1: 3, 2: 6, 3: 9, 4: 12}[q]
            return date(yr, month, 1)

        # Just a year
        yr_match = re.match(r"^\d{4}$", s)
        if yr_match:
            return date(int(s), 1, 1)

        return None
