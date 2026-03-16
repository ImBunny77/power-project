"""
Main pipeline refresh module.

Usage:
    python -m src.pipeline.refresh

This will run all scrapers, deduplicate results, update the DB, and write a changelog.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml

# Allow running as __main__
if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.models.project import Project
from src.models.scraper_run import ScraperRun, ScraperStatus
from src.pipeline.dedup import dedup_projects
from src.scrapers.nyiso import NYISOScraper
from src.scrapers.pjm import PJMScraper
from src.scrapers.caiso import CAISOScraper
from src.scrapers.spp import SPPScraper
from src.scrapers.miso import MISOScraper
from src.scrapers.ercot import ERCOTScraper
from src.scrapers.iso_ne import ISONEScraper
from src.scrapers.ferc_filings import FERCFilingsScraper
from src.scrapers.eia_860m import EIA860MScraper
from src.storage.database import Database

logger = logging.getLogger(__name__)

SCRAPER_REGISTRY = {
    "nyiso": NYISOScraper,
    "pjm": PJMScraper,
    "caiso": CAISOScraper,
    "spp": SPPScraper,
    "miso": MISOScraper,
    "ercot": ERCOTScraper,
    "iso_ne": ISONEScraper,
}


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning(f"Config file {config_path} not found, using defaults")
        return {}


def setup_logging(level: str = "INFO"):
    """Configure logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_refresh(
    config_path: str = "config.yaml",
    force_refresh: bool = False,
    sources: Optional[list[str]] = None,
    db: Optional[Database] = None,
) -> dict:
    """
    Run the full ETL refresh pipeline.

    Args:
        config_path: Path to config.yaml
        force_refresh: Force re-download even if content unchanged
        sources: Limit to specific source keys (None = all enabled)
        db: Database instance (creates new one from config if None)

    Returns:
        Summary dict with stats and run_id
    """
    run_id = str(uuid.uuid4())[:8]
    started_at = datetime.utcnow()
    logger.info(f"=== Pipeline refresh starting (run_id={run_id}) ===")

    config = load_config(config_path)
    app_cfg = config.get("app", {})
    db_path = app_cfg.get("db_path", "data/power_project.db")
    min_mw = app_cfg.get("min_mw", 100)

    if db is None:
        db = Database(db_path)

    sources_cfg = config.get("sources", {})
    ferc_dockets = config.get("ferc_dockets", [])
    dedup_cfg = config.get("dedup", {})

    summary = {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "sources_run": [],
        "total_new": 0,
        "total_updated": 0,
        "total_removed": 0,
        "total_projects": 0,
        "errors": [],
    }

    all_scraped_projects: list[dict] = []
    scraper_runs: list[dict] = []
    fallback_isos: set[str] = set()  # ISOs that used EIA-860M fallback (don't remove their existing data)

    # Get existing projects for dedup comparison
    existing_projects = db.get_projects(min_mw=0, limit=100000)
    existing_by_iso: dict[str, list[dict]] = {}
    for p in existing_projects:
        iso = p.get("iso", "UNKNOWN")
        existing_by_iso.setdefault(iso, []).append(p)

    # Run scrapers
    scraper_configs = {
        "nyiso":    sources_cfg.get("nyiso", {}),
        "pjm":      sources_cfg.get("pjm", {}),
        "caiso":    sources_cfg.get("caiso", {}),
        "spp":      sources_cfg.get("spp", {}),
        "miso":     sources_cfg.get("miso", {}),
        "ercot":    sources_cfg.get("ercot", {}),
        "iso_ne":   sources_cfg.get("iso_ne", {}),
        "ferc_filings": {"ferc_dockets": ferc_dockets},
    }

    for source_key, ScraperClass in SCRAPER_REGISTRY.items():
        # Skip if not in requested sources
        if sources and source_key not in sources:
            continue

        # Check if enabled in config
        src_cfg = sources_cfg.get(source_key, {})
        if isinstance(src_cfg, dict) and not src_cfg.get("enabled", True):
            logger.info(f"Skipping disabled source: {source_key}")
            continue

        scraper_cfg = scraper_configs.get(source_key, {})
        scraper = ScraperClass(config=scraper_cfg, db=db)

        logger.info(f"Running scraper: {source_key}")
        try:
            projects, scraper_run = scraper.run()
        except Exception as e:
            logger.exception(f"Scraper {source_key} threw exception: {e}")
            projects, scraper_run = [], ScraperRun(
                run_id=f"{run_id}_{source_key}_failed",
                source=source_key,
                status=ScraperStatus.FAILED,
                started_at=datetime.utcnow(),
                finished_at=datetime.utcnow(),
                error_message=str(e)
            )

        # Robust Fallback to EIA-860M for missing ISO data
        needs_fallback = (not projects or scraper_run.status != ScraperStatus.SUCCESS)
        if needs_fallback and source_key not in ("eia_860m", "ferc_filings"):
            logger.warning(f"[{source_key}] primary scraper returned {len(projects)} records (status: {scraper_run.status.value}). Falling back to EIA-860M...")
            try:
                from src.scrapers.eia_860m import EIA860MScraper
                fallback = EIA860MScraper(config=scraper_configs.get("eia_860m", {}), db=db)
                fallback.iso = scraper.iso
                fallback_projects, fallback_run = fallback.run()
                if fallback_projects:
                    logger.info(f"[{source_key}] Fallback successful: found {len(fallback_projects)} EIA-860M records.")
                    projects = fallback_projects
                    scraper_run = fallback_run
                    scraper_run.source = source_key
                    scraper_run.error_message = None
                    scraper_run.status = ScraperStatus.SUCCESS
                    fallback_isos.add(scraper.iso)  # Mark this ISO as fallback — preserve existing DB data
            except Exception as fe:
                logger.exception(f"Fallback {source_key} failed: {fe}")

        try:
            run_dict = scraper_run.model_dump()
            run_dict["run_id"] = f"{run_id}_{source_key}"
            scraper_runs.append(run_dict)
            db.save_scraper_run(run_dict)

            # Convert Project objects to dicts
            for p in projects:
                if isinstance(p, Project):
                    p_dict = p.model_dump()
                    # Serialize dates
                    for field in ["in_service_date", "queue_date"]:
                        if p_dict.get(field) and hasattr(p_dict[field], "isoformat"):
                            p_dict[field] = p_dict[field].isoformat()
                    all_scraped_projects.append(p_dict)
                else:
                    all_scraped_projects.append(p)

            summary["sources_run"].append({
                "source": source_key,
                "status": scraper_run.status.value,
                "projects": len(projects),
            })
            logger.info(f"  {source_key}: {len(projects)} projects, status={scraper_run.status.value}")

        except Exception as e:
            logger.exception(f"Scraper {source_key} crash during saving: {e}")
            summary["errors"].append({"source": source_key, "error": str(e)})
            summary["sources_run"].append({
                "source": source_key,
                "status": "error",
                "projects": 0,
            })

    # Filter to min_mw
    scraped_above_min = [
        p for p in all_scraped_projects
        if (float(p.get("mw_requested") or p.get("mw_in_service") or p.get("mw_adjusted") or 0) >= min_mw)
    ]
    logger.info(f"Total scraped projects ≥{min_mw} MW: {len(scraped_above_min)}")

    # Dedup and write to DB
    name_threshold = dedup_cfg.get("name_similarity_threshold", 85)
    mw_tolerance = dedup_cfg.get("mw_tolerance_pct", 10.0)

    to_insert, to_update, unchanged_ids = dedup_projects(
        scraped_above_min, existing_projects,
        name_threshold=name_threshold,
        mw_tolerance_pct=mw_tolerance,
    )

    # Insert new projects
    inserted_count = 0
    for p in to_insert:
        try:
            is_new, _ = db.upsert_project(p)
            if is_new:
                inserted_count += 1
                db.add_changelog_entry(
                    run_id=run_id,
                    change_type="new",
                    project_id=p.get("id", ""),
                    project_name=p.get("project_name", "Unknown"),
                    iso=p.get("iso", "Unknown"),
                    summary=f"New project: {p.get('project_name', 'Unknown')} | {p.get('mw_requested', '?')} MW | {p.get('iso', '?')}",
                    details={k: p.get(k) for k in ["mw_requested", "state", "in_service_date", "source_url"]},
                )
        except Exception as e:
            logger.warning(f"Insert error for {p.get('id')}: {e}")

    # Update changed projects
    updated_count = 0
    for p in to_update:
        try:
            _, is_updated = db.upsert_project(p)
            if is_updated:
                updated_count += 1
                db.add_changelog_entry(
                    run_id=run_id,
                    change_type="updated",
                    project_id=p.get("id", ""),
                    project_name=p.get("project_name", "Unknown"),
                    iso=p.get("iso", "Unknown"),
                    summary=f"Updated: {p.get('project_name', 'Unknown')} | {p.get('iso', '?')}",
                    details={k: p.get(k) for k in ["mw_requested", "state", "in_service_date", "source_url"]},
                )
        except Exception as e:
            logger.warning(f"Update error for {p.get('id')}: {e}")

    # Detect removals: projects that were in existing but NOT in new scraped results
    # Only for ISOs that were actually scraped this run AND not using EIA fallback
    scraped_isos = {p.get("iso") for p in scraped_above_min}
    scraped_ids = (
        {p.get("id") for p in to_insert if p.get("id")} |
        {p.get("id") for p in to_update if p.get("id")} |
        set(unchanged_ids)
    )

    # Safety: count scraped projects per ISO vs existing per ISO
    scraped_per_iso: dict[str, int] = {}
    for p in scraped_above_min:
        iso = p.get("iso", "")
        scraped_per_iso[iso] = scraped_per_iso.get(iso, 0) + 1

    existing_per_iso: dict[str, int] = {}
    for p in existing_projects:
        iso = p.get("iso", "")
        existing_per_iso[iso] = existing_per_iso.get(iso, 0) + 1

    # Determine which ISOs are safe to run removal on
    safe_removal_isos: set[str] = set()
    for iso in scraped_isos:
        if iso in fallback_isos:
            logger.info(f"[{iso}] Skipping removal — used EIA-860M fallback (preserving existing DB data)")
            continue
        new_count = scraped_per_iso.get(iso, 0)
        old_count = existing_per_iso.get(iso, 0)
        if old_count > 0 and new_count < old_count * 0.5:
            logger.warning(f"[{iso}] Skipping removal — scraped only {new_count} vs {old_count} existing (< 50% threshold)")
            continue
        safe_removal_isos.add(iso)

    removed_count = 0
    for existing_p in existing_projects:
        e_iso = existing_p.get("iso")
        e_id = existing_p.get("id")
        e_status = existing_p.get("status", "")

        if e_iso not in safe_removal_isos:
            continue  # Not safe to remove — either not scraped, used fallback, or below threshold
        if e_status in ("withdrawn", "completed"):
            continue  # Already handled
        if e_id not in scraped_ids:
            try:
                db.mark_projects_removed([e_id], run_id)
                removed_count += 1
                db.add_changelog_entry(
                    run_id=run_id,
                    change_type="removed",
                    project_id=e_id,
                    project_name=existing_p.get("project_name", "Unknown"),
                    iso=e_iso,
                    summary=f"No longer in queue: {existing_p.get('project_name', 'Unknown')} | {e_iso}",
                    details={},
                )
            except Exception as e:
                logger.warning(f"Remove error for {e_id}: {e}")

    # Final stats
    total_projects = db.get_project_count(min_mw=min_mw)
    finished_at = datetime.utcnow()
    duration = (finished_at - started_at).total_seconds()

    summary.update({
        "finished_at": finished_at.isoformat(),
        "duration_seconds": duration,
        "total_new": inserted_count,
        "total_updated": updated_count,
        "total_removed": removed_count,
        "total_unchanged": len(unchanged_ids),
        "total_projects": total_projects,
    })

    logger.info(
        f"=== Refresh complete (run_id={run_id}) ===\n"
        f"  New: {inserted_count}, Updated: {updated_count}, "
        f"Removed: {removed_count}, Unchanged: {len(unchanged_ids)}\n"
        f"  Total projects in DB: {total_projects}\n"
        f"  Duration: {duration:.1f}s"
    )

    # Write summary JSON artifact
    summary_path = Path("data/last_refresh_summary.json")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2))

    return summary


async def run_refresh_async(**kwargs) -> dict:
    """Async wrapper for run_refresh (runs in thread pool)."""
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: run_refresh(**kwargs))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Power Project data refresh pipeline")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--force", action="store_true", help="Force re-download all sources")
    parser.add_argument("--sources", nargs="+", help="Limit to specific sources")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()

    setup_logging(args.log_level)

    summary = run_refresh(
        config_path=args.config,
        force_refresh=args.force,
        sources=args.sources,
    )

    print(json.dumps(summary, indent=2))
    sys.exit(0 if not summary.get("errors") else 1)
