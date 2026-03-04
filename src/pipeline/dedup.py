"""Deterministic deduplication for large load projects."""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from rapidfuzz import fuzz, process
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False
    logger.warning("rapidfuzz not installed; fuzzy dedup will be disabled")


def make_dedup_key(project_dict: dict) -> str:
    """
    Create a deterministic dedup key.
    Priority: (ISO + queue_id) if available, else fuzzy fields.
    """
    iso = (project_dict.get("iso") or "").upper()
    queue_id = (project_dict.get("queue_id") or "").strip()

    if iso and queue_id:
        return f"{iso}::{queue_id}"

    # Fuzzy key from multiple fields
    name = (project_dict.get("project_name") or "").lower().strip()
    state = (project_dict.get("state") or "").upper()
    county = (project_dict.get("county") or "").lower().strip()
    poi = (project_dict.get("poi_text") or "").lower().strip()
    mw = str(round(float(project_dict.get("mw_requested") or 0), -1))  # Round to nearest 10

    return f"{iso}::{name}::{state}::{county}::{mw}"


def dedup_projects(
    new_projects: list[dict],
    existing_projects: list[dict],
    name_threshold: int = 85,
    mw_tolerance_pct: float = 10.0,
) -> tuple[list[dict], list[dict], list[str]]:
    """
    Deduplicate new projects against existing ones.

    Returns:
        (to_insert, to_update, unchanged_ids)
    """
    if not new_projects:
        return [], [], []

    to_insert = []
    to_update = []
    unchanged_ids = []

    # Build lookup maps
    existing_by_id = {p["id"]: p for p in existing_projects}
    existing_by_key: dict[str, dict] = {}
    for p in existing_projects:
        key = make_dedup_key(p)
        existing_by_key[key] = p

    # For fuzzy matching
    existing_names = []
    if HAS_RAPIDFUZZ:
        existing_names = [
            (p["id"], (p.get("project_name") or "").lower(), p)
            for p in existing_projects
            if p.get("project_name")
        ]

    seen_ids = set()

    for new_p in new_projects:
        project_id = new_p.get("id")

        # 1) Exact ID match
        if project_id and project_id in existing_by_id:
            existing = existing_by_id[project_id]
            if _projects_differ(new_p, existing):
                to_update.append(new_p)
            else:
                unchanged_ids.append(project_id)
            seen_ids.add(project_id)
            continue

        # 2) Dedup key match
        key = make_dedup_key(new_p)
        if key in existing_by_key:
            existing = existing_by_key[key]
            if _projects_differ(new_p, existing):
                # Preserve existing ID
                new_p["id"] = existing["id"]
                to_update.append(new_p)
            else:
                unchanged_ids.append(existing["id"])
            seen_ids.add(existing["id"])
            continue

        # 3) Fuzzy name + MW match (if rapidfuzz available)
        if HAS_RAPIDFUZZ and new_p.get("project_name") and existing_names:
            matched = _fuzzy_match(
                new_p, existing_names, name_threshold, mw_tolerance_pct
            )
            if matched:
                existing = matched
                if _projects_differ(new_p, existing):
                    new_p["id"] = existing["id"]
                    to_update.append(new_p)
                else:
                    unchanged_ids.append(existing["id"])
                seen_ids.add(existing["id"])
                continue

        # No match — this is a new project
        to_insert.append(new_p)

    logger.info(
        f"Dedup: {len(to_insert)} new, {len(to_update)} updated, "
        f"{len(unchanged_ids)} unchanged from {len(new_projects)} total"
    )
    return to_insert, to_update, unchanged_ids


def _fuzzy_match(
    new_p: dict,
    existing_names: list[tuple],
    name_threshold: int,
    mw_tolerance_pct: float,
) -> Optional[dict]:
    """Fuzzy match on name + MW."""
    new_name = (new_p.get("project_name") or "").lower()
    new_mw = float(new_p.get("mw_requested") or 0)
    new_state = (new_p.get("state") or "").upper()
    new_iso = (new_p.get("iso") or "").upper()

    candidates = []
    for pid, existing_name, existing_p in existing_names:
        # Must be same ISO (if both known)
        e_iso = (existing_p.get("iso") or "").upper()
        if new_iso and e_iso and new_iso != e_iso:
            continue

        # Must be same state (if both known)
        e_state = (existing_p.get("state") or "").upper()
        if new_state and e_state and new_state != e_state:
            continue

        score = fuzz.token_sort_ratio(new_name, existing_name)
        if score >= name_threshold:
            # Check MW within tolerance
            e_mw = float(existing_p.get("mw_requested") or 0)
            if new_mw > 0 and e_mw > 0:
                mw_diff_pct = abs(new_mw - e_mw) / max(new_mw, e_mw) * 100
                if mw_diff_pct > mw_tolerance_pct:
                    continue
            candidates.append((score, existing_p))

    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]
    return None


def _projects_differ(new_p: dict, existing_p: dict) -> bool:
    """Check if meaningful fields changed between new and existing project."""
    COMPARE_FIELDS = [
        "mw_requested", "mw_adjusted", "mw_in_service",
        "in_service_date", "status", "substation", "poi_text",
        "utility", "state", "county", "confidence",
    ]
    for field in COMPARE_FIELDS:
        new_val = new_p.get(field)
        existing_val = existing_p.get(field)
        if new_val is None and existing_val is None:
            continue
        if str(new_val) != str(existing_val):
            return True
    return False
