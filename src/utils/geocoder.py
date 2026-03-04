"""Geocoding utility with Nominatim support and local cache."""
from __future__ import annotations

import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

_RATE_LIMIT_DELAY = 1.1  # Nominatim requires 1 request/second


def geocode_location(
    location_str: str,
    db=None,  # Database instance for caching
    provider: str = "nominatim",
) -> Optional[tuple[float, float]]:
    """
    Geocode a location string to (lat, lon).
    Uses local DB cache to avoid repeated requests.
    Returns None if geocoding fails or is disabled.
    """
    if not location_str or location_str.strip() == "":
        return None

    location_key = location_str.strip().lower()

    # Check cache
    if db is not None:
        cached = db.get_geocode(location_key)
        if cached:
            return cached

    # Nominatim (free, requires User-Agent, rate limited)
    if provider == "nominatim":
        result = _geocode_nominatim(location_str)
        if result and db is not None:
            db.save_geocode(location_key, result[0], result[1], provider="nominatim")
        return result

    return None


def _geocode_nominatim(location_str: str) -> Optional[tuple[float, float]]:
    """Geocode using Nominatim (OpenStreetMap)."""
    try:
        import requests
        headers = {
            "User-Agent": "PowerProjectBot/1.0 (electricity demand tracker)"
        }
        # Add ", USA" if no country specified
        query = location_str
        if "," not in query or len(query.split(",")) < 2:
            query = f"{query}, USA"

        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "limit": 1, "countrycodes": "us"},
            headers=headers,
            timeout=10,
        )
        time.sleep(_RATE_LIMIT_DELAY)  # Rate limit

        if resp.status_code == 200:
            results = resp.json()
            if results:
                return (float(results[0]["lat"]), float(results[0]["lon"]))
    except Exception as e:
        logger.debug(f"Nominatim geocoding failed for '{location_str}': {e}")
    return None


# US state name to abbreviation mapping
STATE_ABBREV = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}

STATE_ABBREV_REVERSE = {v: k.title() for k, v in STATE_ABBREV.items()}


def normalize_state(state_str: Optional[str]) -> Optional[str]:
    """Convert state name or abbreviation to 2-letter code."""
    if not state_str:
        return None
    s = state_str.strip()
    if len(s) == 2:
        return s.upper()
    lower = s.lower()
    return STATE_ABBREV.get(lower)


def parse_location_from_text(text: str) -> dict:
    """
    Best-effort extract state/county from free text.
    Returns dict with 'state', 'county', 'city' keys (all may be None).
    """
    result = {"state": None, "county": None, "city": None}
    if not text:
        return result

    # Check for state abbreviations
    import re
    # Look for comma+space+2letters pattern (e.g., "Albany, NY")
    state_pattern = re.compile(r',\s*([A-Z]{2})\b')
    match = state_pattern.search(text)
    if match:
        abbrev = match.group(1).upper()
        if abbrev in STATE_ABBREV_REVERSE:
            result["state"] = abbrev

    # Check for full state names
    if result["state"] is None:
        lower_text = text.lower()
        for name, abbrev in STATE_ABBREV.items():
            if name in lower_text:
                result["state"] = abbrev
                break

    return result
