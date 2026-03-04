"""Geocoding utility with static county centroids + Nominatim fallback."""
from __future__ import annotations

import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Static county centroid lookup  (state -> {county_lower: (lat, lon)})
# Covers all NY counties (NYISO) plus key counties in PJM/MISO/SPP/ERCOT states.
# ---------------------------------------------------------------------------
COUNTY_CENTROIDS: dict[str, dict[str, tuple[float, float]]] = {
    "NY": {
        "albany": (42.6526, -73.7562), "allegany": (42.2679, -78.0289),
        "bronx": (40.8448, -73.8648), "broome": (42.1648, -75.8180),
        "cattaraugus": (42.2479, -78.6764), "cayuga": (42.9135, -76.5654),
        "chautauqua": (42.2801, -79.3950), "chemung": (42.1465, -76.7613),
        "chenango": (42.4944, -75.6071), "clinton": (44.7451, -73.6726),
        "columbia": (42.2487, -73.6294), "cortland": (42.5954, -76.0726),
        "delaware": (42.1999, -74.9741), "dutchess": (41.7800, -73.7479),
        "erie": (42.7584, -78.8456), "essex": (44.1156, -73.7543),
        "franklin": (44.5961, -74.3069), "fulton": (43.1255, -74.4171),
        "genesee": (43.0014, -78.1944), "greene": (42.2726, -74.2127),
        "hamilton": (43.6721, -74.5210), "herkimer": (43.4378, -74.9748),
        "jefferson": (44.0281, -75.9826), "kings": (40.6501, -73.9496),
        "brooklyn": (40.6501, -73.9496), "lewis": (43.7917, -75.4535),
        "livingston": (42.6040, -77.7716), "madison": (42.9126, -75.6677),
        "manhattan": (40.7831, -73.9712), "new york": (40.7831, -73.9712),
        "monroe": (43.1566, -77.6088), "montgomery": (42.9023, -74.4418),
        "nassau": (40.7282, -73.5900), "niagara": (43.2145, -78.6904),
        "oneida": (43.2437, -75.4271), "onondaga": (43.0117, -76.1481),
        "ontario": (42.8620, -77.2942), "orange": (41.3912, -74.3054),
        "orleans": (43.2625, -78.1769), "oswego": (43.4606, -76.2085),
        "otsego": (42.6346, -74.9218), "putnam": (41.4351, -73.7949),
        "queens": (40.7282, -73.7949), "rensselaer": (42.7131, -73.5116),
        "richmond": (40.5795, -74.1502), "staten island": (40.5795, -74.1502),
        "rockland": (41.1498, -74.0366), "saratoga": (43.1097, -73.8710),
        "schenectady": (42.8142, -73.9396), "schoharie": (42.5929, -74.4407),
        "schuyler": (42.3937, -76.8699), "seneca": (42.7897, -76.8311),
        "st. lawrence": (44.5001, -75.1499), "steuben": (42.2676, -77.3839),
        "suffolk": (40.9849, -72.6151), "sullivan": (41.7245, -74.7703),
        "tioga": (42.1701, -76.3040), "tompkins": (42.4513, -76.4969),
        "ulster": (41.8886, -74.2621), "warren": (43.5835, -73.8271),
        "washington": (43.3116, -73.4321), "wayne": (43.0826, -77.1161),
        "westchester": (41.1220, -73.7949), "wyoming": (42.7076, -78.2407),
        "yates": (42.6387, -77.1063),
    },
    "PA": {
        "allegheny": (40.4406, -79.9959), "philadelphia": (39.9526, -75.1652),
        "montgomery": (40.2115, -75.3697), "bucks": (40.3288, -75.1305),
        "chester": (39.9607, -75.6055), "delaware": (39.9176, -75.3996),
        "lancaster": (40.0379, -76.3055), "york": (39.9626, -76.7279),
        "berks": (40.4162, -75.9268), "lehigh": (40.6085, -75.4916),
        "northampton": (40.7459, -75.3079), "dauphin": (40.2737, -76.8844),
        "cumberland": (40.1937, -77.2083), "luzerne": (41.2579, -75.8813),
        "lackawanna": (41.4834, -75.6218), "erie": (42.1292, -80.0851),
        "westmoreland": (40.3090, -79.4657), "washington": (40.1735, -80.2462),
        "cambria": (40.4851, -78.7989), "centre": (40.9176, -77.8169),
    },
    "NJ": {
        "bergen": (40.9585, -74.0745), "essex": (40.7857, -74.2469),
        "hudson": (40.7357, -74.0710), "middlesex": (40.4393, -74.4059),
        "monmouth": (40.2171, -74.2613), "ocean": (39.8601, -74.2341),
        "union": (40.6640, -74.2707), "passaic": (40.9262, -74.2396),
        "morris": (40.8590, -74.5548), "somerset": (40.5662, -74.5887),
    },
    "VA": {
        "fairfax": (38.8462, -77.3064), "prince william": (38.6985, -77.5511),
        "loudoun": (39.0850, -77.6582), "arlington": (38.8816, -77.0910),
        "chesterfield": (37.3776, -77.5833), "virginia beach": (36.8529, -75.9780),
        "henrico": (37.5054, -77.3472), "stafford": (38.4218, -77.4383),
    },
    "OH": {
        "franklin": (39.9612, -82.9988), "cuyahoga": (41.4993, -81.6944),
        "hamilton": (39.1612, -84.5419), "summit": (41.0534, -81.5382),
        "montgomery": (39.7589, -84.1916), "lucas": (41.6528, -83.5552),
        "stark": (40.8151, -81.3777), "butler": (39.4453, -84.5738),
    },
    "IL": {
        "cook": (41.8781, -87.6298), "dupage": (41.8419, -88.0937),
        "lake": (42.2917, -87.9673), "will": (41.4478, -88.0429),
        "kane": (41.9338, -88.4316), "winnebago": (42.3360, -89.0640),
        "sangamon": (39.7817, -89.6501), "peoria": (40.6936, -89.5889),
    },
    "TX": {
        "harris": (29.7604, -95.3698), "dallas": (32.7767, -96.7970),
        "tarrant": (32.7555, -97.3308), "bexar": (29.4241, -98.4936),
        "travis": (30.2672, -97.7431), "collin": (33.2148, -96.6649),
        "denton": (33.2148, -97.1331), "el paso": (31.7619, -106.4850),
        "williamson": (30.6224, -97.6914), "fort bend": (29.5441, -95.7750),
        "montgomery": (30.3077, -95.5011), "brazoria": (29.1570, -95.4371),
    },
    "MI": {
        "wayne": (42.3314, -83.0458), "oakland": (42.6389, -83.2911),
        "macomb": (42.6689, -82.9180), "kent": (42.9634, -85.6681),
        "genesee": (43.0225, -83.6875), "washtenaw": (42.2459, -83.7130),
    },
    "IN": {
        "marion": (39.7684, -86.1581), "lake": (41.4731, -87.3811),
        "allen": (41.0793, -85.1394), "hamilton": (40.0462, -86.0461),
        "tippecanoe": (40.4168, -86.8753),
    },
    "MN": {
        "hennepin": (44.9778, -93.2650), "ramsey": (44.9537, -93.0900),
        "dakota": (44.7246, -93.1144), "anoka": (45.2733, -93.2466),
        "washington": (44.9970, -92.8685), "scott": (44.6552, -93.5246),
    },
    "WI": {
        "milwaukee": (43.0389, -87.9065), "dane": (43.0731, -89.4013),
        "waukesha": (43.0117, -88.2315), "brown": (44.4999, -87.9065),
        "racine": (42.7261, -87.7829),
    },
    "MO": {
        "st. louis": (38.6270, -90.1994), "jackson": (39.0997, -94.5786),
        "st. louis city": (38.6270, -90.1994), "greene": (37.1978, -93.2350),
        "clay": (39.3183, -94.4079),
    },
    "KS": {
        "johnson": (38.8814, -94.8191), "wyandotte": (39.1141, -94.7626),
        "sedgwick": (37.6872, -97.3301), "shawnee": (39.0481, -95.7781),
        "douglas": (38.9717, -95.2353),
    },
    "OK": {
        "oklahoma": (35.4676, -97.5164), "tulsa": (36.1540, -95.9928),
        "cleveland": (35.2226, -97.4395), "canadian": (35.5270, -97.9787),
        "comanche": (34.6586, -98.4934),
    },
    "GA": {
        "fulton": (33.7490, -84.3880), "gwinnett": (33.9519, -83.9895),
        "dekalb": (33.7717, -84.2218), "cobb": (33.9526, -84.5499),
        "chatham": (31.9724, -81.1000), "hall": (34.3035, -83.8410),
    },
    "CA": {
        "los angeles": (34.0522, -118.2437), "san diego": (32.7157, -117.1611),
        "orange": (33.7175, -117.8311), "riverside": (33.9533, -116.9906),
        "san bernardino": (34.1083, -117.2898), "santa clara": (37.3541, -121.9552),
        "alameda": (37.6017, -121.7195), "contra costa": (37.9161, -121.9113),
        "sacramento": (38.5816, -121.4944), "fresno": (36.7378, -119.7871),
        "kern": (35.3733, -118.7798), "san joaquin": (37.9577, -121.2908),
        "solano": (38.2494, -121.9018), "ventura": (34.2745, -119.2290),
    },
}

# State centroid fallbacks
STATE_CENTROIDS: dict[str, tuple[float, float]] = {
    "AL": (32.806671, -86.791130), "AK": (61.370716, -152.404419),
    "AZ": (33.729759, -111.431221), "AR": (34.969704, -92.373123),
    "CA": (36.116203, -119.681564), "CO": (39.059811, -105.311104),
    "CT": (41.597782, -72.755371), "DE": (39.318523, -75.507141),
    "FL": (27.766279, -81.686783), "GA": (33.040619, -83.643074),
    "HI": (21.094318, -157.498337), "ID": (44.240459, -114.478828),
    "IL": (40.349457, -88.986137), "IN": (39.849426, -86.258278),
    "IA": (42.011539, -93.210526), "KS": (38.526600, -96.726486),
    "KY": (37.668140, -84.670067), "LA": (31.169960, -91.867805),
    "ME": (44.693947, -69.381927), "MD": (39.063946, -76.802101),
    "MA": (42.230171, -71.530106), "MI": (43.326618, -84.536095),
    "MN": (45.694454, -93.900192), "MS": (32.741646, -89.678696),
    "MO": (38.456085, -92.288368), "MT": (46.921925, -110.454353),
    "NE": (41.125370, -98.268082), "NV": (38.313515, -117.055374),
    "NH": (43.452492, -71.563896), "NJ": (40.298904, -74.521011),
    "NM": (34.840515, -106.248482), "NY": (42.165726, -74.948051),
    "NC": (35.630066, -79.806419), "ND": (47.528912, -99.784012),
    "OH": (40.388783, -82.764915), "OK": (35.565342, -96.928917),
    "OR": (44.572021, -122.070938), "PA": (40.590752, -77.209755),
    "RI": (41.680893, -71.511780), "SC": (33.856892, -80.945007),
    "SD": (44.299782, -99.438828), "TN": (35.747845, -86.692345),
    "TX": (31.054487, -97.563461), "UT": (40.150032, -111.862434),
    "VT": (44.045876, -72.710686), "VA": (37.769337, -78.169968),
    "WA": (47.400902, -121.490494), "WV": (38.491226, -80.954453),
    "WI": (44.268543, -89.616508), "WY": (42.755966, -107.302490),
    "DC": (38.895111, -77.036366),
}


def lookup_county_centroid(county: Optional[str], state: Optional[str]) -> Optional[tuple[float, float]]:
    """
    Look up lat/lon for a US county from the static table.
    Falls back to state centroid if county not found.
    """
    if state:
        state_up = state.upper().strip()
        if county:
            county_lower = county.lower().strip()
            # Try direct match
            state_counties = COUNTY_CENTROIDS.get(state_up, {})
            if county_lower in state_counties:
                return state_counties[county_lower]
            # Try partial match (e.g. "St. Lawrence" -> "st. lawrence")
            for key, coords in state_counties.items():
                if key in county_lower or county_lower in key:
                    return coords
        # Fall back to state centroid
        if state_up in STATE_CENTROIDS:
            return STATE_CENTROIDS[state_up]
    return None


def geocode_projects_inplace(df, db=None) -> None:
    """
    Geocode a DataFrame of projects inplace using county/state lookups.
    Updates latitude/longitude columns. No network calls — static lookup only.
    Persists results to DB cache if provided.
    """
    if df.empty:
        return
    for idx, row in df.iterrows():
        lat = row.get("latitude")
        lon = row.get("longitude")
        if lat and lon and lat != 0 and lon != 0:
            continue  # Already has coords
        coords = lookup_county_centroid(
            row.get("county"), row.get("state")
        )
        if coords:
            df.at[idx, "latitude"] = coords[0]
            df.at[idx, "longitude"] = coords[1]
            if db is not None:
                try:
                    key = f"{row.get('county','')},{row.get('state','')}".lower()
                    db.save_geocode(key, coords[0], coords[1], provider="static_centroid")
                except Exception:
                    pass

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
