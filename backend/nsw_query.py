# nsw_query.py
import re
import requests
from typing import Optional, Tuple, Dict, Any, List

NSW_FEATURESERVER_8 = (
    "https://portal.spatial.nsw.gov.au/server/rest/services/"
    "NSW_Land_Parcel_Property_Theme/FeatureServer/8/query"
)

# Common attribute keys we might see for "section"
SECTION_KEYS = ["section", "sectionnumber", "sec", "section_no", "sect_no", "section_num"]

class NSWQueryError(Exception):
    pass

def _clean_token(s: str) -> str:
    return re.sub(r"\s+", "", s.strip())

def parse_lot_section_plan(raw: str) -> Tuple[str, Optional[str], str]:
    """
    Accepts:
      - 'lot/section/plan'   e.g. '3/2/DP753311'
      - 'lot//plan'          e.g. '3//DP753311' (no section)
      - 'lot/plan'           e.g. '3/DP753311'  (interprets as lot//plan)
      - 'Lot 3 Sec 2 DP753311', 'Lot 3 DP753311', etc.
    Returns: (lot, section_or_None, planlabel with prefix e.g. 'DP753311')
    Raises NSWQueryError if it cannot parse.
    """
    s = raw.strip()

    # Normalise common verbose formats → "lot/section/plan"
    # Examples: "Lot 3 Sec 2 DP 753311", "Lot 3 DP753311", "3 DP753311"
    # First, pull out tokens
    m = re.search(
        r"(?i)lot\s*(\d+)\s*(?:sec(?:tion)?\s*(\w+))?\s*(?:dp|sp|cp|pp|mp)?\s*([a-zA-Z]{1,3})?\s*(\d{1,7})",
        s,
    )
    if m:
        lot = m.group(1)
        sec = m.group(2)
        pref = (m.group(3) or "").upper()
        num = m.group(4)
        planlabel = f"{pref}{num}" if pref else f"DP{num}"  # default to DP if prefix omitted
        section = _clean_token(sec) if sec else None
        return lot, section, planlabel.upper()

    # Slash formats
    #  a) lot/section/plan   (section optional)
    #  b) lot//plan          (empty section)
    #  c) lot/plan           (2-part version; treat as lot//plan)
    parts = [p.strip() for p in s.split("/") if p is not None]

    if len(parts) == 3:
        lot, section, plan = parts[0], parts[1], parts[2]
        lot = _clean_token(lot)
        section = _clean_token(section) or None
        planlabel = _normalise_plan(plan)
        _validate_lot_plan(lot, planlabel)
        return lot, section, planlabel

    if len(parts) == 2:
        # Interpret as lot//plan (no section)
        lot, plan = parts[0], parts[1]
        lot = _clean_token(lot)
        planlabel = _normalise_plan(plan)
        _validate_lot_plan(lot, planlabel)
        return lot, None, planlabel

    # Try space separated "3 DP753311"
    m2 = re.match(r"^\s*(\d+)\s*([A-Za-z]{1,3})\s*(\d{1,7})\s*$", s)
    if m2:
        lot = m2.group(1)
        planlabel = f"{m2.group(2).upper()}{m2.group(3)}"
        _validate_lot_plan(lot, planlabel)
        return lot, None, planlabel

    raise NSWQueryError(
        "NSW expects 'lot/section/plan' (use empty section like '3//DP753311')."
    )

def _normalise_plan(plan: str) -> str:
    p = _clean_token(plan).upper()
    # If user only typed digits, assume DP (NSW default)
    if re.fullmatch(r"\d{1,7}", p):
        return f"DP{p}"
    # If they typed e.g. SP181800 or DP753311, keep as-is
    if re.fullmatch(r"[A-Z]{1,3}\d{1,7}", p):
        return p
    # If they typed "DP 753311" or "SP 181800"
    p2 = re.sub(r"\s+", "", p)
    if re.fullmatch(r"[A-Z]{1,3}\d{1,7}", p2):
        return p2
    raise NSWQueryError(f"Could not parse plan label from '{plan}'. Use e.g. 'DP753311'.")

def _validate_lot_plan(lot: str, planlabel: str) -> None:
    if not re.fullmatch(r"\d+", lot):
        raise NSWQueryError(f"Invalid lot '{lot}'. Lot must be an integer.")
    if not re.fullmatch(r"[A-Z]{1,3}\d{1,7}", planlabel):
        raise NSWQueryError(f"Invalid plan '{planlabel}'. Expected like 'DP753311'.")

def query_nsw_lsp(user_input: str, timeout: int = 30) -> Dict[str, Any]:
    """
    Queries NSW by lot/section/plan (section optional).
    Server-side WHERE uses planlabel + lotnumber.
    If a section was supplied, we filter the returned features client-side
    across multiple possible section attribute names.
    Returns a GeoJSON FeatureCollection in EPSG:4326.
    """
    lot, section, planlabel = parse_lot_section_plan(user_input)

    where = f"UPPER(lotnumber)=UPPER('{lot}') AND UPPER(planlabel)=UPPER('{planlabel}')"
    params = {
        "where": where,
        "outFields": "*",
        "outSR": 4326,
        "f": "geojson",
        "returnGeometry": "true",
    }

    r = requests.get(NSW_FEATURESERVER_8, params=params, timeout=timeout)
    try:
        r.raise_for_status()
    except Exception as e:
        raise NSWQueryError(f"NSW request failed: {e}")

    data = r.json()
    feats: List[Dict[str, Any]] = data.get("features", [])

    if not feats:
        raise NSWQueryError(
            f"No NSW parcels for lot '{lot}' and plan '{planlabel}'. "
            "Check that the plan is a NSW DP/SP/etc. and the lot exists on that plan."
        )

    if section is None:
        # No section provided → return all matches for that lot+plan
        return data

    # Filter by section across common attribute names
    def _sec_match(attrs: Dict[str, Any]) -> bool:
        for k in SECTION_KEYS:
            if k in attrs and attrs[k] is not None:
                val = str(attrs[k]).strip().upper()
                if val == str(section).strip().upper():
                    return True
        return False

    filtered = [f for f in feats if _sec_match(f.get("properties", {}))]
    if not filtered:
        # If nothing matched on section, keep behaviour helpful:
        # return empty collection but with a useful message
        return {
            "type": "FeatureCollection",
            "features": [],
            "note": (
                f"Found {len(feats)} feature(s) for lot '{lot}' and plan '{planlabel}', "
                f"but none matched section '{section}'. "
                f"If this parcel has no section, try input 'lot//plan'."
            ),
        }

    return {"type": "FeatureCollection", "features": filtered}
