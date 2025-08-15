# nsw_query.py
import re
import requests
from typing import Optional, Tuple, Dict, Any, List

NSW_FEATURESERVER_8 = (
    "https://portal.spatial.nsw.gov.au/server/rest/services/"
    "NSW_Land_Parcel_Property_Theme/FeatureServer/8/query"
)

# Common attribute keys that may hold "section"
SECTION_KEYS = ["section", "sectionnumber", "sec", "section_no", "sect_no", "section_num"]

class NSWQueryError(Exception):
    pass

def _clean_token(s: str) -> str:
    return re.sub(r"\s+", "", s.strip())

def _normalise_plan(plan: str) -> str:
    p = _clean_token(plan).upper()
    # digits only -> assume DP (NSW default)
    if re.fullmatch(r"\d{1,7}", p):
        return f"DP{p}"
    # e.g. DP753311 / SP181800
    if re.fullmatch(r"[A-Z]{1,3}\d{1,7}", p):
        return p
    p2 = re.sub(r"\s+", "", p)
    if re.fullmatch(r"[A-Z]{1,3}\d{1,7}", p2):
        return p2
    raise NSWQueryError(f"Could not parse plan label from '{plan}'. Use e.g. 'DP753311'.")

def _validate_lot_plan(lot: str, planlabel: str) -> None:
    if not re.fullmatch(r"\d+", lot):
        raise NSWQueryError(f"Invalid lot '{lot}'. Lot must be an integer.")
    if not re.fullmatch(r"[A-Z]{1,3}\d{1,7}", planlabel):
        raise NSWQueryError(f"Invalid plan '{planlabel}'. Expected like 'DP753311'.")

def parse_lot_section_plan(raw: str) -> Tuple[str, Optional[str], str]:
    """
    Accepts:
      - 'lot/section/plan'   e.g. '3/2/DP753311'
      - 'lot//plan'          e.g. '3//DP753311' (no section)
      - 'lot/plan'           e.g. '3/DP753311'  (treated as lot//plan)
      - 'Lot 3 Sec 2 DP753311' or 'Lot 3 DP753311'
    Returns: (lot, section_or_None, planlabel)
    """
    s = raw.strip()

    # Verbose formats (Lot/Sec/Plan in any spacing)
    m = re.search(
        r"(?i)lot\s*(\d+)\s*(?:sec(?:tion)?\s*(\w+))?\s*(?:dp|sp|cp|pp|mp)?\s*([a-zA-Z]{1,3})?\s*(\d{1,7})",
        s,
    )
    if m:
        lot = m.group(1)
        sec = m.group(2)
        pref = (m.group(3) or "").upper()
        num = m.group(4)
        planlabel = f"{pref}{num}" if pref else f"DP{num}"
        section = _clean_token(sec) if sec else None
        _validate_lot_plan(lot, planlabel)
        return lot, section, planlabel

    # Slash formats
    parts = [p.strip() for p in s.split("/") if p is not None]

    if len(parts) == 3:
        lot, section, plan = parts[0], parts[1], parts[2]
        lot = _clean_token(lot)
        section = _clean_token(section) or None
        planlabel = _normalise_plan(plan)
        _validate_lot_plan(lot, planlabel)
        return lot, section, planlabel

    if len(parts) == 2:
        # treat 'lot/plan' as 'lot//plan'
        lot, plan = parts[0], parts[1]
        lot = _clean_token(lot)
        planlabel = _normalise_plan(plan)
        _validate_lot_plan(lot, planlabel)
        return lot, None, planlabel

    # Space separated: "3 DP753311"
    m2 = re.match(r"^\s*(\d+)\s*([A-Za-z]{1,3})\s*(\d{1,7})\s*$", s)
    if m2:
        lot = _clean_token(m2.group(1))
        planlabel = f"{m2.group(2).upper()}{m2.group(3)}"
        _validate_lot_plan(lot, planlabel)
        return lot, None, planlabel

    raise NSWQueryError(
        "NSW expects 'lot/section/plan'. If there is no section, use 'lot//plan' (e.g., 3//DP753311)."
    )

def query_nsw_lsp(user_input: str, timeout: int = 30) -> Dict[str, Any]:
    """
    Queries NSW by lot/section/plan (section optional).
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
    r.raise_for_status()
    data = r.json()
    feats: List[Dict[str, Any]] = data.get("features", [])

    if not feats:
        raise NSWQueryError(
            f"No NSW parcels for lot '{lot}' and plan '{planlabel}'. "
            "Confirm the plan is NSW (DP/SP/etc.) and that the lot exists on that plan."
        )

    if section is None:
        return data

    # Filter section client-side across common keys
    def _sec_match(props: Dict[str, Any]) -> bool:
        for k in SECTION_KEYS:
            if k in props and props[k] is not None:
                if str(props[k]).strip().upper() == str(section).strip().upper():
                    return True
        return False

    filtered = [
        f for f in feats
        if _sec_match(f.get("properties") or f.get("attributes") or {})
    ]
    if not filtered:
        return {
            "type": "FeatureCollection",
            "features": [],
            "note": (
                f"Found {len(feats)} feature(s) for Lot {lot} {planlabel}, "
                f"but none matched section '{section}'. If there is no section, try 'lot//plan'."
            ),
        }

    return {"type": "FeatureCollection", "features": filtered}
