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
    Robust NSW query:
      1) Parse lot/section/plan from user input
      2) Query ALL parcels for the plan (planlabel) once
      3) Filter locally by lotnumber (and sectionnumber if provided)
    Returns GeoJSON FeatureCollection in EPSG:4326.
    """
    lot, section, planlabel = parse_lot_section_plan(user_input)

    # --- Step 1: fetch all features on the plan (server-side WHERE is simple & reliable)
    base_params = {
        "where": f"UPPER(planlabel)=UPPER('{planlabel}')",
        "outFields": "*",
        "outSR": 4326,
        "returnGeometry": "true",
        "f": "geojson",
    }
    r = requests.get(NSW_FEATURESERVER_8, params=base_params, timeout=timeout)
    try:
        r.raise_for_status()
    except Exception as e:
        raise NSWQueryError(f"NSW request failed: {e}")

    data = r.json()
    feats = data.get("features", [])

    if not feats:
        # Extra helpful context for your UI
        raise NSWQueryError(
            f"No NSW parcels for plan '{planlabel}'. "
            f"Confirm the plan exists and is a NSW DP/SP/CP."
        )

    # --- Step 2: filter by lot (and optional section) client-side
    def _props(feat):
        return feat.get("properties") or feat.get("attributes") or {}

    lot_filtered = [f for f in feats if str(_props(f).get("lotnumber", "")).strip().upper() == str(lot).strip().upper()]

    if not lot_filtered:
        # Show available lots on this plan to guide the user
        lots_available = sorted({str(_props(f).get("lotnumber", "")).strip() for f in feats if _props(f).get("lotnumber") is not None})
        raise NSWQueryError(
            f"No lot '{lot}' found on plan '{planlabel}'. "
            f"Lots on this plan include: {', '.join(lots_available) if lots_available else 'n/a'}."
        )

    if section is None:
        return {"type": "FeatureCollection", "features": lot_filtered}

    # Filter by section across common keys; NSW generally uses 'sectionnumber'
    SECTION_KEYS = ["sectionnumber", "section", "sec", "section_no", "sect_no", "section_num"]
    def _section_match(props):
        target = str(section).strip().upper()
        for k in SECTION_KEYS:
            if k in props and props[k] is not None:
                if str(props[k]).strip().upper() == target:
                    return True
        return False

    final = [f for f in lot_filtered if _section_match(_props(f))]

    if not final:
        return {
            "type": "FeatureCollection",
            "features": [],
            "note": (
                f"Found {len(lot_filtered)} feature(s) for Lot {lot} on {planlabel}, "
                f"but none matched section '{section}'. If there is no section, use 'lot//plan'."
            ),
        }

    return {"type": "FeatureCollection", "features": final}

