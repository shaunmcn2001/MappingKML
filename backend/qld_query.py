# qld_query.py
"""
QLD parcel lookup returning GeoJSON (WGS84).

Accepts flexible inputs and normalises to QLD "lotplan", for example:
- "3SP181800"
- "Lot 3 on Survey Plan 181800"  ->  "3SP181800"
- "3 RP912949"                    ->  "3RP912949"
- "3//SP181800"                   ->  "3SP181800"
- "3/ SP181800" or "3 SP181800"   ->  "3SP181800"

Returns a GeoJSON FeatureCollection with geometry in EPSG:4326 and attributes preserved.
"""

import re
import requests
from typing import Tuple, Dict, Any

# QLD DCDB lot boundary layer (polygons)
# Fields (key ones): lotplan, lot, plan
QLD_FEATURESERVER_LOT_BOUNDARY = (
    "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/"
    "Basemaps/FoundationData/FeatureServer/2/query"
)

# Common QLD plan prefixes you’re likely to see
_PREFIXES = ["SP", "RP", "CP", "BUP", "GTP", "PUP", "SL", "AP", "CH", "MCH", "PH", "SUB", "USL"]

class QLDQueryError(Exception):
    pass

def _clean(s: str) -> str:
    return re.sub(r"\s+", "", s.strip())

def _parse_qld_lotplan(raw: str) -> Tuple[str, str, str]:
    """
    Returns (lot, plan_prefix+number, lotplan) where lotplan=lot+planlabel.
    Raises QLDQueryError if it cannot parse.
    """
    s = raw.strip()

    # 1) Verbose "Lot X on Survey/Registered Plan Y"
    m = re.search(
        r"(?i)lot\s*(\d+)\s*(?:on\s*(?:registered|survey)\s*plan\s*)?"
        r"([A-Za-z]{1,4})?\s*(\d{1,7})",
        s,
    )
    if m:
        lot = m.group(1)
        pref = (m.group(2) or "SP").upper()  # default to SP when omitted in verbose text
        num = m.group(3)
        planlabel = f"{pref}{num}"
        return lot, planlabel, f"{lot}{planlabel}"

    # 2) Slash/space combos like "3//SP181800", "3/SP181800", "3 SP181800"
    m = re.match(r"^\s*(\d+)\s*[/ ]{0,2}\s*([A-Za-z]{1,4})\s*(\d{1,7})\s*$", s)
    if m:
        lot = m.group(1)
        planlabel = f"{m.group(2).upper()}{m.group(3)}"
        return lot, planlabel, f"{lot}{planlabel}"

    # 3) Pure concatenated lotplan like "3SP181800"
    m = re.match(r"^\s*(\d+)\s*([A-Za-z]{1,4})\s*(\d{1,7})\s*$", s)
    if m:
        lot = m.group(1)
        planlabel = f"{m.group(2).upper()}{m.group(3)}"
        return lot, planlabel, f"{lot}{planlabel}"

    # 4) Two tokens: "3 181800" -> assume SP if prefix omitted
    m = re.match(r"^\s*(\d+)\s+(\d{1,7})\s*$", s)
    if m:
        lot = m.group(1)
        planlabel = f"SP{m.group(2)}"
        return lot, planlabel, f"{lot}{planlabel}"

    # 5) Already a single combined token like "3SP181800" (no spaces at all)
    t = _clean(s)
    m = re.match(r"^(\d+)([A-Za-z]{1,4})(\d{1,7})$", t)
    if m:
        lot = m.group(1)
        planlabel = f"{m.group(2).upper()}{m.group(3)}"
        return lot, planlabel, f"{lot}{planlabel}"

    raise QLDQueryError(
        "Could not parse QLD lot/plan. Try formats like '3SP181800' or 'Lot 3 on Survey Plan 181800'."
    )

def query_qld(user_input: str, timeout: int = 30) -> Dict[str, Any]:
    """
    Queries QLD DCDB lot boundary by exact lotplan match.
    Returns GeoJSON FeatureCollection in EPSG:4326.
    """
    if not user_input or not user_input.strip():
        raise QLDQueryError("Empty input.")

    lot, planlabel, lotplan = _parse_qld_lotplan(user_input)

    where = f"UPPER(lotplan)=UPPER('{lotplan}')"
    params = {
        "where": where,
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": 4326,   # WGS84 for your map/KML
        "f": "geojson",
    }

    r = requests.get(QLD_FEATURESERVER_LOT_BOUNDARY, params=params, timeout=timeout)
    try:
        r.raise_for_status()
    except Exception as e:
        raise QLDQueryError(f"QLD request failed: {e}")

    data = r.json()
    feats = data.get("features", [])
    if not feats:
        # Friendly fallback: if exact lotplan missed, try lot + plan fields separately
        params2 = dict(params)
        params2["where"] = (
            f"UPPER(lot)=UPPER('{lot}') AND UPPER(plan)=UPPER('{planlabel}')"
        )
        r2 = requests.get(QLD_FEATURESERVER_LOT_BOUNDARY, params=params2, timeout=timeout)
        r2.raise_for_status()
        data2 = r2.json()
        feats2 = data2.get("features", [])
        if not feats2:
            raise QLDQueryError(
                f"QLD returned 0 features for lotplan '{lotplan}'. "
                f"Tried lot='{lot}' AND plan='{planlabel}' as well."
            )
        return data2

    return data
