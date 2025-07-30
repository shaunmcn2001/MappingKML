"""
VIC Vicmap Parcel query helpers.

We hit:
  https://services6.arcgis.com/GB33F62SbDxJjwEL/ArcGIS/rest/services/Vicmap_Parcel/FeatureServer/0/query

Key fields:
  - parcel_plan_number (e.g. PS601720, TP17741)
  - parcel_lot_number  (e.g. 24)
"""

import re
import requests

VIC_FEATURE_URL = "https://services6.arcgis.com/GB33F62SbDxJjwEL/ArcGIS/rest/services/Vicmap_Parcel/FeatureServer/0/query"

# Accepts "24PS601720" or "24 PS601720" or just "PS601720"
_VIC_WITH_LOT = re.compile(r"^\s*(?P<lot>\d{1,5})\s*(?P<plan>(?:PS|TP)[0-9A-Z]+)\s*$", re.IGNORECASE)
_VIC_PLAN_ONLY = re.compile(r"^\s*(?P<plan>(?:PS|TP)[0-9A-Z]+)\s*$", re.IGNORECASE)

def _clean_plan(p: str) -> str:
    return p.replace(" ", "").upper()

def _query(where: str, out_sr: int = 4326) -> dict:
    params = {
        "f": "geojson",
        "where": where,
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": str(out_sr),
        "cacheHint": "true",
        "resultRecordCount": "2000",
    }
    r = requests.get(VIC_FEATURE_URL, params=params, timeout=40)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        return {"type": "FeatureCollection", "features": [], "error": data}
    return data

def search_vic(q: str) -> dict:
    """
    Try to parse VIC lot/plan input and query Vicmap. Returns FeatureCollection.
    """
    if not q:
        return {"type": "FeatureCollection", "features": []}

    text = q.strip().upper().replace("/", " ")

    m = _VIC_WITH_LOT.match(text)
    if m:
        lot = m.group("lot").lstrip("0") or "0"
        plan = _clean_plan(m.group("plan"))
        where = f"parcel_lot_number='{lot}' AND parcel_plan_number='{plan}'"
        try:
            return _query(where)
        except Exception as exc:
            return {"type": "FeatureCollection", "features": [], "error": str(exc)}

    m = _VIC_PLAN_ONLY.match(text)
    if m:
        plan = _clean_plan(m.group("plan"))
        where = f"parcel_plan_number='{plan}'"
        try:
            return _query(where)
        except Exception as exc:
            return {"type": "FeatureCollection", "features": [], "error": str(exc)}

    return {"type": "FeatureCollection", "features": []}