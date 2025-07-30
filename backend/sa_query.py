import re
import requests

SA_FEATURE_URL = "https://dpti.geohub.sa.gov.au/server/rest/services/Hosted/Reference_WFL1/FeatureServer/1/query"

# Accepts "H835100 B829" or "H 835100 B 829" (case-insensitive)
_SA_PATTERN = re.compile(
    r"^\s*(?P<plan_t>[A-Za-z])\s*(?P<plan>\d{1,9})\s+(?P<parcel_t>[A-Za-z])\s*(?P<parcel>\d{1,10})\s*$"
)

def parse_sa_token(q: str):
    """
    Parse inputs like 'H835100 B829' into:
    {'plan_t': 'H', 'plan': '835100', 'parcel_t': 'B', 'parcel': '829'}
    Return None if not matched.
    """
    if not q:
        return None
    m = _SA_PATTERN.match(q.strip())
    if not m:
        return None
    return {
        "plan_t": m.group("plan_t").upper(),
        "plan": m.group("plan"),
        "parcel_t": m.group("parcel_t").upper(),
        "parcel": m.group("parcel"),
    }

def build_sa_where(parts: dict) -> str:
    # Layer 1 fields: plan_t, plan, parcel_t, parcel
    return (
        f"plan_t='{parts['plan_t']}' AND plan='{parts['plan']}' "
        f"AND parcel_t='{parts['parcel_t']}' AND parcel='{parts['parcel']}'"
    )

def query_sa_feature_server(where: str, out_sr: int = 4326):
    """
    Call SA FeatureServer and return GeoJSON FeatureCollection.
    """
    params = {
        "f": "geojson",
        "where": where,
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": str(out_sr),
        "cacheHint": "true",
        "resultRecordCount": "2000",
    }
    r = requests.get(SA_FEATURE_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    # Normalize unexpected responses to an empty FeatureCollection with error info
    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        return {"type": "FeatureCollection", "features": [], "error": data}
    return data

def search_sa(q: str):
    """
    Public entrypoint used by the region router. Returns GeoJSON FeatureCollection.
    """
    parts = parse_sa_token(q)
    if not parts:
        return {"type": "FeatureCollection", "features": []}
    where = build_sa_where(parts)
    try:
        return query_sa_feature_server(where)
    except Exception as exc:
        return {"type": "FeatureCollection", "features": [], "error": str(exc)}
