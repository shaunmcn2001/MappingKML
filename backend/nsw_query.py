# nsw_cadastre.py
"""
Lightweight NSW Cadastre (layer 9) client — query by lotidstring only.

- Endpoint: https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_Cadastre/MapServer/9/query
- WHERE supports simple equality only (no SQL functions).
- Use exact: where=lotidstring='<UPPER VALUE WITH //>'

Public API:
    - normalize_lotid(raw: str) -> str
    - fetch_one(lotid: str, *, timeout=12) -> dict[FeatureCollection]
    - fetch_bulk(lotids: list[str], *, max_workers=6, timeout=12) -> dict[FeatureCollection]
    - count(lotid: str, *, timeout=8) -> int
    - ids_only(lotid: str, *, timeout=8) -> list[int]

All functions return GeoJSON FeatureCollection (or simple types where noted).
No external deps beyond 'requests' (install via pip if needed).
"""

from __future__ import annotations

import json
import re
from typing import Dict, List, Tuple
import concurrent.futures
import requests

# ---- Constants ----

NSW_LAYER9_QUERY = (
    "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_Cadastre/MapServer/9/query"
)

DEFAULT_TIMEOUT = 12
DEFAULT_MAX_WORKERS = 6

# Shared HTTP session for connection reuse
_SESSION = requests.Session()

# ---- Utilities ----

_RE_NSW_LOTID_DOUBLE = re.compile(r"^\s*(?P<lot>\d+)\s*//\s*(?P<plan>[A-Za-z]{1,6}\d+)\s*$")
_RE_NSW_LOTID_ONE    = re.compile(r"^\s*(?P<lot>\d+)\s*/\s*(?P<plan>[A-Za-z]{1,6}\d+)\s*$")


def normalize_lotid(raw: str) -> str:
    """
    Normalize user input to NSW 'LOT//PLAN' uppercase form.

    Accepts:
        - "13//DP1246224"
        - "13/DP1246224"
        - with arbitrary spacing and casing

    Returns:
        "13//DP1246224" (uppercased, double slash), or original trimmed UPPER if no match.
    """
    s = (raw or "").strip().upper().replace(" ", "")
    m = _RE_NSW_LOTID_DOUBLE.match(s)
    if m:
        return f"{m.group('lot')}//{m.group('plan')}"
    m = _RE_NSW_LOTID_ONE.match(s)
    if m:
        return f"{m.group('lot')}//{m.group('plan')}"
    return s


def _http_get_json(params: Dict, *, timeout: int) -> Dict:
    """
    Issue a GET to the NSW layer with safe defaults; caller passes specific params.
    """
    base = {
        "f": "json",
        "outSR": 4326,
        "returnGeometry": "true",
        "geometryPrecision": 6,
        "returnExceededLimitFeatures": "false",
    }
    resp = _SESSION.get(NSW_LAYER9_QUERY, params={**base, **params}, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _arcgis_to_featurecollection(data: Dict) -> Dict:
    """
    Convert ArcGIS service 'features' into GeoJSON FeatureCollection.
    """
    feats = []
    for g in data.get("features", []):
        geom = g.get("geometry")
        attrs = g.get("attributes", {}) or {}

        if not geom:  # skip if no geometry
            continue

        if "rings" in geom:  # polygon
            geo = {"type": "Polygon", "coordinates": geom["rings"]}
        elif "paths" in geom:  # multiline
            geo = {"type": "MultiLineString", "coordinates": geom["paths"]}
        elif "x" in geom and "y" in geom:  # point
            geo = {"type": "Point", "coordinates": [geom["x"], geom["y"]]}
        else:
            continue

        feats.append({"type": "Feature", "geometry": geo, "properties": attrs})

    return {"type": "FeatureCollection", "features": feats}


# ---- Public: one-shot feature fetch ----

def fetch_one(lotid: str, *, timeout: int = DEFAULT_TIMEOUT) -> Dict:
    """
    One-shot query: attributes + geometry by exact lotidstring.

    Returns:
        GeoJSON FeatureCollection (may have 0, 1, or many features).
    """
    lotid_norm = normalize_lotid(lotid)
    params = {
        "where": f"lotidstring='{lotid_norm}'",  # NOTE: simple equality only
        "outFields": "*",
    }
    data = _http_get_json(params, timeout=timeout)
    return _arcgis_to_featurecollection(data)


# ---- Public: bulk feature fetch (parallel) ----

def fetch_bulk(lotids: List[str], *, max_workers: int = DEFAULT_MAX_WORKERS, timeout: int = DEFAULT_TIMEOUT) -> Dict:
    """
    Fetch many lotidstrings in parallel and merge into one FeatureCollection.

    - Each lot is fetched with a one-shot query (attributes + geometry).
    - Results are de-duplicated by (objectid, lotidstring).

    Args:
        lotids: list of lotidstring-like inputs (any casing; '/' or '//' accepted)
        max_workers: small thread pool size (default 6)
        timeout: per-request timeout in seconds (default 12)

    Returns:
        GeoJSON FeatureCollection.
    """
    lotids_norm = [normalize_lotid(x) for x in lotids if x and str(x).strip()]
    if not lotids_norm:
        return {"type": "FeatureCollection", "features": []}

    def _task(lid: str) -> Tuple[str, Dict]:
        try:
            return lid, fetch_one(lid, timeout=timeout)
        except Exception as e:
            return lid, {"_error": str(e)}

    features: List[Dict] = []
    errors: List[str] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        for lid, res in ex.map(_task, lotids_norm):
            if "_error" in res:
                errors.append(f"{lid}: {res['_error']}")
                continue
            features.extend(res.get("features", []))

    # De-dup by (objectid, lotidstring)
    seen = set()
    uniq = []
    for f in features:
        props = f.get("properties") or {}
        sig = (props.get("objectid"), props.get("lotidstring"))
        if sig not in seen:
            seen.add(sig)
            uniq.append(f)

    fc = {"type": "FeatureCollection", "features": uniq}
    if errors:
        # Non-fatal; if you want to inspect:
        fc["_errors"] = errors
    return fc


# ---- Optional helpers: count & ids-only (you can ignore if not needed) ----

def count(lotid: str, *, timeout: int = 8) -> int:
    """
    Fast existence check; returns integer count.
    """
    lotid_norm = normalize_lotid(lotid)
    data = _http_get_json(
        {"where": f"lotidstring='{lotid_norm}'", "returnCountOnly": "true"},
        timeout=timeout,
    )
    return int(data.get("count", 0) or 0)


def ids_only(lotid: str, *, timeout: int = 8) -> List[int]:
    """
    Fetch only object IDs for a lotidstring (tiny response).
    Useful if you want to fetch geometry later via objectIds=...
    """
    lotid_norm = normalize_lotid(lotid)
    data = _http_get_json(
        {"where": f"lotidstring='{lotid_norm}'", "returnIdsOnly": "true"},
        timeout=timeout,
    )
    return list(data.get("objectIds", []) or [])


# ---- Tiny CLI for ad-hoc testing ----

if __name__ == "__main__":
    import argparse, sys

    ap = argparse.ArgumentParser(description="NSW layer 9 client (lotidstring only)")
    ap.add_argument("lotids", nargs="+", help="lotidstrings like 13//DP1246224 or 13/DP1246224")
    ap.add_argument("--bulk", action="store_true", help="fetch all lotids in parallel")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = ap.parse_args()

    try:
        if args.bulk and len(args.lotids) > 1:
            fc = fetch_bulk(args.lotids, timeout=args.timeout)
        else:
            fc = fetch_one(args.lotids[0], timeout=args.timeout)
        sys.stdout.write(json.dumps(fc, ensure_ascii=False, indent=2))
    except Exception as e:
        sys.stderr.write(f"ERROR: {e}\n")
        sys.exit(1)