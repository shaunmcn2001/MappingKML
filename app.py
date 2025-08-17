# app.py — MappingKML
# NSW: layer 9, search ONLY by lotidstring (e.g., 13//DP1246224)
# QLD: lot + plan
# SA : planparcel or title (volume/folio in any order)
# Exports: GeoJSON, KML, KMZ (Google Earth balloons show ALL attributes)

import io
import json
import math
import re
import time
import zipfile
from typing import Dict, List, Optional, Tuple

import requests
import streamlit as st
import pydeck as pdk

# Optional KML export
try:
    import simplekml
    HAVE_SIMPLEKML = True
except Exception:
    HAVE_SIMPLEKML = False

# --------------------- App Config ---------------------

st.set_page_config(page_title="MappingKML", layout="wide")

ENDPOINTS = {
    "QLD": "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Cadastre/LandParcels/MapServer/0/query",
    # NSW pinned to layer 9 (Lot)
    "NSW": "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_Cadastre/MapServer/9/query",
    "SA":  "https://dpti.geohub.sa.gov.au/server/rest/services/Hosted/Reference_WFL1/FeatureServer/1/query",
}

DEFAULT_VIEW = pdk.ViewState(latitude=-32.0, longitude=147.0, zoom=5.0, pitch=0, bearing=0)

# Request tuning (keep the app responsive)
REQUEST_TIMEOUT = 12
REQUEST_RETRIES = 0

# --------------------- Geometry Helpers ---------------------

def _as_fc(fc_like):
    if fc_like is None:
        return None
    if isinstance(fc_like, str):
        try:
            fc_like = json.loads(fc_like)
        except Exception:
            return None
    if not isinstance(fc_like, dict):
        return None
    t = fc_like.get("type")
    if t == "FeatureCollection":
        feats = fc_like.get("features", [])
        return {"type": "FeatureCollection", "features": feats if isinstance(feats, list) else []}
    if t == "Feature":
        return {"type": "FeatureCollection", "features": [fc_like]}
    if t in {"Point","MultiPoint","LineString","MultiLineString","Polygon","MultiPolygon"}:
        return {"type":"FeatureCollection","features":[{"type":"Feature","geometry":fc_like,"properties":{}}]}
    return None

def _iter_coords(geom):
    gtype = (geom or {}).get("type")
    coords = (geom or {}).get("coordinates")
    if gtype == "Point":
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            yield coords[:2]
    elif gtype in ("MultiPoint", "LineString"):
        for c in coords or []:
            if isinstance(c, (list, tuple)) and len(c) >= 2:
                yield c[:2]
    elif gtype in ("MultiLineString", "Polygon"):
        for part in coords or []:
            for c in part or []:
                if isinstance(c, (list, tuple)) and len(c) >= 2:
                    yield c[:2]
    elif gtype == "MultiPolygon":
        for poly in coords or []:
            for ring in poly or []:
                for c in ring or []:
                    if isinstance(c, (list, tuple)) and len(c) >= 2:
                        yield c[:2]

def _geom_bbox(geom):
    min_lng = min_lat = math.inf
    max_lng = max_lat = -math.inf
    found = False
    for lng, lat in _iter_coords(geom or {}):
        if not (isinstance(lng, (int, float)) and isinstance(lat, (int, float))):
            continue
        found = True
        min_lng = min(min_lng, lng); max_lng = max(max_lng, lng)
        min_lat = min(min_lat, lat); max_lat = max(max_lat, lat)
    return (min_lng, min_lat, max_lng, max_lat) if found else None

def _merge_bbox(b1, b2):
    if b1 is None: return b2
    if b2 is None: return b1
    return (min(b1[0], b2[0]), min(b1[1], b2[1]), max(b1[2], b2[2]), max(b1[3], b2[3]))

def _bbox_to_viewstate(bbox, padding_ratio=0.12):
    if not bbox:
        return DEFAULT_VIEW
    min_lng, min_lat, max_lng, max_lat = bbox
    pad_lng = (max_lng - min_lng) * padding_ratio
    pad_lat = (max_lat - min_lat) * padding_ratio
    min_lng -= pad_lng; max_lng += pad_lng
    min_lat -= pad_lat; max_lat += pad_lat
    center_lng = (min_lng + max_lng) / 2.0
    center_lat = (min_lat + max_lat) / 2.0
    extent = max(max_lng - min_lng, max_lat - min_lat)
    if extent <= 0 or not math.isfinite(extent):
        return pdk.ViewState(latitude=center_lat, longitude=center_lng, zoom=14)
    if extent > 20: zoom = 5
    elif extent > 10: zoom = 6
    elif extent > 5: zoom = 7
    elif extent > 2: zoom = 8
    elif extent > 1: zoom = 9
    elif extent > 0.5: zoom = 10
    elif extent > 0.25: zoom = 11
    elif extent > 0.1: zoom = 12
    else: zoom = 13
    return pdk.ViewState(latitude=center_lat, longitude=center_lng, zoom=zoom)

def _fit_view(fc_like):
    fc = _as_fc(fc_like)
    if not fc or not fc.get("features"):
        st.warning("No features to display. Showing default view.", icon="⚠️")
        return DEFAULT_VIEW
    bbox = None
    for feat in fc["features"]:
        b = _geom_bbox((feat or {}).get("geometry") or {})
        bbox = _merge_bbox(bbox, b)
    if bbox is None:
        st.warning("Features had no valid coordinates. Showing default view.", icon="⚠️")
        return DEFAULT_VIEW
    return _bbox_to_viewstate(bbox)

# --------------------- Parsing ---------------------

# NSW lotidstring only (also accept 13/DP1246224 and normalize to 13//DP1246224)
RE_NSW_LOTID = re.compile(r"^\s*(?P<lotid>\d+//[A-Za-z]{1,6}\d+)\s*$")
RE_NSW_ONE_SLASH = re.compile(r"^\s*(?P<lot>\d+)\s*/\s*(?P<plan>[A-Za-z]{1,6}\d+)\s*$")

# QLD
RE_LOTPLAN_SLASH = re.compile(
    r"^\s*(?P<lot>\d+)\s*(?:/(?P<section>\d+))?\s*/\s*(?P<plan_type>[A-Za-z]{1,6})\s*(?P<plan_number>\d+)\s*$"
)
RE_COMPACT = re.compile(r"^\s*(?P<lot>\d+)\s*(?P<plan_type>[A-Za-z]{1,6})\s*(?P<plan_number>\d+)\s*$")
RE_VERBOSE = re.compile(
    r"^\s*Lot\s+(?P<lot>\d+)\s+on\s+(?P<plan_label>(Registered|Survey)\s+Plan)\s+(?P<plan_number>\d+)\s*$",
    re.IGNORECASE
)

# SA
RE_SA_PLANPARCEL = re.compile(r"^\s*(?P<planparcel>[A-Za-z]{1,2}\d+[A-Za-z]{1,2}\d+)\s*$")
RE_SA_TITLEPAIR  = re.compile(r"^\s*(?P<a>\d{1,6})\s*/\s*(?P<b>\d{1,6})\s*$")

def _nsw_normalize_lotid(raw: str) -> str:
    s = (raw or "").strip().upper().replace(" ", "")
    if "//" in s:
        return s
    m = RE_NSW_ONE_SLASH.match(s)
    return f"{m.group('lot')}//{m.group('plan')}" if m else s

def parse_queries(multiline: str) -> List[Dict]:
    items: List[Dict] = []
    lines = [x.strip() for x in (multiline or "").splitlines() if x.strip()]
    for raw in lines:
        m = RE_NSW_LOTID.match(raw) or RE_NSW_ONE_SLASH.match(raw)
        if m:
            lotid = _nsw_normalize_lotid(raw)
            items.append({"raw": raw, "nsw_lotid": lotid})
            continue

        m = RE_LOTPLAN_SLASH.match(raw)
        if m:
            items.append({
                "raw": raw,
                "lot": m.group("lot"),
                "section": m.group("section"),
                "plan_type": (m.group("plan_type") or "").upper(),
                "plan_number": m.group("plan_number"),
            })
            continue

        m = RE_COMPACT.match(raw)
        if m:
            items.append({
                "raw": raw,
                "lot": m.group("lot"),
                "section": None,
                "plan_type": (m.group("plan_type") or "").upper(),
                "plan_number": m.group("plan_number"),
            })
            continue

        m = RE_VERBOSE.match(raw)
        if m:
            plan_type = "SP" if "Survey" in (m.group("plan_label") or "") else "RP"
            items.append({
                "raw": raw,
                "lot": m.group("lot"),
                "section": None,
                "plan_type": plan_type,
                "plan_number": m.group("plan_number"),
            })
            continue

        m = RE_SA_PLANPARCEL.match(raw)
        if m:
            items.append({"raw": raw, "sa_planparcel": m.group("planparcel").upper()})
            continue

        m = RE_SA_TITLEPAIR.match(raw)
        if m:
            a, b = m.group("a"), m.group("b")
            items.append({"raw": raw, "sa_titlepair": (a, b)})
            continue

        items.append({"raw": raw, "unparsed": True})
    return items

# --------------------- HTTP / ArcGIS ---------------------

def _http_get_json(url: str, params: Dict, retries: int = REQUEST_RETRIES, timeout: int = REQUEST_TIMEOUT) -> Dict:
    last_err = None
    for attempt in range(retries + 1):
        try:
            base = dict(
                f="json",
                outSR=4326,
                returnGeometry="true",
                geometryPrecision=6,
                returnExceededLimitFeatures="false",
            )
            merged = {**base, **params}
            r = requests.get(url, params=merged, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(0.5)
    raise last_err if last_err else RuntimeError("Unknown request error")

def _arcgis_query(url: str, where: str, out_fields: str = "*") -> Dict:
    params = {"where": where, "outFields": out_fields}
    data = _http_get_json(url, params)
    features = []
    for g in data.get("features", []):
        geom = g.get("geometry")
        attrs = g.get("attributes", {})
        if not geom:
            continue
        if "rings" in geom:
            coords = [ring for ring in geom["rings"]]
            geo = {"type": "Polygon", "coordinates": coords}
        elif "paths" in geom:
            geo = {"type": "MultiLineString", "coordinates": geom["paths"]}
        elif "x" in geom and "y" in geom:
            geo = {"type": "Point", "coordinates": [geom["x"], geom["y"]]}
        else:
            continue
        features.append({"type": "Feature", "geometry": geo, "properties": attrs})
    return {"type": "FeatureCollection", "features": features}

# --------------------- Fetchers ---------------------

def fetch_nsw_by_lotid(lotid: str) -> Dict:
    url = ENDPOINTS["NSW"]
    lotid_norm = _nsw_normalize_lotid(lotid)
    where = f"UPPER(lotidstring)=UPPER('{lotid_norm}')"
    return _arcgis_query(url, where)

def fetch_qld(lot: str, plan_type: str, plan_number: str) -> Dict:
    url = ENDPOINTS["QLD"]
    plan_full = f"{plan_type}{plan_number}"
    where = f"(UPPER(LOT)=UPPER('{lot}')) AND (UPPER(PLAN)=UPPER('{plan_full}'))"
    return _arcgis_query(url, where)

def fetch_sa_by_planparcel(planparcel_str: str) -> Dict:
    url = ENDPOINTS["SA"]
    where = f"UPPER(planparcel)=UPPER('{planparcel_str}')"
    return _arcgis_query(url, where)

def fetch_sa_by_title(volume: str, folio: str) -> Dict:
    url = ENDPOINTS["SA"]
    where = f"(UPPER(volume)=UPPER('{volume}')) AND (UPPER(folio)=UPPER('{folio}'))"
    return _arcgis_query(url, where)

# --------------------- Exports ---------------------

def features_to_geojson(fc: Dict) -> bytes:
    return json.dumps(fc, ensure_ascii=False).encode("utf-8")

def features_to_kml_kmz(fc: Dict, as_kmz: bool = False) -> Tuple[str, bytes]:
    if not HAVE_SIMPLEKML:
        raise RuntimeError("simplekml is not installed; cannot create KML/KMZ.")
    kml = simplekml.Kml()
    for feat in fc.get("features", []):
        props = (feat.get("properties") or {}).copy()
        name = (
            props.get("lotidstring")
            or props.get("planparcel")
            or props.get("planlabel") or props.get("PLAN_LABEL")
            or props.get("PLAN") or props.get("plan")
            or "parcel"
        )
        lines = []
        for k, v in sorted(props.items(), key=lambda kv: kv[0].lower()):
            if v not in (None, ""):
                lines.append(f"{k}: {v}")
        desc = "\n".join(lines) if lines else "No attributes"

        geom = feat.get("geometry", {}) or {}
        gtype = geom.get("type")
        if gtype == "Polygon":
            coords = []
            for ring in geom.get("coordinates", []):
                coords.append([(lng, lat) for lng, lat in ring])
            poly = kml.newpolygon(name=name, description=desc)
            if coords:
                poly.outerboundaryis = coords[0]
                if len(coords) > 1:
                    poly.innerboundaryis = coords[1:]
        elif gtype == "MultiLineString":
            for path in geom.get("coordinates", []):
                ls = kml.newlinestring(name=name, description=desc)
                ls.coords = [(lng, lat) for lng, lat in path]
        elif gtype == "LineString":
            ls = kml.newlinestring(name=name, description=desc)
            ls.coords = [(lng, lat) for lng, lat in geom.get("coordinates", [])]
        elif gtype == "Point":
            pt = kml.newpoint(name=name, description=desc)
            lng, lat = geom.get("coordinates", [None, None])[:2]
            if lng is not None and lat is not None:
                pt.coords = [(lng, lat)]
    if as_kmz:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("doc.kml", kml.kml())
        return ("application/vnd.google-earth.kmz", buf.getvalue())
    else:
        return ("application/vnd.google-earth.kml+xml", kml.kml().encode("utf-8"))

# --------------------- UI ---------------------

st.title("MappingKML — Parcel Finder")

with st.sidebar:
    st.subheader("Search scope")
    sel_qld = st.checkbox("Queensland (QLD)", value=True)
    sel_nsw = st.checkbox("New South Wales (NSW)", value=True)
    sel_sa  = st.checkbox("South Australia (SA)", value=True)

    st.markdown("**Input formats:**")
    st.markdown(
        "- **NSW (lotidstring only):** `13//DP1246224`  (also accepts `13/DP1246224`)  \n"
        "- **QLD:** `13/DP1242624`, `77//DP753955`, `3SP181800`, `Lot 3 on Survey Plan 181800`  \n"
        "- **SA:** `D10001AL12`  •  `FOLIO/VOLUME` or `VOLUME/FOLIO` (e.g., `1234/5678`)"
    )

examples = (
    "13//DP1246224  # NSW lotidstring\n"
    "13/DP1242624   # QLD\n"
    "3SP181800      # QLD\n"
    "D10001AL12     # SA planparcel\n"
    "1234/5678      # SA title\n"
)
queries_text = st.text_area("Enter one query per line", height=170, placeholder=examples)

col_a, col_b = st.columns([1,1])
with col_a:
    run_btn = st.button("Search", type="primary")
with col_b:
    clear_btn = st.button("Clear")

if clear_btn:
    st.experimental_rerun()

parsed = parse_queries(queries_text)
unparsed = [p["raw"] for p in parsed if p.get("unparsed")]
if unparsed:
    st.info("Could not parse these lines (ignored):\n- " + "\n- ".join(unparsed))

if run_btn and not (sel_qld or sel_nsw or sel_sa):
    st.warning("Please tick at least one state to search.", icon="⚠️")

accum_features: List[Dict] = []
state_warnings: List[str] = []
state_counts = {"NSW":0, "QLD":0, "SA":0}

def _add_features(fc):
    for f in (fc or {}).get("features", []):
        accum_features.append(f)

# --------------------- Run ---------------------

if run_btn and (sel_qld or sel_nsw or sel_sa):
    with st.spinner("Querying selected states..."):
        # NSW — lotidstring ONLY
        if sel_nsw:
            for p in parsed:
                if p.get("unparsed"): 
                    continue
                if "nsw_lotid" in p:
                    lotid = _nsw_normalize_lotid(p["nsw_lotid"])
                    # Show what we send (helps diagnose)
                    st.caption(f"NSW where: UPPER(lotidstring)=UPPER('{lotid}')")
                    try:
                        fc = fetch_nsw_by_lotid(lotid)
                    except requests.exceptions.Timeout:
                        state_warnings.append("NSW request timed out (12s). Try again.")
                        fc = {"type": "FeatureCollection", "features": []}
                    c = len(fc.get("features", []))
                    state_counts["NSW"] += c
                    if c == 0:
                        state_warnings.append(f"NSW: No parcels for lotidstring '{lotid}'.")
                    _add_features(fc)

        # QLD
        if sel_qld:
            for p in parsed:
                if p.get("unparsed") or "nsw_lotid" in p or p.get("sa_planparcel") or p.get("sa_titlepair"):
                    continue
                pt = (p.get("plan_type") or "").upper()
                if pt:
                    try:
                        fc = fetch_qld(
                            lot=p.get("lot"),
                            plan_type=pt,
                            plan_number=p.get("plan_number")
                        )
                    except requests.exceptions.Timeout:
                        state_warnings.append("QLD request timed out (12s).")
                        fc = {"type": "FeatureCollection", "features": []}
                    c = len(fc.get("features", []))
                    state_counts["QLD"] += c
                    if c == 0:
                        state_warnings.append(f"QLD: No parcels for lot '{p.get('lot')}', plan '{pt}{p.get('plan_number')}'.")
                    _add_features(fc)

        # SA
        if sel_sa:
            for p in parsed:
                if p.get("unparsed"):
                    continue
                try:
                    if "sa_planparcel" in p:
                        fc = fetch_sa_by_planparcel(p["sa_planparcel"])
                        c = len(fc.get("features", []))
                        state_counts["SA"] += c
                        if c == 0:
                            state_warnings.append(f"SA: No parcels for planparcel '{p['sa_planparcel']}'.")
                        _add_features(fc)
                        continue
                    if "sa_titlepair" in p:
                        a, b = p["sa_titlepair"]
                        fc1 = fetch_sa_by_title(volume=a, folio=b)
                        fc2 = fetch_sa_by_title(volume=b, folio=a)
                        seen = set(); merged = {"type":"FeatureCollection","features":[]}
                        for fc_try in (fc1, fc2):
                            for feat in fc_try.get("features", []):
                                pid = (feat.get("properties") or {}).get("parcel_id") or json.dumps(
                                    feat.get("geometry", {}), sort_keys=True
                                )
                                if pid in seen: 
                                    continue
                                seen.add(pid)
                                merged["features"].append(feat)
                        c = len(merged["features"]); state_counts["SA"] += c
                        if c == 0:
                            state_warnings.append(f"SA: No parcels for title inputs '{a}/{b}'. (Tried both volume/folio and folio/volume.)")
                        _add_features(merged)
                except requests.exceptions.Timeout:
                    state_warnings.append("SA request timed out (12s).")

# --------------------- Map ---------------------

fc_all = {"type": "FeatureCollection", "features": accum_features}

if state_warnings:
    for w in state_warnings:
        st.warning(w, icon="⚠️")

if run_btn and (sel_qld or sel_nsw or sel_sa):
    st.success(f"Found — NSW: {state_counts['NSW']}  |  QLD: {state_counts['QLD']}  |  SA: {state_counts['SA']}")

layers = []
if accum_features:
    layers.append(
        pdk.Layer(
            "GeoJsonLayer",
            fc_all,
            pickable=True,
            stroked=True,
            filled=True,
            wireframe=True,
            get_line_width=2,
        )
    )

tooltip_html = """
<div style="font-family:Arial,sans-serif;">
  <b>{planlabel}</b><br/>
  LotID: <b>{lotidstring}</b><br/>
  Lot {lotnumber}{sectionnumber}<br/>
  SA Title: Vol {volume} / Fol {folio}
</div>
"""

view_state = _fit_view(fc_all if accum_features else None)
deck = pdk.Deck(layers=layers, initial_view_state=view_state, map_style=None, tooltip={"html": tooltip_html})
st.pydeck_chart(deck, use_container_width=True)

# --------------------- Downloads ---------------------

st.subheader("Downloads")
c1, c2, c3 = st.columns(3)

with c1:
    if accum_features:
        st.download_button("⬇️ GeoJSON", data=features_to_geojson(fc_all),
                           file_name="parcels.geojson", mime="application/geo+json")
    else:
        st.caption("No features yet.")

with c2:
    if HAVE_SIMPLEKML and accum_features:
        mime, kml_data = features_to_kml_kmz(fc_all, as_kmz=False)
        st.download_button("⬇️ KML", data=kml_data,
                           file_name="parcels.kml", mime=mime)
    elif not HAVE_SIMPLEKML:
        st.caption("Install `simplekml` for KML/KMZ: pip install simplekml")
    else:
        st.caption("No features yet.")

with c3:
    if HAVE_SIMPLEKML and accum_features:
        mime, kmz_data = features_to_kml_kmz(fc_all, as_kmz=True)
        st.download_button("⬇️ KMZ", data=kmz_data,
                           file_name="parcels.kmz", mime="application/vnd.google-earth.kmz")
    else:
        st.caption(" ")