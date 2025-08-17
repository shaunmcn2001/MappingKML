# app.py — MappingKML (NSW via lotidstring on layer 9; QLD + SA supported)
# --------------------------------------------------------------------------------------
# NSW: input lotidstring like 13//DP1246224 (we query layer 9 field 'lotidstring' directly)
# QLD: lot/plan formats (13/DP1242624, 77//DP753955, 3SP181800, Lot 3 on Survey Plan 181800)
# SA:  planparcel like D10001AL12  OR title search "folio/volume" or "volume/folio" (e.g., 1234/5678)
# Exports: GeoJSON / KML / KMZ — KML balloons list ALL attributes
# --------------------------------------------------------------------------------------

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

# Optional: enable KML/KMZ export if simplekml is installed
try:
    import simplekml
    HAVE_SIMPLEKML = True
except Exception:
    HAVE_SIMPLEKML = False

# -------------------------- App config --------------------------

st.set_page_config(page_title="MappingKML", layout="wide")

ENDPOINTS = {
    # QLD Cadastre (adjust if your layer differs)
    "QLD": "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Cadastre/LandParcels/MapServer/0/query",

    # NSW Cadastre — use MapServer/9 (Lot layer); we query by 'lotidstring'
    "NSW": "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_Cadastre/MapServer/9/query",

    # SA Cadastre (Reference_WFL1 FeatureServer, Layer 1)
    "SA":  "https://dpti.geohub.sa.gov.au/server/rest/services/Hosted/Reference_WFL1/FeatureServer/1/query",
}

DEFAULT_VIEW = pdk.ViewState(latitude=-32.0, longitude=147.0, zoom=5.0, pitch=0, bearing=0)

# Plan-type hints (used only for rookie warnings)
NSW_PLAN_TYPES = {"DP", "SP", "SC", "CP", "DPX"}
QLD_PLAN_TYPES = {"SP", "RP", "CP", "BUP", "GTP", "PC", "SL", "CSP"}

# -------------------------- Geo helpers --------------------------

def _as_feature_collection(fc_like):
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
        if lng < min_lng: min_lng = lng
        if lng > max_lng: max_lng = lng
        if lat < min_lat: min_lat = lat
        if lat > max_lat: max_lat = lat
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
    if extent > 20:      zoom = 5
    elif extent > 10:    zoom = 6
    elif extent > 5:     zoom = 7
    elif extent > 2:     zoom = 8
    elif extent > 1:     zoom = 9
    elif extent > 0.5:   zoom = 10
    elif extent > 0.25:  zoom = 11
    elif extent > 0.1:   zoom = 12
    else:                zoom = 13
    return pdk.ViewState(latitude=center_lat, longitude=center_lng, zoom=zoom)

def _fit_view(fc_like, warn_if_empty=True):
    fc = _as_feature_collection(fc_like)
    if not fc or not fc.get("features"):
        if warn_if_empty:
            st.warning("No features to display. Showing default view.", icon="⚠️")
        return DEFAULT_VIEW
    bbox = None
    for feat in fc["features"]:
        b = _geom_bbox((feat or {}).get("geometry") or {})
        bbox = _merge_bbox(bbox, b)
    if bbox is None:
        if warn_if_empty:
            st.warning("Features had no valid coordinates. Showing default view.", icon="⚠️")
        return DEFAULT_VIEW
    return _bbox_to_viewstate(bbox, padding_ratio=0.12)

# -------------------------- Input parsing (SAFE) --------------------------

# NSW: lotidstring like 13//DP1246224
RE_NSW_LOTID = re.compile(r"^\s*(?P<lotid>\d+//[A-Za-z]{1,6}\d+)\s*$")

# QLD/NSW generic styles (still supported for QLD; NSW prefers lotidstring)
RE_LOTPLAN_SLASH = re.compile(
    r"^\s*(?P<lot>\d+)\s*(?:/(?P<section>\d+))?\s*/\s*(?P<plan_type>[A-Za-z]{1,6})\s*(?P<plan_number>\d+)\s*$"
)
RE_COMPACT = re.compile(
    r"^\s*(?P<lot>\d+)\s*(?P<plan_type>[A-Za-z]{1,6})\s*(?P<plan_number>\d+)\s*$"
)
RE_VERBOSE = re.compile(
    r"^\s*Lot\s+(?P<lot>\d+)\s+on\s+(?P<plan_label>(Registered|Survey)\s+Plan)\s+(?P<plan_number>\d+)\s*$",
    re.IGNORECASE
)

# SA patterns
RE_SA_PLANPARCEL = re.compile(r"^\s*(?P<planparcel>[A-Za-z]{1,2}\d+[A-Za-z]{1,2}\d+)\s*$")
RE_SA_TITLEPAIR  = re.compile(r"^\s*(?P<a>\d{1,6})\s*/\s*(?P<b>\d{1,6})\s*$")

def parse_queries(multiline: str) -> List[Dict]:
    """
    Return a list of parsed query dicts.
    - NSW: prefer nsw_lotid (13//DP1246224)
    - QLD: lot/plan formats
    - SA:  planparcel OR title pair (folio/volume) any order
    """
    items: List[Dict] = []
    lines = [x.strip() for x in (multiline or "").splitlines() if x.strip()]

    for raw in lines:
        # NSW lotidstring (do this first so NSW users don't need special syntax)
        m = RE_NSW_LOTID.match(raw)
        if m:
            items.append({"raw": raw, "nsw_lotid": m.group("lotid").upper()})
            continue

        # QLD/NSW like 13/1/DP1242624 or 13/DP1242624 (for QLD)
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

        # Compact like 3SP181800 (QLD)
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

        # Verbose like "Lot 3 on Survey Plan 181800" (QLD)
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

        # SA planparcel, e.g., D10001AL12
        m = RE_SA_PLANPARCEL.match(raw)
        if m:
            items.append({"raw": raw, "sa_planparcel": m.group("planparcel").upper()})
            continue

        # SA title pair (folio/volume OR volume/folio), e.g., 1234/5678
        m = RE_SA_TITLEPAIR.match(raw)
        if m:
            a, b = m.group("a"), m.group("b")
            items.append({"raw": raw, "sa_titlepair": (a, b)})
            continue

        # Nothing matched
        items.append({"raw": raw, "unparsed": True})
    return items

# -------------------------- HTTP (retry) --------------------------

def _http_get_json(url: str, params: Dict, retries: int = 2, timeout: int = 25) -> Dict:
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(0.6 * (attempt + 1))
    raise last_err if last_err else RuntimeError("Unknown request error")

def _arcgis_query(url: str, where: str, out_fields: str = "*") -> Dict:
    params = {
        "f": "json",
        "where": where,
        "outFields": out_fields,
        "outSR": 4326,
        "returnGeometry": "true",
    }
    data = _http_get_json(url, params)
    features = []
    for g in data.get("features", []):
        geom = g.get("geometry")
        attrs = g.get("attributes", {})
        if not geom:
            continue
        if "rings" in geom:
            coords = []
            for ring in geom["rings"]:
                coords.append(ring)
            geo = {"type": "Polygon", "coordinates": coords}
        elif "paths" in geom:
            geo = {"type": "MultiLineString", "coordinates": geom["paths"]}
        elif "x" in geom and "y" in geom:
            geo = {"type": "Point", "coordinates": [geom["x"], geom["y"]]}
        else:
            continue
        features.append({"type": "Feature", "geometry": geo, "properties": attrs})
    return {"type": "FeatureCollection", "features": features}

# -------------------------- NSW helpers --------------------------

def _nsw_normalize_lotid(raw: str) -> str:
    """
    Accepts '13//DP1246224', '13/DP1246224', '13 / DP1246224' and returns '13//DP1246224'.
    """
    s = (raw or "").strip().upper().replace(" ", "")
    if "//" in s:
        return s
    m = re.match(r"^(?P<lot>\d+)/(?P<plan>[A-Z]{1,6}\d+)$", s)
    return f"{m.group('lot')}//{m.group('plan')}" if m else s

def fetch_nsw_by_lotid(lotid: str) -> Dict:
    """
    NSW robust search against layer 9:
      1) lotidstring exact (lowercase field)
      2) lotnumber + planlabel (lowercase fields)
      3) planlabel only
    """
    url = ENDPOINTS["NSW"]
    lotid_norm = _nsw_normalize_lotid(lotid)

    # split for fallbacks
    lot, planlabel = None, None
    m = re.match(r"^(?P<lot>\d+)//(?P<plan>[A-Z]{1,6}\d+)$", lotid_norm)
    if m:
        lot, planlabel = m.group("lot"), m.group("plan")

    # 1) lotidstring exact
    fc1 = _arcgis_query(url, f"UPPER(lotidstring)=UPPER('{lotid_norm}')")
    if fc1.get("features"):
        for f in fc1["features"]:
            (f.get("properties") or {}).update({"_match": "NSW lotidstring exact"})
        return fc1

    # 2) lotnumber + planlabel
    if lot and planlabel:
        fc2 = _arcgis_query(
            url,
            f"(UPPER(lotnumber)=UPPER('{lot}')) AND (UPPER(planlabel)=UPPER('{planlabel}'))"
        )
        if fc2.get("features"):
            for f in fc2["features"]:
                (f.get("properties") or {}).update({"_match": "NSW lotnumber+planlabel"})
            return fc2

    # 3) planlabel only (still shows plan if lot was wrong)
    if planlabel:
        fc3 = _arcgis_query(url, f"UPPER(planlabel)=UPPER('{planlabel}')")
        for f in fc3.get("features", []):
            (f.get("properties") or {}).update({"_match": "NSW plan only"})
        return fc3

    # Nothing matched
    return {"type": "FeatureCollection", "features": []}

# -------------------------- Per-state fetchers --------------------------

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

# -------------------------- Exports --------------------------

def features_to_geojson(fc: Dict) -> bytes:
    return json.dumps(fc, ensure_ascii=False).encode("utf-8")

def features_to_kml_kmz(fc: Dict, as_kmz: bool = False) -> Tuple[str, bytes]:
    """
    Build KML/KMZ with ALL attributes in the description so Google Earth balloons show everything.
    """
    if not HAVE_SIMPLEKML:
        raise RuntimeError("simplekml is not installed; cannot create KML/KMZ.")
    kml = simplekml.Kml()
    for feat in fc.get("features", []):
        props = (feat.get("properties") or {}).copy()

        # Friendly name: prefer NSW/SA identifiers, else plan labels
        name = (
            props.get("lotidstring")
            or props.get("planparcel")
            or props.get("planlabel") or props.get("PLAN_LABEL")
            or props.get("PLAN") or props.get("plan")
            or "parcel"
        )

        # Verbose description listing *all* attributes
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

# -------------------------- UI --------------------------

st.title("MappingKML — Parcel Finder")

with st.sidebar:
    st.subheader("Search scope")
    sel_qld = st.checkbox("Queensland (QLD)", value=True)
    sel_nsw = st.checkbox("New South Wales (NSW)", value=True)
    sel_sa  = st.checkbox("South Australia (SA)", value=True)

    st.markdown("**Supported input formats:**")
    st.markdown(
        "- **NSW (lotidstring):** `13//DP1246224`  \n"
        "- **QLD:** `13/DP1242624`, `77//DP753955`, `3SP181800`, `Lot 3 on Survey Plan 181800`  \n"
        "- **SA:** `D10001AL12` (planparcel)  •  `FOLIO/VOLUME` or `VOLUME/FOLIO` (e.g., `1234/5678`)"
    )

MAX_LINES = 200
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
if len(parsed) > MAX_LINES:
    st.error(f"Too many lines ({len(parsed)}). Please keep it under {MAX_LINES}.")
    st.stop()

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

def _warn_wrong_state(pt: str, state: str) -> Optional[str]:
    if state == "NSW" and pt and pt not in NSW_PLAN_TYPES:
        return f"Input has plan type '{pt}', which is unusual for NSW. Double-check."
    if state == "QLD" and pt and pt not in QLD_PLAN_TYPES:
        return f"Input has plan type '{pt}', which is unusual for QLD. Double-check."
    return None

if run_btn and (sel_qld or sel_nsw or sel_sa):
    with st.spinner("Querying selected states..."):
        # NSW (lotidstring direct on layer 9)
        if sel_nsw:
            for p in parsed:
                if p.get("unparsed"):
                    continue
                try:
                    if "nsw_lotid" in p:
                        lotid = _nsw_normalize_lotid(p["nsw_lotid"])
                        fc = fetch_nsw_by_lotid(lotid)
                        c = len(fc.get("features", []))
                        state_counts["NSW"] += c
                        if c == 0:
                            state_warnings.append(f"NSW: No parcels for lotidstring '{lotid}'.")
                        _add_features(fc)
                        continue
                except Exception as e:
                    state_warnings.append(f"NSW error for {p.get('raw')}: {e}")

        # QLD
        if sel_qld:
            for p in parsed:
                if p.get("unparsed") or "nsw_lotid" in p or p.get("sa_planparcel") or p.get("sa_titlepair"):
                    continue
                pt = (p.get("plan_type") or "").upper()
                warn = _warn_wrong_state(pt, "QLD")
                if warn: state_warnings.append("QLD: " + warn)
                if pt:
                    try:
                        fc = fetch_qld(
                            lot=p.get("lot"),
                            plan_type=pt,
                            plan_number=p.get("plan_number")
                        )
                        c = len(fc.get("features", []))
                        state_counts["QLD"] += c
                        if c == 0:
                            state_warnings.append(f"QLD: No parcels for lot '{p.get('lot')}', plan '{pt}{p.get('plan_number')}'.")
                        _add_features(fc)
                    except Exception as e:
                        state_warnings.append(f"QLD error for {p.get('raw')}: {e}")

        # SA
        if sel_sa:
            for p in parsed:
                if p.get("unparsed"):
                    continue
                try:
                    # Planparcel search
                    if "sa_planparcel" in p:
                        fc = fetch_sa_by_planparcel(p["sa_planparcel"])
                        c = len(fc.get("features", []))
                        state_counts["SA"] += c
                        if c == 0:
                            state_warnings.append(f"SA: No parcels for planparcel '{p['sa_planparcel']}'.")
                        _add_features(fc)
                        continue

                    # Title search (folio/volume OR volume/folio) — try both orders, union results.
                    if "sa_titlepair" in p:
                        a, b = p["sa_titlepair"]
                        fc1 = fetch_sa_by_title(volume=a, folio=b)  # a=volume, b=folio
                        fc2 = fetch_sa_by_title(volume=b, folio=a)  # b=volume, a=folio
                        # Merge (avoid duplicates)
                        seen = set()
                        merged = {"type": "FeatureCollection", "features": []}
                        for fc_try in (fc1, fc2):
                            for feat in fc_try.get("features", []):
                                pid = (feat.get("properties") or {}).get("parcel_id") or json.dumps(
                                    feat.get("geometry", {}), sort_keys=True
                                )
                                if pid in seen:
                                    continue
                                seen.add(pid)
                                merged["features"].append(feat)
                        c = len(merged["features"])
                        state_counts["SA"] += c
                        if c == 0:
                            state_warnings.append(f"SA: No parcels for title inputs '{a}/{b}'. (Tried both volume/folio and folio/volume.)")
                        _add_features(merged)
                        continue
                except Exception as e:
                    state_warnings.append(f"SA error for {p.get('raw')}: {e}")

# Build a FeatureCollection for the map
fc_all = {"type": "FeatureCollection", "features": accum_features}

# Warnings + summary
if state_warnings:
    for w in state_warnings:
        st.warning(w, icon="⚠️")

if run_btn and (sel_qld or sel_nsw or sel_sa):
    summary = f"Found — NSW: {state_counts['NSW']}  |  QLD: {state_counts['QLD']}  |  SA: {state_counts['SA']}"
    st.success(summary)

# Map
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

# Tooltip: NSW uses lowercase keys (lotidstring, planlabel, lotnumber, sectionnumber)
tooltip_html = """
<div style="font-family:Arial,sans-serif;">
  <b>{planlabel}</b><br/>
  LotID: <b>{lotidstring}</b><br/>
  Lot {lotnumber}{sectionnumber}<br/>
  SA Title: Vol {volume} / Fol {folio}<br/>
  <small style="opacity:.7;">{_match}</small>
</div>
"""

view_state = _fit_view(fc_all if accum_features else None)
deck = pdk.Deck(
    layers=layers,
    initial_view_state=view_state,
    map_style=None,
    tooltip={"html": tooltip_html}
)
st.pydeck_chart(deck, use_container_width=True)

# Downloads
st.subheader("Downloads")
col1, col2, col3 = st.columns(3)

with col1:
    if accum_features:
        st.download_button("⬇️ GeoJSON", data=features_to_geojson(fc_all),
                           file_name="parcels.geojson", mime="application/geo+json")
    else:
        st.caption("No features yet.")

with col2:
    if HAVE_SIMPLEKML and accum_features:
        mime, kml_data = features_to_kml_kmz(fc_all, as_kmz=False)
        st.download_button("⬇️ KML", data=kml_data,
                           file_name="parcels.kml", mime=mime)
    elif not HAVE_SIMPLEKML:
        st.caption("Install `simplekml` for KML/KMZ:  pip install simplekml")
    else:
        st.caption("No features yet.")

with col3:
    if HAVE_SIMPLEKML and accum_features:
        mime, kmz_data = features_to_kml_kmz(fc_all, as_kmz=True)
        st.download_button("⬇️ KMZ", data=kmz_data,
                           file_name="parcels.kmz", mime="application/vnd.google-earth.kmz")
    else:
        st.caption(" ")