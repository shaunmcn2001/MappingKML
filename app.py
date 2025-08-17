# app.py — MappingKML (QLD + NSW + SA, safe full version)
# --------------------------------------------------------------------------------------
# ✅ Tick QLD / NSW / SA (each uses its own ArcGIS endpoint)
# ✅ NSW/QLD: 13/DP1242624, 13/1/DP1242624, 77//DP753955, 3SP181800,
#            "Lot 3 on Survey Plan 181800", "Lot 1 on Registered Plan 164839"
# ✅ SA: planparcel like D10001AL12  +  title search "folio/volume" or "volume/folio" (e.g., 1234/5678)
# ✅ Defensive parsing (no NameError), retries, safe map fit (no crashes), GeoJSON/KML/KMZ downloads
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

# ArcGIS REST endpoints
ENDPOINTS = {
    "QLD": "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Cadastre/LandParcels/MapServer/0/query",
    "NSW": "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_Cadastre/MapServer/0/query",
    "SA":  "https://dpti.geohub.sa.gov.au/server/rest/services/Hosted/Reference_WFL1/FeatureServer/1/query",
}

# Default east-AU view fallback
DEFAULT_VIEW = pdk.ViewState(latitude=-32.0, longitude=147.0, zoom=5.0, pitch=0, bearing=0)

# Per-state accepted plan types (helps catch mismatches)
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

RE_SA_PLANPARCEL = re.compile(r"^\s*(?P<planparcel>[A-Za-z]{1,2}\d+[A-Za-z]{1,2}\d+)\s*$")
RE_SA_TITLEPAIR  = re.compile(r"^\s*(?P<a>\d{1,6})\s*/\s*(?P<b>\d{1,6})\s*$")

def parse_queries(multiline: str) -> List[Dict]:
    """Never references undefined variables; unparsed lines are marked safely."""
    items: List[Dict] = []
    lines = [x.strip() for x in (multiline or "").splitlines() if x.strip()]

    for raw in lines:
        # 1) NSW like 13/1/DP1242624 or 13/DP1242624
        m = RE_LOTPLAN_SLASH.match(raw)
        if m:
            lot       = m.group("lot")
            section   = m.group("section")
            plan_type = (m.group("plan_type") or "").upper()
            plan_num  = m.group("plan_number")
            items.append({
                "raw": raw, "lot": lot, "section": section,
                "plan_type": plan_type, "plan_number": plan_num
            })
            continue

        # 2) Compact like 3SP181800
        m = RE_COMPACT.match(raw)
        if m:
            lot       = m.group("lot")
            plan_type = (m.group("plan_type") or "").upper()
            plan_num  = m.group("plan_number")
            items.append({
                "raw": raw, "lot": lot, "section": None,
                "plan_type": plan_type, "plan_number": plan_num
            })
            continue

        # 3) Verbose like "Lot 3 on Survey Plan 181800"
        m = RE_VERBOSE.match(raw)
        if m:
            lot       = m.group("lot")
            plan_lbl  = m.group("plan_label") or ""
            plan_num  = m.group("plan_number")
            plan_type = "SP" if "Survey" in plan_lbl else "RP"
            items.append({
                "raw": raw, "lot": lot, "section": None,
                "plan_type": plan_type, "plan_number": plan_num
            })
            continue

        # 4) SA planparcel, e.g., D10001AL12
        m = RE_SA_PLANPARCEL.match(raw)
        if m:
            items.append({"raw": raw, "sa_planparcel": m.group("planparcel").upper()})
            continue

        # 5) SA title pair (folio/volume OR volume/folio), e.g., 1234/5678
        m = RE_SA_TITLEPAIR.match(raw)
        if m:
            a, b = m.group("a"), m.group("b")
            items.append({"raw": raw, "sa_titlepair": (a, b)})
            continue

        # 6) NSW variant “77//DP753955” (explicit missing section)
        if "//" in raw:
            try:
                left, right = raw.split("//", 1)
                lot = left.strip()
                m2 = re.match(r"^([A-Za-z]{1,6})\s*(\d+)$", right.strip())
                if lot and m2:
                    plan_type = m2.group(1).upper()
                    plan_num  = m2.group(2)
                    items.append({
                        "raw": raw, "lot": lot, "section": None,
                        "plan_type": plan_type, "plan_number": plan_num
                    })
                    continue
            except Exception:
                pass

        # 7) Nothing matched
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

# -------------------------- Per-state fetchers --------------------------

def fetch_qld(lot: str, plan_type: str, plan_number: str) -> Dict:
    url = ENDPOINTS["QLD"]
    plan_full = f"{plan_type}{plan_number}"
    where = f"(UPPER(LOT)=UPPER('{lot}')) AND (UPPER(PLAN)=UPPER('{plan_full}'))"
    return _arcgis_query(url, where)

def fetch_nsw(lot: str, plan_type: str, plan_number: str, section: Optional[str] = None) -> Dict:
    url = ENDPOINTS["NSW"]
    plan_full = f"{plan_type}{plan_number}"
    if section:
        where = (
            f"(UPPER(LOT_NUMBER)=UPPER('{lot}')) AND (UPPER(SECTION_NUMBER)=UPPER('{section}')) "
            f"AND (UPPER(PLAN_LABEL)=UPPER('{plan_full}'))"
        )
    else:
        where = f"(UPPER(LOT_NUMBER)=UPPER('{lot}')) AND (UPPER(PLAN_LABEL)=UPPER('{plan_full}'))"
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
    if not HAVE_SIMPLEKML:
        raise RuntimeError("simplekml is not installed; cannot create KML/KMZ.")
    kml = simplekml.Kml()
    for feat in fc.get("features", []):
        props = feat.get("properties", {}) or {}
        geom = feat.get("geometry", {}) or {}
        gtype = geom.get("type")
        name = (
            props.get("PLAN_LABEL")
            or props.get("PLAN")
            or props.get("planparcel")
            or "parcel"
        )
        desc = "\n".join([f"{k}: {v}" for k, v in props.items() if v not in (None, "")])
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
        "- **NSW/QLD:** `13/DP1242624`, `13/1/DP1242624`, `77//DP753955`, `3SP181800`, "
        "`Lot 3 on Survey Plan 181800`, `Lot 1 on Registered Plan 164839`  \n"
        "- **SA planparcel:** `D10001AL12`  \n"
        "- **SA title:** `FOLIO/VOLUME` or `VOLUME/FOLIO` (e.g., `1234/5678`)"
    )

MAX_LINES = 200
examples = (
    "13/DP1242624\n13/1/DP1242624\n77//DP753955\n3SP181800\n"
    "Lot 3 on Survey Plan 181800\nD10001AL12\n1234/5678"
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
        # NSW
        if sel_nsw:
            for p in parsed:
                if p.get("unparsed") or p.get("sa_planparcel") or p.get("sa_titlepair"):
                    continue
                pt = (p.get("plan_type") or "").upper()
                warn = _warn_wrong_state(pt, "NSW")
                if warn: state_warnings.append("NSW: " + warn)
                if pt:
                    try:
                        fc = fetch_nsw(
                            lot=p.get("lot"),
                            plan_type=pt,
                            plan_number=p.get("plan_number"),
                            section=p.get("section")
                        )
                        c = len(fc.get("features", []))
                        state_counts["NSW"] += c
                        if c == 0:
                            msg = f"NSW: No parcels for lot '{p.get('lot')}'"
                            if p.get("section"): msg += f", section '{p.get('section')}'"
                            msg += f", plan '{pt}{p.get('plan_number')}'."
                            state_warnings.append(msg)
                        _add_features(fc)
                    except Exception as e:
                        state_warnings.append(f"NSW error for {p.get('raw')}: {e}")

        # QLD
        if sel_qld:
            for p in parsed:
                if p.get("unparsed") or p.get("sa_planparcel") or p.get("sa_titlepair"):
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
                        fc1 = fetch_sa_by_title(volume=a, folio=b)  # assume a=volume, b=folio
                        fc2 = fetch_sa_by_title(volume=b, folio=a)  # and b=volume, a=folio
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
                            state_warnings.append(
                                f"SA: No parcels for title inputs '{a}/{b}'. (Tried both volume/folio and folio/volume.)"
                            )
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

tooltip_html = """
<div style="font-family:Arial,sans-serif;">
  <b>{PLAN_LABEL}</b><br/>
  Lot {LOT_NUMBER} | Plan {PLAN}<br/>
  <i>{planparcel}</i><br/>
  SA Title: Vol {volume} / Fol {folio}
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