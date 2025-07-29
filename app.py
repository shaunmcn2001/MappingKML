# app.py
import os
import json
import math
import requests
import pandas as pd
import streamlit as st
from keplergl import KeplerGl

# Prefer streamlit-keplergl if available (better sizing); else render raw HTML
try:
    from streamlit_keplergl import keplergl_static
    HAS_ST_KEPLER = True
except Exception:
    from streamlit.components.v1 import html as st_html
    HAS_ST_KEPLER = False

# -------------------------
# Config & constants
# -------------------------
st.set_page_config(page_title="MappingKML • Kepler", layout="wide", initial_sidebar_state="collapsed")

API_BASE = os.getenv("API_BASE", "").rstrip("/")
MAP_STYLE = os.getenv("MAP_STYLE", "dark")  # Kepler styleType: dark|light|satellite|outdoors
NAV_HEIGHT = 56  # px
MAP_HEIGHT = int(os.getenv("MAP_HEIGHT", "900"))  # pixel height for Kepler map

# -------------------------
# Global styles (full-bleed + sticky top nav)
# -------------------------
st.markdown(
    f"""
<style>
/* Remove Streamlit default padding */
.appview-container .main .block-container {{
  padding-top: 0rem; padding-left: 0rem; padding-right: 0rem; padding-bottom: 0rem;
}}
/* Top bar */
#topbar {{
  position: sticky; top: 0; z-index: 999;
  height: {NAV_HEIGHT}px; background: #111; color: #fff;
  display: flex; align-items: center; gap: 12px; padding: 0 14px;
  border-bottom: 1px solid #222;
}}
#topbar .title {{ font-weight: 600; letter-spacing: .2px; }}
#mapwrap {{ height: calc(100vh - {NAV_HEIGHT}px); }}
.block-container .stButton > button {{ border-radius: 8px; }}
</style>
""",
    unsafe_allow_html=True,
)

# -------------------------
# Helpers
# -------------------------
def features_to_rows(fc: dict) -> pd.DataFrame:
    """Flatten FeatureCollection -> DataFrame with a 'geometry' column containing the geometry dict."""
    feats = fc.get("features") or []
    rows = []
    for f in feats:
        props = (f.get("properties") or {}).copy()
        props["geometry"] = f.get("geometry")
        rows.append(props)
    return pd.DataFrame(rows)

def _extract_coords(geom: dict):
    """Return flat list of [lon, lat] pairs from any GeoJSON geometry."""
    coords_out = []
    def rec(node):
        if isinstance(node, (list, tuple)) and node and isinstance(node[0], (int, float)):
            coords_out.append(node[:2])
        elif isinstance(node, (list, tuple)):
            for child in node:
                rec(child)
    if geom:
        rec(geom.get("coordinates"))
    return coords_out

def featurecollection_bbox(fc: dict):
    """Compute [minx, miny, maxx, maxy] from FeatureCollection; if empty, default near Australia center."""
    minx = miny = math.inf
    maxx = maxy = -math.inf
    for f in fc.get("features", []):
        for (x, y) in _extract_coords(f.get("geometry") or {}):
            minx = min(minx, x); miny = min(miny, y)
            maxx = max(maxx, x); maxy = max(maxy, y)
    if math.isinf(minx):
        return [133.0, -26.0, 134.5, -24.5]
    return [minx, miny, maxx, maxy]

def center_from_bbox(b):  # (lon, lat)
    return ((b[0]+b[2])/2.0, (b[1]+b[3])/2.0)

def build_kepler_config(data_id: str, df: pd.DataFrame | None, fc: dict | None):
    """Build a Kepler config with a GeoJSON layer bound to 'geometry' column, auto-center to data bbox."""
    if fc:
        b = featurecollection_bbox(fc)
        lon, lat = center_from_bbox(b)
        # crude zoom heuristic based on bbox size
        lon_span, lat_span = (b[2]-b[0]), (b[3]-b[1])
        span = max(lon_span, lat_span)
        zoom = 9 if span > 0.3 else 12 if span > 0.08 else 13.5
    else:
        lon, lat, zoom = 153.0260, -27.4705, 9  # Brisbane default

    # Tooltip: show all non-geometry columns if we have data
    fields = []
    if isinstance(df, pd.DataFrame):
        for c in df.columns:
            if c != "geometry":
                fields.append({"name": c, "format": None})

    config = {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [],
                "layers": [
                    {
                        "id": "parcels",
                        "type": "geojson",
                        "config": {
                            "dataId": data_id,
                            "label": "Parcels",
                            "color": [18, 92, 255],
                            "columns": {"geojson": "geometry"},
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.6, "strokeOpacity": 0.9,
                                "thickness": 1.2, "stroked": True, "filled": True,
                                "enable3d": False, "wireframe": False
                            },
                        },
                        "visualChannels": {}
                    }
                ],
                "interactionConfig": {
                    "tooltip": {"enabled": True, "fieldsToShow": {data_id: fields}}
                },
                "layerBlending": "normal",
                "animationConfig": {"enabled": False}
            },
            "mapState": {
                "bearing": 0, "dragRotate": False,
                "latitude": lat, "longitude": lon,
                "pitch": 0, "zoom": zoom
            },
            "mapStyle": {"styleType": MAP_STYLE}
        }
    }
    return config

def render_kepler(df: pd.DataFrame | None, fc: dict | None):
    """Render KeplerGl in Streamlit."""
    data_id = "query_results"
    kg = KeplerGl(
        height=MAP_HEIGHT,
        data={data_id: df if df is not None else pd.DataFrame()},
        config=build_kepler_config(data_id, df, fc)
    )
    if HAS_ST_KEPLER:
        keplergl_static(kg)
    else:
        st_html(kg._repr_html_(), height=MAP_HEIGHT, scrolling=False)

# --- KML export (lightweight) ---
def fc_to_kml_bytes(fc: dict) -> bytes:
    """
    Convert a FeatureCollection to a simple KML (Polygon/MultiPolygon/Point/LineString).
    Uses simplekml for minimal dependencies.
    """
    try:
        import simplekml
    except Exception as e:
        raise RuntimeError("simplekml not installed. Add 'simplekml' to requirements.txt") from e

    kml = simplekml.Kml()
    feats = fc.get("features", [])
    for i, f in enumerate(feats, 1):
        geom = (f or {}).get("geometry") or {}
        gtype = geom.get("type")
        props = (f or {}).get("properties") or {}
        name = str(props.get("name") or props.get("LOTPLAN") or props.get("lot_plan") or f"feature_{i}")

        def ring_to_coords(ring):
            # ring: [[lon,lat], ...]
            return [(pt[0], pt[1]) for pt in ring if isinstance(pt, (list, tuple)) and len(pt) >= 2]

        if gtype == "Polygon":
            for idx, ring in enumerate(geom.get("coordinates", [])):
                poly = kml.newpolygon(name=name if idx == 0 else f"{name}_{idx}")
                poly.outerboundaryis = ring_to_coords(ring)
                poly.style.linestyle.width = 1
                poly.style.polystyle.fill = 1
        elif gtype == "MultiPolygon":
            for pidx, polycoords in enumerate(geom.get("coordinates", []), 1):
                for ridx, ring in enumerate(polycoords):
                    poly = kml.newpolygon(name=f"{name}_{pidx}_{ridx}")
                    poly.outerboundaryis = ring_to_coords(ring)
                    poly.style.linestyle.width = 1
                    poly.style.polystyle.fill = 1
        elif gtype == "Point":
            coords = geom.get("coordinates", [])
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                kml.newpoint(name=name, coords=[(coords[0], coords[1])])
        elif gtype == "LineString":
            line = kml.newlinestring(name=name)
            line.coords = ring_to_coords(geom.get("coordinates", []))
            line.style.linestyle.width = 2
        elif gtype == "MultiLineString":
            for lidx, line_coords in enumerate(geom.get("coordinates", []), 1):
                line = kml.newlinestring(name=f"{name}_{lidx}")
                line.coords = ring_to_coords(line_coords)
                line.style.linestyle.width = 2
        # other types ignored for brevity
    return kml.kml().encode("utf-8")

# -------------------------
# Session state
# -------------------------
if "fc" not in st.session_state: st.session_state.fc = None
if "df" not in st.session_state: st.session_state.df = None
if "last_error" not in st.session_state: st.session_state.last_error = None

# -------------------------
# Top nav
# -------------------------
st.markdown(
    f"""
<div id="topbar">
  <div class="title">MappingKML • Kepler</div>
  <div style="margin-left:auto;"></div>
</div>
""",
    unsafe_allow_html=True,
)

# -------------------------
# Query UI (popover if available; otherwise sidebar)
# -------------------------
def query_form():
    st.write("**Bulk Lot/Plan Search**")
    st.caption("Enter one per line (e.g. `1/DP1104787`, `246//DP753311`, `329/753311`)")

    lots_text = st.text_area("Lots / Plans", value="", height=160, label_visibility="collapsed", key="lotplan_input")
    c1, c2, c3 = st.columns([1, 1, 2])
    run = c1.button("Run Query", type="primary")
    clear = c2.button("Clear")

    if run:
        if not API_BASE:
            st.error("API_BASE is not set. Set env var API_BASE to your backend base URL.")
        else:
            try:
                resp = requests.post(f"{API_BASE}/search", json={"q": lots_text}, timeout=120)
                resp.raise_for_status()
                fc = resp.json()
                # Accept FC, {features:[]}, or [] and coerce into FC
                if isinstance(fc, dict) and fc.get("type") == "FeatureCollection":
                    pass
                elif isinstance(fc, dict) and "features" in fc:
                    fc = {"type": "FeatureCollection", "features": fc["features"]}
                elif isinstance(fc, list):
                    fc = {"type": "FeatureCollection", "features": fc}
                else:
                    raise ValueError("Response is not a FeatureCollection.")

                df = features_to_rows(fc)
                st.session_state.fc = fc
                st.session_state.df = df
                st.session_state.last_error = None
                st.success(f"Loaded {len(df)} feature(s). Map updated.")
            except Exception as e:
                st.session_state.last_error = str(e)
                st.error(f"Query failed: {e}")

    if clear:
        st.session_state.fc = None
        st.session_state.df = None
        st.session_state.last_error = None
        st.success("Cleared results.")

    # Exports (if we have data)
    if st.session_state.fc:
        gj = json.dumps(st.session_state.fc, ensure_ascii=False)
        st.download_button("Download GeoJSON", gj, file_name="query.geojson", mime="application/geo+json")
        try:
            kml_bytes = fc_to_kml_bytes(st.session_state.fc)
            st.download_button(
                "Download KML",
                kml_bytes,
                file_name="query.kml",
                mime="application/vnd.google-earth.kml+xml"
            )
        except Exception as e:
            st.info(f"KML export unavailable: {e}")

# Popover (Streamlit >= 1.31) or sidebar fallback
if hasattr(st, "popover"):
    navcols = st.columns([0.82, 0.18])
    with navcols[1]:
        with st.popover("Query", use_container_width=True):
            query_form()
else:
    with st.sidebar:
        st.header("Query")
        query_form()

# -------------------------
# Map area (full-height under nav)
# -------------------------
st.markdown('<div id="mapwrap">', unsafe_allow_html=True)
render_kepler(st.session_state.df, st.session_state.fc)
st.markdown('</div>', unsafe_allow_html=True)

# Optional: show last error unobtrusively
if st.session_state.last_error:
    st.info(f"Last error: {st.session_state.last_error}")
