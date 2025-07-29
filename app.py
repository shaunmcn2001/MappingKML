# app.py
import os
import json
import math
import requests
import pandas as pd
import streamlit as st
from keplergl import KeplerGl

# If you have streamlit-keplergl installed, use keplergl_static; otherwise, we inline the HTML.
try:
    from streamlit_keplergl import keplergl_static
    HAS_ST_KEPLER = True
except Exception:
    from streamlit.components.v1 import html as st_html
    HAS_ST_KEPLER = False

# -------------------------
# Config
# -------------------------
st.set_page_config(page_title="MappingKML • Kepler", layout="wide", initial_sidebar_state="collapsed")

API_BASE = os.getenv("API_BASE", "").rstrip("/")
MAP_STYLE = os.getenv("MAP_STYLE", "dark")  # kepler styleType: dark | light | satellite | outdoors, etc.
NAV_HEIGHT = 56  # px
MAP_HEIGHT = int(os.getenv("MAP_HEIGHT", "900"))  # fallback height if we can't use 100vh

# -------------------------
# Styles – full-bleed & top nav
# -------------------------
st.markdown(
    f"""
<style>
/* Remove default Streamlit padding */
.appview-container .main .block-container {{
  padding-top: 0rem;
  padding-left: 0rem;
  padding-right: 0rem;
  padding-bottom: 0rem;
}}
/* Top bar */
#topbar {{
  position: sticky;
  top: 0;
  z-index: 999;
  height: {NAV_HEIGHT}px;
  background: #111;
  color: #fff;
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 0 14px;
  border-bottom: 1px solid #222;
}}
#topbar .title {{
  font-weight: 600;
  letter-spacing: .2px;
}}
/* Map wrapper tries to fill viewport */
#mapwrap {{
  height: calc(100vh - {NAV_HEIGHT}px);
}}
/* Buttons & inputs in popover */
.block-container .stButton > button {{
  border-radius: 8px;
}}
</style>
""",
    unsafe_allow_html=True,
)

# -------------------------
# Helpers
# -------------------------
def features_to_rows(fc: dict) -> pd.DataFrame:
    """
    Convert a GeoJSON FeatureCollection into a flat DataFrame where
    each row = feature properties + a 'geometry' column (the GeoJSON geometry object).
    Kepler config will map 'geometry' via a GeoJSON layer.
    """
    feats = fc.get("features") or []
    rows = []
    for f in feats:
        props = (f.get("properties") or {}).copy()
        props["geometry"] = f.get("geometry")
        rows.append(props)
    return pd.DataFrame(rows)

def _extract_coords(geom: dict):
    """Return a flat list of [lon, lat] pairs from any GeoJSON geometry."""
    coords_out = []
    def rec(node):
        if isinstance(node, (list, tuple)) and node and isinstance(node[0], (int, float)):
            # assume [lon, lat] (ignore altitude if present)
            coords_out.append(node[:2])
        elif isinstance(node, (list, tuple)):
            for child in node:
                rec(child)
    if not geom:
        return coords_out
    rec(geom.get("coordinates"))
    return coords_out

def featurecollection_bbox(fc: dict):
    """Compute [minx, miny, maxx, maxy] from a FeatureCollection."""
    minx = miny = math.inf
    maxx = maxy = -math.inf
    for f in fc.get("features", []):
        coords = _extract_coords(f.get("geometry") or {})
        for (x, y) in coords:
            minx = min(minx, x)
            miny = min(miny, y)
            maxx = max(maxx, x)
            maxy = max(maxy, y)
    if math.isinf(minx):
        # No geometry; default to Australia-ish center
        return [133.7751 - 1, -25.2744 - 1, 133.7751 + 1, -25.2744 + 1]
    return [minx, miny, maxx, maxy]

def center_from_bbox(b):
    """Return (lon, lat) center of bbox."""
    return ((b[0] + b[2]) / 2.0, (b[1] + b[3]) / 2.0)

def build_kepler_config(data_id: str, fc: dict | None):
    """
    Build a kepler config using a GeoJSON layer that reads geometry from
    the 'geometry' column.
    If fc is provided, center the map to its bbox; else use a sensible default.
    """
    if fc:
        bbox = featurecollection_bbox(fc)
        lon, lat = center_from_bbox(bbox)
        zoom = 9 if (bbox[2] - bbox[0]) > 0.3 or (bbox[3] - bbox[1]) > 0.3 else 12
    else:
        lon, lat, zoom = 153.0260, -27.4705, 9  # Brisbane default

    config = {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [],
                "layers": [
                    {
                        "id": "parcels-layer",
                        "type": "geojson",
                        "config": {
                            "dataId": data_id,
                            "label": "Parcels",
                            "color": [18, 92, 255],
                            "columns": {"geojson": "geometry"},
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.6,
                                "strokeOpacity": 0.9,
                                "thickness": 1.2,
                                "stroked": True,
                                "filled": True,
                                "enable3d": False,
                                "wireframe": False
                            }
                        },
                        "visualChannels": {}
                    }
                ],
                "interactionConfig": {
                    "tooltip": {"fieldsToShow": {data_id: []}, "enabled": True}
                },
                "animationConfig": {"enabled": False}
            },
            "mapState": {
                "bearing": 0,
                "dragRotate": False,
                "latitude": lat,
                "longitude": lon,
                "pitch": 0,
                "zoom": zoom
            },
            "mapStyle": {"styleType": MAP_STYLE}
        }
    }
    return config

def render_kepler(df: pd.DataFrame | None, fc: dict | None):
    """
    Create a KeplerGl instance and render it full-width.
    """
    data_id = "query_results"
    config = build_kepler_config(data_id, fc)
    # KeplerGl accepts a dict mapping dataset name -> DataFrame
    kg = KeplerGl(height=MAP_HEIGHT, data={data_id: df if df is not None else pd.DataFrame()}, config=config)

    if HAS_ST_KEPLER:
        keplergl_static(kg)
    else:
        # Fallback: render as raw HTML if streamlit-keplergl is not available.
        st_html(kg._repr_html_(), height=MAP_HEIGHT, scrolling=False)

# -------------------------
# Session defaults
# -------------------------
if "fc" not in st.session_state:
    st.session_state.fc = None
if "df" not in st.session_state:
    st.session_state.df = None
if "last_error" not in st.session_state:
    st.session_state.last_error = None

# -------------------------
# Top Navigation
# -------------------------
st.markdown(
    f"""
<div id="topbar">
  <div class="title">MappingKML • Kepler</div>
  <div style="margin-left:auto;display:flex;gap:8px;align-items:center;">
    <!-- The Streamlit button/popover will render just below -->
  </div>
</div>
""",
    unsafe_allow_html=True,
)

# Query UI: prefer st.popover if available; otherwise fall back to the sidebar.
def query_ui():
    st.write("**Bulk Lot/Plan Search**")
    st.caption("Enter one per line (examples: `1/DP1104787`, `246//DP753311`, `329/753311`)")

    text = st.text_area("Lots / Plans", value="", height=160, key="lotplan_input", label_visibility="collapsed")
    cols = st.columns([1, 1, 3])
    with cols[0]:
        run = st.button("Run Query", type="primary")
    with cols[1]:
        clear = st.button("Clear")

    if run:
        if not API_BASE:
            st.error("API_BASE is not set. Set the environment variable API_BASE to your backend base URL.")
            return
        payload = {"q": text}
        try:
            resp = requests.post(f"{API_BASE}/search", json=payload, timeout=120)
            resp.raise_for_status()
            fc = resp.json()
            if not isinstance(fc, dict) or fc.get("type") != "FeatureCollection":
                st.warning("Response does not look like a GeoJSON FeatureCollection. Attempting to coerce.")
                if isinstance(fc, dict) and "features" in fc:
                    fc = {"type": "FeatureCollection", "features": fc["features"]}
                elif isinstance(fc, list):
                    fc = {"type": "FeatureCollection", "features": fc}
                else:
                    raise ValueError("Cannot coerce response to FeatureCollection.")

            df = features_to_rows(fc)
            st.session_state.fc = fc
            st.session_state.df = df
            st.session_state.last_error = None
            st.success(f"Loaded {len(df)} feature(s). Map updated below.")
        except Exception as e:
            st.session_state.last_error = str(e)
            st.error(f"Query failed: {e}")

    if clear:
        st.session_state.fc = None
        st.session_state.df = None
        st.session_state.last_error = None
        st.success("Cleared results.")

# Render Query trigger in the top bar
supports_popover = hasattr(st, "popover")  # Streamlit >= 1.31
if supports_popover:
    # Render a small container so the popover aligns closely with the nav
    nav_cols = st.columns([0.82, 0.18])
    with nav_cols[1]:
        with st.popover("Query", use_container_width=True):
            query_ui()
else:
    # Fallback: show Query in the sidebar
    with st.sidebar:
        st.header("Query")
        query_ui()

# -------------------------
# Map (full-height below the nav bar)
# -------------------------
with st.container():
    # Wrap in an element that tries to claim full viewport height.
    st.markdown('<div id="mapwrap">', unsafe_allow_html=True)
    render_kepler(st.session_state.df, st.session_state.fc)
    st.markdown("</div>", unsafe_allow_html=True)

# Optional: display errors (if any)
if st.session_state.last_error:
    st.info(f"Last error: {st.session_state.last_error}")
