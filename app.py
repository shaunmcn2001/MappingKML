import re
import itertools
from math import inf
import os

import streamlit as st
import pydeck as pdk
import requests

import kml_utils


# --------------------------------------------------------------------------------------
# Page + CSS
# --------------------------------------------------------------------------------------
st.set_page_config(page_title="MappingKML — Mapbox Viewer", layout="wide")
# Hide Streamlit header/menu/footer; tidy sidebar
st.markdown(
    "<style>" + open("style.css", "r", encoding="utf-8").read() + "</style>",
    unsafe_allow_html=True,
)


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def compute_bbox_of_featurecollections(named_fcs: dict[str, dict]):
    """Return (minx, miny, maxx, maxy) across all FeatureCollections; None if empty."""
    minx, miny, maxx, maxy = inf, inf, -inf, -inf

    def walk_coords(coords):
        nonlocal minx, miny, maxx, maxy
        if isinstance(coords, (list, tuple)) and coords and isinstance(coords[0], (int, float)):
            x, y = coords[:2]
            if x < minx: minx = x
            if y < miny: miny = y
            if x > maxx: maxx = x
            if y > maxy: maxy = y
        elif isinstance(coords, (list, tuple)):
            for c in coords:
                walk_coords(c)
@ -217,221 +208,127 @@ def run_lotplan_query(raw_text: str) -> dict:
            features.extend(gj.get("features", []))

    # NSW: for entries like "169-173 // DP753311" normaliser produced 169DP753311, etc.
    # We query by lotnumber + plannumber (section optional).
    for lp in nsw_reported:
        # If the user typed just "//DPxxxx" with ranges it got expanded to '169DPxxxx'; split
        m = re.match(r"^([0-9]+)(DP|SP)([0-9A-Z]+)$", lp)
        if not m:
            continue
        lot_val, plan_prefix, plan_num = m.group(1), m.group(2), m.group(3)
        where = f"lotnumber='{lot_val}' AND (sectionnumber IS NULL OR sectionnumber='') AND plannumber={re.sub('[^0-9]', '', plan_num)}"
        params = {
            "where": where,
            "outFields": "lotnumber,sectionnumber,planlabel,plannumber",
            "outSR": "4326",
            "returnGeometry": "true",
            "f": "geoJSON",
        }
        gj = _safe_request(NSW_FEATURESERVER, params)
        features.extend(gj.get("features", []))

    st.info(f"Queried {len(tokens)} token(s); returned {len(features)} feature(s).")
    return {"type": "FeatureCollection", "features": features}




def _approx_zoom_from_bbox(minx, miny, maxx, maxy):
    """Very rough zoom estimate for WebMercator; good enough for auto-fit."""
    span_lon = max(1e-6, maxx - minx)
    span_lat = max(1e-6, maxy - miny)
    span = max(span_lon, span_lat)
    # crude mapping: world ~360 deg -> z ~1; 0.01 deg -> ~15
    import math
    z = max(3.0, min(16.0, 1.0 + 8.0 - math.log(span, 2)))  # clamp
    return float(z)


# --------------------------------------------------------------------------------------
# Sidebar UI (Query + Upload + Dataset management)
# --------------------------------------------------------------------------------------
st.sidebar.title("Query & Data")
st.sidebar.caption("Search for Lot/Plan polygons and manage downloads.")

with st.sidebar.expander("Query (Lot/Plan search)", expanded=True):
    lotplan = st.text_area(
        "Enter Lot/Plan (supports comma/range syntax):",
        placeholder="e.g. 169-173, 203 // DP753311 or 1RP912949",
        height=100,
    )
    q_run = st.button("Run Query", type="primary", use_container_width=True)
    st.caption("The query returns a **GeoJSON FeatureCollection**.")

if "query_fc" not in st.session_state:
    st.session_state["query_fc"] = {"type": "FeatureCollection", "features": []}

if q_run and lotplan.strip():
    try:
        fc = run_lotplan_query(lotplan.strip())
        if not isinstance(fc, dict) or fc.get("type") != "FeatureCollection":
            st.sidebar.error("The query must return a GeoJSON FeatureCollection dict.")
        else:
            st.session_state["query_fc"] = fc
            st.success(f"Found {len(fc.get('features', []))} feature(s)")
    except Exception as e:
        st.sidebar.error(f"Query error: {e}")

with st.sidebar.expander("Layers on map", expanded=True):
    if st.session_state.get("query_fc", {}).get("features"):
        st.write("QueryResults")
    else:
        st.write("No layers")


# --------------------------------------------------------------------------------------
# Export / Download
# --------------------------------------------------------------------------------------
with st.sidebar.expander("Export / Download", expanded=False):
    folder_name = st.text_input("Folder name", value="QueryResults")
    fill_hex = st.text_input("Fill colour (hex)", "#00AAFF")
    fill_opacity = st.slider("Fill opacity", 0.0, 1.0, 0.3, 0.05)
    outline_hex = st.text_input("Outline colour (hex)", "#000000")
    outline_weight = st.number_input("Outline width (px)", 1, 10, 2)
    features = st.session_state.get("query_fc", {}).get("features", [])
    if features:
        region = "NSW" if any("planlabel" in (f.get("properties") or {}) for f in features) else "QLD"
        kml_str = kml_utils.generate_kml(features, region, fill_hex, fill_opacity, outline_hex, outline_weight, folder_name)
        st.download_button(
            "Download KML",
            data=kml_str.encode("utf-8"),
            file_name=f"{folder_name}.kml",
            mime="application/vnd.google-earth.kml+xml",
            use_container_width=True,
        )


# ---------------------------------------------------------------------------
# Mapbox map render (auto-zoom when query results available)
# ---------------------------------------------------------------------------
features = st.session_state.get("query_fc", {}).get("features", [])

mapbox_token = st.secrets.get("MAPBOX_API_KEY") or os.getenv("MAPBOX_API_KEY")
view_state = pdk.ViewState(latitude=-27.5, longitude=153.0, zoom=7)
if features:
    bbox = compute_bbox_of_featurecollections({"query": st.session_state["query_fc"]})
    if bbox:
        minx, miny, maxx, maxy = bbox
        view_state.longitude = (minx + maxx) / 2.0
        view_state.latitude = (miny + maxy) / 2.0
        view_state.zoom = _approx_zoom_from_bbox(minx, miny, maxx, maxy)

layer = pdk.Layer(
    "GeoJsonLayer",
    st.session_state.get("query_fc", {}),
    get_fill_color=[0, 170, 255, 77],
    get_line_color=[0, 0, 0, 255],
    get_line_width=2,
    pickable=True,
)

r = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    map_style="mapbox://styles/mapbox/satellite-v9",
    mapbox_key=mapbox_token,
)
st.pydeck_chart(r, use_container_width=True)
