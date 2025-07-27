import json
from io import BytesIO

import streamlit as st
from keplergl import KeplerGl
from streamlit_keplergl import keplergl_static

from shapely.geometry import mapping as shp_mapping
from fastkml import kml
import pandas as pd

from kepler_config import BASE_CONFIG

st.set_page_config(page_title="MappingKML — Kepler Layout + Query", layout="wide")
st.markdown("<style>" + open("style.css", "r", encoding="utf-8").read() + "</style>", unsafe_allow_html=True)

# ---------------------------
# KML -> GeoJSON (FeatureCollection)
# ---------------------------
def kml_to_featurecollection(kml_bytes: bytes) -> dict:
    kdoc = kml.KML()
    kdoc.from_string(kml_bytes)

    def collect(node):
        geoms = []
        if hasattr(node, "geometry") and node.geometry is not None:
            geoms.append(node.geometry)
        if hasattr(node, "features") and node.features() is not None:
            for f in node.features():
                geoms.extend(collect(f))
        return geoms

    features = []
    # Walk all top-level features (Documents/Folders/Placemarks)
    for f in getattr(kdoc, "features")() or []:
        for g in collect(f):
            try:
                features.append({"type": "Feature", "geometry": shp_mapping(g), "properties": {}})
            except Exception:
                pass

    # Edge case: nothing on top-level
    if not features:
        for g in collect(kdoc):
            try:
                features.append({"type": "Feature", "geometry": shp_mapping(g), "properties": {}})
            except Exception:
                pass

    return {"type": "FeatureCollection", "features": features}

# ---------------------------
# YOUR QUERY HOOK (replace stub with your real function)
# ---------------------------
def run_lotplan_query(lotplan_text: str) -> dict:
    """
    Replace this stub with your existing query implementation.
    It MUST return a GeoJSON FeatureCollection (polygons preferred).
    Contract:
      input: raw lot/plan string (e.g. '169-173, 203, 220, 246, 329//DP753311' or '1RP912949')
      output: {"type":"FeatureCollection","features":[{"type":"Feature","geometry":{...},"properties":{...}}, ...]}
    """
    # TODO: integrate your ArcGIS/QLD cadastral query code here, producing a FeatureCollection.
    return {"type": "FeatureCollection", "features": []}

# ---------------------------
# Sidebar UI (Query + KML upload + dataset management)
# ---------------------------
st.sidebar.title("Query & Data")
st.sidebar.caption("Add a **Query** panel next to Kepler’s Layers/Filters to drive datasets rendered on the map.")

with st.sidebar.expander("Query (Lot/Plan search)", expanded=True):
    lotplan = st.text_area(
        "Enter Lot/Plan (supports comma/range syntax):",
        placeholder="e.g. 169-173, 203, 220, 246, 329//DP753311 or 1RP912949",
        height=100,
    )
    q_run = st.button("Run Query", type="primary", use_container_width=True)
    st.caption("Wire your query in run_lotplan_query() to return a **GeoJSON FeatureCollection**.")

with st.sidebar.expander("Add Data (KML Upload)", expanded=False):
    kml_file = st.file_uploader("Upload .kml", type=["kml"])
    st.caption("Uploaded polygons/lines/points will render as a new dataset.")

with st.sidebar.expander("Datasets on map", expanded=True):
    ds_query_name = st.text_input("Query dataset name", value="QueryResults")
    ds_kml_name = st.text_input("KML dataset name", value="KMLUpload")

# Hold datasets in session
if "datasets" not in st.session_state:
    st.session_state["datasets"] = {}  # name -> FeatureCollection

# Run query
if q_run and lotplan.strip():
    try:
        fc = run_lotplan_query(lotplan.strip())
        if not isinstance(fc, dict) or fc.get("type") != "FeatureCollection":
            st.sidebar.error("Your query must return a GeoJSON FeatureCollection dict.")
        else:
            st.session_state["datasets"][ds_query_name] = fc
            st.sidebar.success(f"Added: {ds_query_name} ({len(fc.get('features', []))} features)")
    except Exception as e:
        st.sidebar.error(f"Query error: {e}")

# Handle KML upload
if kml_file is not None:
    try:
        fc_kml = kml_to_featurecollection(kml_file.read())
        st.session_state["datasets"][ds_kml_name] = fc_kml
        st.sidebar.success(f"Added: {ds_kml_name} ({len(fc_kml.get('features', []))} features)")
    except Exception as e:
        st.sidebar.error(f"KML parse error: {e}")

# Remove dataset (optional)
if st.session_state["datasets"]:
    remove_key = st.selectbox("Remove dataset", ["—"] + list(st.session_state["datasets"].keys()))
    if remove_key and remove_key != "—":
        if st.button("Remove selected", use_container_width=True):
            st.session_state["datasets"].pop(remove_key, None)

# ---------------------------
# Kepler map render
# ---------------------------
# Build KeplerGl map and add each dataset as a named GeoJSON source.
map_ = KeplerGl(height=800, config=BASE_CONFIG)
for name, fc in st.session_state["datasets"].items():
    # KeplerGl Python accepts GeoJSON dicts via add_data
    map_.add_data(data=fc, name=name)

keplergl_static(map_)
