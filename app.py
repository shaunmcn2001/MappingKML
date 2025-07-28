import json
from io import BytesIO
import re
import itertools
import copy
from math import inf

import streamlit as st
from keplergl import KeplerGl
from streamlit_keplergl import keplergl_static
from shapely.geometry import mapping as shp_mapping
from fastkml import kml
import pandas as pd
import requests

from kepler_config import BASE_CONFIG
import kml_utils

import os  # needed for loading style.css


def compute_bbox_of_featurecollections(named_fcs: dict[str, dict]):
    """Return (minx, miny, maxx, maxy) across all FeatureCollections.

    Parameters
    ----------
    named_fcs: dict
        Mapping of dataset names to GeoJSON FeatureCollection dictionaries.

    Returns
    -------
    tuple or None
        Bounding box as (minx, miny, maxx, maxy) or ``None`` if no
        coordinates are present.
    """
    minx, miny, maxx, maxy = inf, inf, -inf, -inf

    def walk_coords(coords):
        nonlocal minx, miny, maxx, maxy
        if isinstance(coords, (list, tuple)) and coords and isinstance(coords[0], (int, float)):
            x, y = coords[:2]
            if x < minx:
                minx = x
            if y < miny:
                miny = y
            if x > maxx:
                maxx = x
            if y > maxy:
                maxy = y
        elif isinstance(coords, (list, tuple)):
            for c in coords:
                walk_coords(c)

    for _, fc in named_fcs.items():
        if not fc or fc.get("type") != "FeatureCollection":
            continue
        for feat in fc.get("features", []):
            geom = (feat or {}).get("geometry")
            if not geom:
                continue
            walk_coords(geom.get("coordinates"))
    if minx is inf:
        return None
    return (minx, miny, maxx, maxy)


# ------------------------------------------------------------
# KML -> GeoJSON (FeatureCollection)
# ------------------------------------------------------------
def kml_to_featurecollection(kml_bytes: bytes) -> dict:
    """Parse an uploaded KML file into a GeoJSON FeatureCollection.

    This helper walks through the KML structure and extracts any geometries
    present, returning them as simple GeoJSON features with empty property
    dictionaries.  Errors during parsing are suppressed so that malformed
    KML placemarks do not break the entire import.
    """
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


# ------------------------------------------------------------
# Parcel query endpoints
# ------------------------------------------------------------
# Queensland Land Parcel Property Framework MapServer: layer 4
QLD_QUERY_URL = (
    "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/"
    "PlanningCadastre/LandParcelPropertyFramework/MapServer/4/query"
)

# New South Wales Cadastre MapServer: layer 9
NSW_QUERY_URL = (
    "https://maps.six.nsw.gov.au/arcgis/rest/services/public/"
    "NSW_Cadastre/MapServer/9/query"
)


def _safe_request(url: str, params: dict) -> dict:
    """Perform an HTTP GET request and return JSON, handling errors gracefully."""
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        # surface the error in the Streamlit UI if possible
        try:
            st.error(f"Request to {url} failed: {e}")
        except Exception:
            pass
        return {"type": "FeatureCollection", "features": []}


def _split_lot_plan(lotplan: str):
    """Split a canonical lotplan token into its lot and plan components.

    Accepts strings like ``1RP912949`` and returns ("1", "RP912949").
    Leading zeros on the lot are not retained.
    Returns (None, None) if the input does not match the expected pattern.
    """
    m = re.match(r"^\s*([0-9]+)\s*([A-Z]{1,3}[0-9A-Z]+)\s*$", lotplan)
    if not m:
        return None, None
    return m.group(1), m.group(2)


def normalize_lotplan_input(text: str):
    """Normalise messy user input to canonical lotplan tokens.

    Supports comma/range syntax (e.g. ``169-173, 203 // DP753311``), slashed
    formats (``1/RP912949`` or ``L1RP912949``) and de-duplicates the output
    list while stripping leading zeros on the lot number.
    """
    if not text:
        return []
    t = text.upper()
    # Unify some common phrases
    t = t.replace("REGISTERED PLAN", "RP").replace("SURVEY PLAN", "SP")
    t = t.replace("CROWN PLAN A", "CPA").replace("CROWN PLAN", "CP")
    t = t.replace(" ON ", " ").replace(" OF ", " ").replace(":", " ")
    # Split on semicolons/newlines into segments; each may contain // syntax
    parts = re.split(r"[;\n]+", t)

    def expand_range_list(numlist_str):
        nums = []
        for piece in re.split(r"[,\s]+", numlist_str.strip()):
            if not piece:
                continue
            if "-" in piece:
                a, b = piece.split("-", 1)
                if a.isdigit() and b.isdigit():
                    lo, hi = int(a), int(b)
                    step = 1 if hi >= lo else -1
                    nums.extend([str(x) for x in range(lo, hi + step, step)])
            elif piece.isdigit():
                nums.append(piece)
        return nums

    out = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        # Case A: '169-173, 203 // DP753311'
        m = re.search(r"(.+?)\s*//\s*([A-Z]{1,3})\s*([0-9A-Z]+)", p)
        if m:
            lots = expand_range_list(m.group(1))
            plan = f"{m.group(2)}{m.group(3)}"
            out.extend([f"{n}{plan}" for n in lots])
            continue
        # Case B: '1/RP912949' or '1 RP912949'
        m = re.match(r"^([0-9]+)\s*[\/ ]\s*([A-Z]{1,3})\s*([0-9A-Z]+)$", p)
        if m:
            out.append(f"{m.group(1)}{m.group(2)}{m.group(3)}")
            continue
        # Case C: 'L1 RP912949' or 'L1RP912949'
        m = re.match(r"^L?\s*([0-9]+)\s*([A-Z]{1,3})\s*([0-9A-Z]+)$", p)
        if m:
            out.append(f"{m.group(1)}{m.group(2)}{m.group(3)}")
            continue
        # Case D: already canonical '1RP912949'
        m = re.match(r"^([0-9]+)([A-Z]{1,3})([0-9A-Z]+)$", p)
        if m:
            out.append(p)
            continue
        # else ignore; optionally surface in Streamlit
        try:
            st.info(f"Ignored unrecognised token: {p}")
        except Exception:
            pass
    # De-duplicate + strip leading zeros on lot numbers
    clean = []
    for token in out:
        m = re.match(r"^0*([0-9]+)([A-Z]{1,3})([0-9A-Z]+)$", token)
        if m:
            token = f"{m.group(1)}{m.group(2)}{m.group(3)}"
        clean.append(token)
    seen, uniq = set(), []
    for lp in clean:
        if lp not in seen:
            seen.add(lp)
            uniq.append(lp)
    return uniq


def run_lotplan_query(raw_text: str) -> dict:
    """Normalise user input, query parcel services and return features.

    The input string is first normalised into canonical lotplan tokens.  Each
    token is then inspected: if the plan prefix indicates a New South Wales
    plan (``DP`` or ``SP``) the NSW cadastre service is queried.  Otherwise
    the Queensland cadastre service is queried.  Results from all tokens are
    aggregated into a single FeatureCollection.  Any errors encountered
    during requests are surfaced in the Streamlit UI and result in an
    empty feature list for that token.

    Parameters
    ----------
    raw_text: str
        Free-form user input containing lot/plan identifiers.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection containing all found parcel features.
    """
    norm = normalize_lotplan_input(raw_text)
    if not norm:
        return {"type": "FeatureCollection", "features": []}
    features: list = []
    for token in norm:
        m = re.match(r"^0*([0-9]+)([A-Z]{1,3})([0-9A-Z]+)$", token)
        if not m:
            continue
        lot = m.group(1)
        plan_prefix = m.group(2)
        plan_suffix = m.group(3)
        # NSW plans begin with DP or SP (e.g. DP753311)
        if plan_prefix in ("DP", "SP"):
            plan_label = plan_prefix + plan_suffix
            plan_num = "".join(ch for ch in plan_suffix if ch.isdigit())
            where_clauses = [f"lotnumber='{lot}'"]
            # If no section specified we match empty or null sectionnumber
            where_clauses.append("(sectionnumber IS NULL OR sectionnumber = '')")
            if plan_num:
                where_clauses.append(f"plannumber={plan_num}")
            params = {
                "where": " AND ".join(where_clauses),
                "outFields": "lotnumber,sectionnumber,planlabel,plannumber,shape",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "geojson",
            }
            gj = _safe_request(NSW_QUERY_URL, params)
            features.extend(gj.get("features", []))
        else:
            # Assume Queensland plan
            plan = plan_prefix + plan_suffix
            params = {
                "where": f"lot='{lot}' AND plan='{plan}'",
                "outFields": "lot,plan,lotplan,locality,shape",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "geojson",
            }
            gj = _safe_request(QLD_QUERY_URL, params)
            features.extend(gj.get("features", []))
    try:
        st.info(f"Queried {len(norm)} lotplan token(s); returned {len(features)} feature(s).")
    except Exception:
        pass
    return {"type": "FeatureCollection", "features": features}


# ------------------------------------------------------------
# Streamlit layout and interaction
# ------------------------------------------------------------
st.set_page_config(page_title="MappingKML — Kepler Layout + Query", layout="wide")
# Inject our custom stylesheet
try:
    st.markdown("<style>" + open(os.path.join(os.path.dirname(__file__), "style.css"), "r", encoding="utf-8").read() + "</style>", unsafe_allow_html=True)
except Exception:
    # If the CSS cannot be loaded for any reason skip styling
    pass


# Sidebar: Query & Data
st.sidebar.title("Query & Data")
st.sidebar.caption(
    "Add a **Query** panel next to Kepler’s Layers/Filters to drive datasets rendered on the map."
)

with st.sidebar.expander("Query (Lot/Plan search)", expanded=True):
    lotplan = st.text_area(
        "Enter Lot/Plan (supports comma/range syntax):",
        placeholder="e.g. 169-173, 203, 220, 246, 329//DP753311 or 1RP912949",
        height=100,
    )
    q_run = st.button("Run Query", type="primary", use_container_width=True)
    st.caption("Enter one or more lot/plan identifiers.  Examples: `1RP912949`, `1/DP123456`, `169-173,203//DP753311`.  The app will normalise your input and query the appropriate cadastral service.")

with st.sidebar.expander("Add Data (KML Upload)", expanded=False):
    kml_file = st.file_uploader("Upload .kml", type=["kml"])
    st.caption("Uploaded polygons/lines/points will render as a new dataset.")

with st.sidebar.expander("Datasets on map", expanded=True):
    ds_query_name = st.text_input("Query dataset name", value="QueryResults")
    ds_kml_name = st.text_input("KML dataset name", value="KMLUpload")

# Hold datasets in session
if "datasets" not in st.session_state:
    st.session_state["datasets"] = {}  # name -> FeatureCollection

# Run query when the button is clicked
if q_run and lotplan and lotplan.strip():
    try:
        fc = run_lotplan_query(lotplan.strip())
        if not isinstance(fc, dict) or fc.get("type") != "FeatureCollection":
            st.sidebar.error("Your query must return a GeoJSON FeatureCollection dict.")
        else:
            st.session_state["datasets"][ds_query_name] = fc
            st.success(f"Added: {ds_query_name} ({len(fc.get('features', []))} features)")
            st.info(f"Datasets now: {', '.join(st.session_state['datasets'].keys())}")
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

# Export / Download section
if st.session_state["datasets"]:
    with st.sidebar.expander("Export / Download", expanded=False):
        ds_names = list(st.session_state["datasets"].keys())
        selected_ds = st.selectbox("Select dataset", ds_names)
        fc = st.session_state["datasets"].get(selected_ds, {})
        feats = fc.get("features", [])
        if feats:
            # Heuristically determine the region based on property keys
            first_props = (feats[0] or {}).get("properties", {})
            region = "NSW" if "planlabel" in first_props else "QLD"
            fill_hex = st.color_picker("Fill colour", value="#FF0000")
            fill_opacity = st.slider("Fill opacity", 0.0, 1.0, 0.4, 0.05)
            outline_hex = st.color_picker("Outline colour", value="#000000")
            outline_weight = st.slider("Outline weight (px)", 1, 5, 2)
            # Generate files on the fly
            kml_str = kml_utils.generate_kml(
                feats, region, fill_hex, fill_opacity, outline_hex, outline_weight, selected_ds
            )
            st.download_button(
                "Download KML", data=kml_str, file_name=f"{selected_ds}.kml",
                mime="application/vnd.google-earth.kml+xml"
            )
            # Attempt to generate the shapefile; surface a warning if the
            # shapefile library is not available
            try:
                shp_bytes = kml_utils.generate_shapefile(feats, region)
                st.download_button(
                    "Download Shapefile (.zip)", data=shp_bytes,
                    file_name=f"{selected_ds}.zip", mime="application/zip"
                )
            except Exception as exc:
                st.warning(str(exc))
        else:
            st.info("No features available for the selected dataset.")

# ------------------------------------------------------------
# Kepler map render
# ------------------------------------------------------------
# Build the data bundle Kepler expects: { name: FeatureCollection, ... }
data_bundle = {name: fc for name, fc in st.session_state.get("datasets", {}).items()}
# Start from BASE_CONFIG, but remove explicit layers so Kepler auto-creates them.
cfg = dict(BASE_CONFIG) if 'BASE_CONFIG' in globals() else {}
try:
    cfg = copy.deepcopy(cfg)
    if "config" in cfg and "visState" in cfg["config"]:
        cfg["config"]["visState"].pop("layers", None)
except Exception:
    pass
# Optional: centre map to data bbox
bbox = compute_bbox_of_featurecollections(data_bundle) if data_bundle else None
if bbox and "config" in cfg and "mapState" in cfg["config"]:
    minx, miny, maxx, maxy = bbox
    center_lon = (minx + maxx) / 2.0
    center_lat = (miny + maxy) / 2.0
    cfg["config"]["mapState"]["longitude"] = center_lon
    cfg["config"]["mapState"]["latitude"] = center_lat
    cfg["config"]["mapState"]["zoom"] = cfg["config"]["mapState"].get("zoom", 9)
# Pass datasets via data= so Kepler auto-adds layers
m = KeplerGl(height=800, data=data_bundle if data_bundle else None, config=cfg if cfg else None)
keplergl_static(m)
