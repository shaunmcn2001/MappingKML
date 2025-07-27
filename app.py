import json
from io import BytesIO

import streamlit as st
from keplergl import KeplerGl
from streamlit_keplergl import keplergl_static

from shapely.geometry import mapping as shp_mapping
from fastkml import kml
import pandas as pd
from math import inf

def compute_bbox_of_featurecollections(named_fcs: dict[str, dict]):
    """Return (minx, miny, maxx, maxy) across all FeatureCollections; None if empty."""
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

from kepler_config import BASE_CONFIG

import re, itertools, requests

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
QLD_FEATURESERVER = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Basemaps/FoundationData/FeatureServer/2/query"

def run_lotplan_query(raw_text: str) -> dict:
    """
    Normalize messy user input to canonical lotplan tokens (e.g., '1RP912949'),
    query the QLD FeatureServer in batches, and return a GeoJSON FeatureCollection.
    """
    norm = normalize_lotplan_input(raw_text)
    if not norm:
        return {"type": "FeatureCollection", "features": []}

    # Warn if NSW plans (DP/SP) are present; those aren't in QLD DCDB.
    nswe = [lp for lp in norm if lp.startswith("DP") or lp.startswith("SP") or "//DP" in raw_text.upper()]
    if nswe:
        try:
            import streamlit as st
            st.warning("Detected NSW plan prefixes (DP/SP). The QLD endpoint will not return those. Ask to enable NSW routing next.")
        except Exception:
            pass
        norm = [lp for lp in norm if not (lp.startswith("DP") or lp.startswith("SP"))]

    if not norm:
        return {"type": "FeatureCollection", "features": []}

    features = []

    # Primary: query by lotplan IN (...)
    for chunk in _chunks(norm, 150):
        where = "lotplan IN ({})".format(",".join(f"'{lp}'" for lp in chunk))
        params = {
            "where": where,
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson"
        }
        gj = _qld_request(params)
        features.extend(gj.get("features", []))

    # Fallback: for any lotplans not returned, try exact plan+lot match
    found_lp = { (f.get("properties") or {}).get("lotplan") for f in features }
    missing = [lp for lp in norm if lp not in found_lp]
    for lp in missing:
        lot, plan = _split_lot_plan(lp)
        if not lot or not plan:
            continue
        where = f"plan='{plan}' AND lot='{lot}'"
        params = {
            "where": where,
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson"
        }
        gj = _qld_request(params)
        features.extend(gj.get("features", []))

    # Optional: tell user what we actually looked up
    try:
        import streamlit as st
        st.info(f"Queried {len(norm)} lotplan token(s); returned {len(features)} feature(s).")
    except Exception:
        pass

    return {"type": "FeatureCollection", "features": features}

def _qld_request(params: dict) -> dict:
    try:
        r = requests.get(QLD_FEATURESERVER, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        try:
            import streamlit as st
            st.error(f"QLD request failed: {e}")
        except Exception:
            pass
        return {"type": "FeatureCollection", "features": []}

def _chunks(seq, n):
    it = iter(seq)
    while True:
        block = list(itertools.islice(it, n))
        if not block:
            return
        yield block

def _split_lot_plan(lotplan: str):
    """
    '1RP912949' -> ('1','RP912949')
    accepts '1 RP912949' too (space handled earlier).
    """
    m = re.match(r"^\s*([0-9]+)\s*([A-Z]{1,3}[0-9A-Z]+)\s*$", lotplan)
    if not m:
        return None, None
    return m.group(1), m.group(2)

def normalize_lotplan_input(text: str):
    """
    Return canonical lotplan tokens from messy input.
    Handles:
      - 1/RP912949, L1 RP912949, 1 RP912949, 1RP912949
      - '169-173, 203, 220 // DP753311' -> expands to 169..173 + singles with plan 'DP753311'
      - de-duplicates and strips leading zeros on lot numbers.
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
            import streamlit as st
            st.info(f"Ignored unrecognized token: {p}")
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

# ---------------------------
# Kepler map render
# ---------------------------
# Build the data bundle Kepler expects: { name: FeatureCollection, ... }
data_bundle = {name: fc for name, fc in st.session_state.get("datasets", {}).items()}

# Start from BASE_CONFIG, but REMOVE explicit layers so Kepler auto-creates them.
cfg = dict(BASE_CONFIG) if 'BASE_CONFIG' in globals() else {}
try:
    # Deep copy and clear layers safely
    import copy
    cfg = copy.deepcopy(cfg)
    if "config" in cfg and "visState" in cfg["config"]:
        cfg["config"]["visState"].pop("layers", None)
except Exception:
    pass

# Optional: center map to data bbox
bbox = compute_bbox_of_featurecollections(data_bundle) if data_bundle else None
if bbox and "config" in cfg and "mapState" in cfg["config"]:
    minx, miny, maxx, maxy = bbox
    center_lon = (minx + maxx) / 2.0
    center_lat = (miny + maxy) / 2.0
    cfg["config"]["mapState"]["longitude"] = center_lon
    cfg["config"]["mapState"]["latitude"] = center_lat
    # A conservative zoom; user can adjust further
    # (Precise fit requires custom calc; we keep it simple here)
    cfg["config"]["mapState"]["zoom"] = cfg["config"]["mapState"].get("zoom", 9)

# IMPORTANT: pass datasets via `data=` so Kepler auto-adds layers
m = KeplerGl(height=800, data=data_bundle if data_bundle else None, config=cfg if cfg else None)

keplergl_static(m)
