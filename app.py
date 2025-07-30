import re
import itertools
from math import inf
import os

import streamlit as st
import pydeck as pdk
import requests
from backend.sa_query import search_sa
from backend.vic_query import search_vic

import kml_utils


# --------------------------------------------------------------------------------------
# Page + CSS
# --------------------------------------------------------------------------------------
st.set_page_config(page_title="MappingKML — Mapbox Viewer", layout="wide")
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


def _chunks(seq, n):
    it = iter(seq)
    while True:
        block = list(itertools.islice(it, n))
        if not block:
            return
        yield block


def _split_lot_plan(lotplan: str):
    # '1RP912949' -> ('1','RP912949') ; also accepts '1 RP912949'
    m = re.match(r"^\s*([0-9]+)\s*([A-Z]{1,3}[0-9A-Z]+)\s*$", lotplan)
    if not m:
        return None, None
    return m.group(1), m.group(2)


def _canonicalize_feature_props(feat: dict) -> None:
    """Add common property names for tooltips."""
    props = feat.setdefault("properties", {})
    if "lotplan" not in props:
        if "lot" in props and "plan" in props:
            props["lotplan"] = f"{props.get('lot','')}{props.get('plan','')}"
        elif "lotnumber" in props and "planlabel" in props:
            props["lotplan"] = f"{props.get('lotnumber','')}{props.get('planlabel','')}"
    if "lot_number" not in props:
        if "lot" in props:
            props["lot_number"] = props.get("lot", "")
        elif "lotnumber" in props:
            props["lot_number"] = props.get("lotnumber", "")
        elif "parcel_lot_number" in props:
            props["lot_number"] = props.get("parcel_lot_number", "")
    if "plan_value" not in props:
        if "plan" in props:
            props["plan_value"] = props.get("plan", "")
        elif "planlabel" in props:
            props["plan_value"] = props.get("planlabel", "")
        elif "parcel_plan_number" in props:
            props["plan_value"] = props.get("parcel_plan_number", "")


def normalize_lotplan_input(text: str):
    """
    Return canonical lotplan tokens from messy input.

    Handles:
      - 1/RP912949, L1 RP912949, 1 RP912949, 1RP912949 (QLD)
      - '169-173, 203 // DP753311' (NSW ranges)
      - de-duplicates and strips leading zeros on lot numbers.

    NOTE: SA and VIC tokens are handled separately; we no longer report them here as "ignored".
    """
    if not text:
        return []

    t = text.upper()
    t = t.replace("REGISTERED PLAN", "RP").replace("SURVEY PLAN", "SP")
    t = t.replace("CROWN PLAN A", "CPA").replace("CROWN PLAN", "CP")
    t = t.replace(" ON ", " ").replace(" OF ", " ").replace(":", " ")

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

        # NSW '169-173, 203 // DP753311'
        m = re.search(r"(.+?)\s*//\s*([A-Z]{1,3})\s*([0-9A-Z]+)", p)
        if m:
            lots = expand_range_list(m.group(1))
            plan = f"{m.group(2)}{m.group(3)}"
            out.extend([f"{n}{plan}" for n in lots])
            continue

        # QLD '1/RP912949' or '1 RP912949'
        m = re.match(r"^([0-9]+)\s*[\/ ]\s*([A-Z]{1,3})\s*([0-9A-Z]+)$", p)
        if m:
            out.append(f"{m.group(1)}{m.group(2)}{m.group(3)}")
            continue

        # QLD 'L1 RP912949' or 'L1RP912949'
        m = re.match(r"^L?\s*([0-9]+)\s*([A-Z]{1,3})\s*([0-9A-Z]+)$", p)
        if m:
            out.append(f"{m.group(1)}{m.group(2)}{m.group(3)}")
            continue

        # Already canonical '1RP912949'
        m = re.match(r"^([0-9]+)([A-Z]{1,3})([0-9A-Z]+)$", p)
        if m:
            out.append(p)
            continue

        # Do nothing for SA/VIC here; they are handled separately.

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


QLD_FEATURESERVER = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/PlanningCadastre/LandParcelPropertyFramework/MapServer/4/query"
NSW_FEATURESERVER = "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_Cadastre/MapServer/9/query"


def _safe_request(url: str, params: dict) -> dict:
    try:
        r = requests.get(url, params=params, timeout=25)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Request failed: {e}")
        return {"type": "FeatureCollection", "features": []}


def run_lotplan_query(raw_text: str) -> dict:
    """
    Route QLD/NSW via normaliser; always attempt SA & VIC on the raw text too.
    Returns a GeoJSON FeatureCollection.
    """
    features = []

    # Always try SA and VIC on the raw text (these use different token formats)
    try:
        sa_gj = search_sa(raw_text)
        features.extend(sa_gj.get("features", []))
    except Exception:
        pass

    try:
        vic_gj = search_vic(raw_text)
        features.extend(vic_gj.get("features", []))
    except Exception:
        pass

    # QLD/NSW via normaliser
    tokens = normalize_lotplan_input(raw_text)

    # Separate likely NSW by plan prefixes (DP/SP)
    qld_tokens = [lp for lp in tokens if not (lp.startswith("DP") or lp.startswith("SP"))]
    nsw_reported = [lp for lp in tokens if (lp.startswith("DP") or lp.startswith("SP"))]
    if nsw_reported:
        st.warning("Detected NSW DP/SP plans; routing those to NSW cadastre.")

    # QLD: lotplan IN (...) primary, fallback to lot/plan
    if qld_tokens:
        for chunk in _chunks(qld_tokens, 150):
            where = "lotplan IN ({})".format(",".join(f"'{lp}'" for lp in chunk))
            params = {"where": where, "outFields": "*", "returnGeometry": "true", "outSR": "4326", "f": "geojson"}
            gj = _safe_request(QLD_FEATURESERVER, params)
            features.extend(gj.get("features", []))

        found_lp = {(f.get("properties") or {}).get("lotplan") for f in features}
        missing = [lp for lp in qld_tokens if lp not in found_lp]
        for lp in missing:
            lot, plan = _split_lot_plan(lp)
            if not lot or not plan:
                continue
            where = f"lot='{lot}' AND plan='{plan}'"
            params = {"where": where, "outFields": "*", "returnGeometry": "true", "outSR": "4326", "f": "geojson"}
            gj = _safe_request(QLD_FEATURESERVER, params)
            features.extend(gj.get("features", []))

    # NSW: query by lotnumber + plannumber (section optional)
    for lp in nsw_reported:
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

    st.info(f"Queried input; returned {len(features)} feature(s).")
    return {"type": "FeatureCollection", "features": features}


def _approx_zoom_from_bbox(minx, miny, maxx, maxy):
    span_lon = max(1e-6, maxx - minx)
    span_lat = max(1e-6, maxy - miny)
    span = max(span_lon, span_lat)
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
        "Enter Lot/Plan (supports QLD/NSW syntax; SA/VIC also supported):",
        placeholder="e.g. 169-173, 203 // DP753311 | 1RP912949 | D10000A1 | 24PS601720",
        height=100,
    )
    q_run = st.button("Run Query", type="primary", use_container_width=True)
    st.caption("Returns a **GeoJSON FeatureCollection**")

if "query_fc" not in st.session_state:
    st.session_state["query_fc"] = {"type": "FeatureCollection", "features": []}

if q_run and lotplan.strip():
    try:
        fc = run_lotplan_query(lotplan.strip())
        if not isinstance(fc, dict) or fc.get("type") != "FeatureCollection":
            st.sidebar.error("The query must return a GeoJSON FeatureCollection dict.")
        else:
            for f in fc.get("features", []):
                _canonicalize_feature_props(f)
            st.session_state["query_fc"] = fc
            st.success(f"Found {len(fc.get('features', []))} feature(s)")
    except Exception as e:
        st.sidebar.error(f"Query error: {e}")

with st.sidebar.expander("Layers on map", expanded=True):
    if st.session_state.get("query_fc", {}).get("features"):
        col1, col2 = st.columns([1, 1])
        col1.write("QueryResults")
        if col2.button("Remove", key="remove_query_layer"):
            st.session_state["query_fc"] = {"type": "FeatureCollection", "features": []}
            st.experimental_rerun()
    else:
        st.write("No layers")

# --------------------------------------------------------------------------------------
# Export / Download (region detection improved for SA/VIC)
# --------------------------------------------------------------------------------------
with st.sidebar.expander("Export / Download", expanded=False):
    folder_name = st.text_input("Folder name", value="QueryResults")
    fill_hex = st.text_input("Fill colour (hex)", "#00AAFF")
    fill_opacity = st.slider("Fill opacity", 0.0, 1.0, 0.3, 0.05)
    outline_hex = st.text_input("Outline colour (hex)", "#000000")
    outline_weight = st.number_input("Outline width (px)", 1, 10, 2)
    features = st.session_state.get("query_fc", {}).get("features", [])
    for f in features:
        _canonicalize_feature_props(f)
    if features:
        # Detect region by fields present
        region = "QLD"
        if any("planlabel" in (f.get("properties") or {}) for f in features):
            region = "NSW"
        elif any("plan_t" in (f.get("properties") or {}) for f in features):
            region = "SA"
        elif any("parcel_plan_number" in (f.get("properties") or {}) for f in features):
            region = "VIC"

        kml_str = kml_utils.generate_kml(
            features,
            region,
            fill_hex,
            fill_opacity,
            outline_hex,
            outline_weight,
            folder_name,
        )
        st.download_button(
            "Download KML",
            data=kml_str.encode("utf-8"),
            file_name=f"{folder_name}.kml",
            mime="application/vnd.google-earth.kml+xml",
            use_container_width=True,
        )
        try:
            shp_bytes = kml_utils.generate_shapefile(features, region)
            st.download_button(
                "Download Shapefile",
                data=shp_bytes,
                file_name=f"{folder_name}.zip",
                mime="application/zip",
                use_container_width=True,
            )
        except RuntimeError as e:
            st.warning(str(e))

# --------------------------------------------------------------------------------------
# Map render
# --------------------------------------------------------------------------------------
features = st.session_state.get("query_fc", {}).get("features", [])
for f in features:
    _canonicalize_feature_props(f)

mapbox_token = st.secrets.get("MAPBOX_API_KEY") or os.getenv("MAPBOX_API_KEY")
view_state = pdk.ViewState(latitude=-27.5, longitude=153.0, zoom=7)
if features:
    bbox = compute_bbox_of_featurecollections({"query": st.session_state["query_fc"]})
    if bbox:
        minx, miny, maxx, maxy = bbox
        view_state.longitude = (minx + maxx) / 2.0
        view_state.latitude  = (miny + maxy) / 2.0
        view_state.zoom      = _approx_zoom_from_bbox(minx, miny, maxx, maxy)

layer_query = pdk.Layer(
    "GeoJsonLayer",
    st.session_state.get("query_fc", {}),
    pickable=True,
    stroked=True,
    filled=True,
    extruded=False,
    wireframe=False,
    get_fill_color=[0, 170, 255, 80],
    get_line_color=[0, 0, 0, 200],
    get_line_width=2,
)

tooltip = {
    "html": "<b>Lot</b> {{properties.lot_number}}<br/>"
            "<b>Plan</b> {{properties.plan_value}}",
    "style": {"color": "white"},
}

st.pydeck_chart(
    pdk.Deck(
        map_style="mapbox://styles/mapbox/satellite-v9" if mapbox_token else None,
        initial_view_state=view_state,
        layers=[layer_query],
        tooltip=tooltip,
    )
)