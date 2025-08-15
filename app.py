# app.py
import io
import json
import importlib
import streamlit as st
import pydeck as pdk

from kml_utils import build_kml_feature_name, build_kml_balloon

# --- Optional imports: wire to your existing QLD / SA query functions if present
# Expected signatures:
#   query_qld(user_input: str) -> GeoJSON FeatureCollection (WGS84)
#   query_sa(user_input: str)  -> GeoJSON FeatureCollection (WGS84)
def _optional_import(module_name, func_name):
    try:
        m = importlib.import_module(module_name)
        return getattr(m, func_name)
    except Exception:
        return None

query_qld = _optional_import("qld_query", "query_qld")   # <-- change module/name if different
query_sa  = _optional_import("sa_query",  "query_sa")    # <-- change module/name if different

# NSW (required for NSW tab)
from nsw_query import query_nsw_lsp, NSWQueryError

st.set_page_config(page_title="MappingKML", layout="wide")

# ---------------------
# Session state setup
# ---------------------
if "current_geojson" not in st.session_state:
    st.session_state["current_geojson"] = None
if "current_layer_name" not in st.session_state:
    st.session_state["current_layer_name"] = None

# Compatibility bridge (if older code still sets "features")
if st.session_state.get("features") and not st.session_state.get("current_geojson"):
    st.session_state["current_geojson"] = st.session_state["features"]

# ---------------------
# UI Header
# ---------------------
st.title("MappingKML")
st.caption(
    "Tick one or more states, enter the parcel reference, then click **Search**. "
    "• NSW expects **Lot/Section/Plan** (section optional → `lot//plan`). "
    "• QLD/SA accept your existing formats."
)

# ---------------------
# State selection row
# ---------------------
col1, col2, col3, col4 = st.columns([1,1,1,3])
with col1:
    sel_qld = st.checkbox("QLD", value=False)
with col2:
    sel_nsw = st.checkbox("NSW", value=True)  # default ON to showcase new flow
with col3:
    sel_sa  = st.checkbox("SA",  value=False)
with col4:
    st.write("")

# ---------------------
# Single search box (works for all; NSW has special format)
# ---------------------
placeholder = "NSW: 3//DP753311  |  QLD: 3SP181800  |  SA: e.g., H835100 B829 (your format)"
user_input = st.text_input("Parcel input", placeholder=placeholder)

go = st.button("Search", type="primary")

# ---------------------
# Search handler
# ---------------------
per_state_msgs = []
combined = {"type": "FeatureCollection", "features": []}

def _annotate_features(fc, state_label):
    # Ensure each feature has a 'name' and a 'state' for tooltips / KML names
    feats = fc.get("features", [])
    out = []
    for f in feats:
        props = f.get("properties") or f.get("attributes") or {}
        props = dict(props)  # copy
        props.setdefault("state", state_label)
        # Build a friendly display name for tooltip
        try:
            name = build_kml_feature_name(props)
        except Exception:
            name = f"{state_label} parcel"
        props["name"] = name
        # write back
        f2 = dict(f)
        if "properties" in f2:
            f2["properties"] = props
        else:
            f2["properties"] = props  # normalize
        out.append(f2)
    return {"type": "FeatureCollection", "features": out}

if go:
    if not (sel_qld or sel_nsw or sel_sa):
        st.warning("Select at least one state (QLD / NSW / SA).")
    elif not user_input.strip():
        st.warning("Enter a parcel reference.")
    else:
        # Run each selected state, accumulate results
        any_success = False

        # NSW
        if sel_nsw:
            try:
                nsw_fc = query_nsw_lsp(user_input)
                nsw_fc = _annotate_features(nsw_fc, "NSW")
                combined["features"].extend(nsw_fc.get("features", []))
                cnt = len(nsw_fc.get("features", []))
                msg = f"NSW: {cnt} feature(s)."
                if "note" in nsw_fc:
                    msg += f" Note: {nsw_fc['note']}"
                per_state_msgs.append(("success", msg))
                any_success = any_success or cnt > 0
            except NSWQueryError as e:
                per_state_msgs.append(("warning", f"NSW: {e}"))
            except Exception as e:
                per_state_msgs.append(("error", f"NSW error: {e}"))

        # QLD
        if sel_qld:
            if query_qld is None:
                per_state_msgs.append(("warning",
                    "QLD: query function not found. Add `qld_query.py` with `query_qld(user_input)` or update the import path."
                ))
            else:
                try:
                    qld_fc = query_qld(user_input)
                    qld_fc = _annotate_features(qld_fc, "QLD")
                    combined["features"].extend(qld_fc.get("features", []))
                    cnt = len(qld_fc.get("features", []))
                    per_state_msgs.append(("success", f"QLD: {cnt} feature(s)."))
                    any_success = any_success or cnt > 0
                except Exception as e:
                    per_state_msgs.append(("error", f"QLD error: {e}"))

        # SA
        if sel_sa:
            if query_sa is None:
                per_state_msgs.append(("warning",
                    "SA: query function not found. Add `sa_query.py` with `query_sa(user_input)` or update the import path."
                ))
            else:
                try:
                    sa_fc = query_sa(user_input)
                    sa_fc = _annotate_features(sa_fc, "SA")
                    combined["features"].extend(sa_fc.get("features", []))
                    cnt = len(sa_fc.get("features", []))
                    per_state_msgs.append(("success", f"SA: {cnt} feature(s)."))
                    any_success = any_success or cnt > 0
                except Exception as e:
                    per_state_msgs.append(("error", f"SA error: {e}"))

        # Surface per-state messages
        for level, msg in per_state_msgs:
            getattr(st, level)(msg)

        # Save + show combined results (if any)
        if combined["features"]:
            st.session_state["current_layer_name"] = "Combined"
            st.session_state["current_geojson"] = combined
            st.success(f"Loaded total {len(combined['features'])} feature(s) from selected states.")
        else:
            st.session_state["current_layer_name"] = None
            st.session_state["current_geojson"] = None
            st.warning("No features found for the selected states and input.")

# ---------------------
# Map
# ---------------------
st.markdown("### Map")
fc = st.session_state.get("current_geojson")
if not fc or not fc.get("features"):
    st.info("No features loaded yet. Tick states, enter a parcel reference, and click **Search**.")
else:
    st.pydeck_chart(
        pdk.Deck(
            layers=[
                pdk.Layer(
                    "GeoJsonLayer",
                    fc,
                    pickable=True,
                    stroked=True,
                    filled=True,
                    extruded=False,
                    lineWidthMinPixels=1,
                    get_line_color=[0, 0, 0, 255],
                    get_fill_color=[0, 128, 255, 60],
                )
            ],
            initial_view_state=_fit_view(fc),
            tooltip={"html": "<b>{name}</b>", "style": {"backgroundColor": "white", "color": "black"}},
            map_style=None,
        )
    )

# ---------------------
# Sidebar: Downloads
# ---------------------
with st.sidebar:
    st.header("Downloads")
    if st.session_state.get("current_geojson"):
        fc = st.session_state["current_geojson"]
        if st.button("Generate KML"):
            try:
                kml_bytes = io.BytesIO()
                kml_bytes.write(create_kml_from_feature_collection(fc))
                kml_bytes.seek(0)
                st.download_button(
                    "Download KML",
                    data=kml_bytes,
                    file_name="parcels.kml",
                    mime="application/vnd.google-earth.kml+xml",
                )
            except Exception as e:
                st.error(f"KML build failed: {e}")
    else:
        st.info("Run a search to enable downloads.")

# ---------------------
# Helpers: view + KML
# ---------------------
def _fit_view(feature_collection: dict) -> pdk.ViewState:
    """Compute a reasonable view; if centroid available on first feature, zoom into it."""
    vs = pdk.ViewState(latitude=-25.2744, longitude=133.7751, zoom=4)
    try:
        f0 = feature_collection["features"][0]
        lon, lat = _approx_centroid(f0.get("geometry", {}))
        if lon is not None and lat is not None:
            return pdk.ViewState(latitude=lat, longitude=lon, zoom=14)
    except Exception:
        pass
    return vs

def _approx_centroid(geometry: dict):
    """Very rough centroid for Polygon/MultiPolygon."""
    try:
        if geometry["type"] == "Polygon":
            coords = geometry["coordinates"][0]
            lon = sum(x for x, y in coords) / len(coords)
            lat = sum(y for x, y in coords) / len(coords)
            return lon, lat
        elif geometry["type"] == "MultiPolygon":
            coords = geometry["coordinates"][0][0]
            lon = sum(x for x, y in coords) / len(coords)
            lat = sum(y for x, y in coords) / len(coords)
            return lon, lat
    except Exception:
        return None, None
    return None, None

def create_kml_from_feature_collection(feature_collection: dict) -> bytes:
    """
    Build a simple KML from a GeoJSON FeatureCollection (WGS84).
    Uses build_kml_feature_name and build_kml_balloon for labels.
    """
    from xml.sax.saxutils import escape

    def ring_to_kml(ring):
        return " ".join([f"{x},{y},0" for x, y in ring])

    def poly_to_kml(geom: dict) -> str:
        rings = geom["coordinates"]
        outer = ring_to_kml(rings[0])
        inner = "".join([
            f"<innerBoundaryIs><LinearRing><coordinates>{ring_to_kml(r)}</coordinates></LinearRing></innerBoundaryIs>"
            for r in rings[1:]
        ])
        return f"<outerBoundaryIs><LinearRing><coordinates>{outer}</coordinates></LinearRing></outerBoundaryIs>{inner}"

    kml_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<kml xmlns="http://www.opengis.net/kml/2.2">',
        "<Document>",
        "<name>Parcels</name>",
    ]

    for f in feature_collection.get("features", []):
        props = f.get("properties") or f.get("attributes") or {}
        name = build_kml_feature_name(props)
        balloon = build_kml_balloon(props)
        geom = f.get("geometry", {})
        gtype = geom.get("type")
        if gtype == "Polygon":
            body = f"<Polygon><extrude>0</extrude><altitudeMode>clampToGround</altitudeMode>{poly_to_kml(geom)}</Polygon>"
        elif gtype == "MultiPolygon":
            parts = []
            for poly in geom["coordinates"]:
                poly_geom = {"type": "Polygon", "coordinates": poly}
                parts.append(f"<Polygon><extrude>0</extrude><altitudeMode>clampToGround</altitudeMode>{poly_to_kml(poly_geom)}</Polygon>")
            body = "".join(parts)
        else:
            # skip non-polygon for now
            continue

        kml_parts.append(
            f"<Placemark><name>{escape(str(name))}</name>"
            f"<description><![CDATA[{balloon}]]></description>"
            f"{body}</Placemark>"
        )

    kml_parts.append("</Document></kml>")
    return "\n".join(kml_parts).encode("utf-8")
