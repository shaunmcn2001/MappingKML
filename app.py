# app.py
import json
import io
import streamlit as st
import pydeck as pdk

from nsw_query import query_nsw_lsp, NSWQueryError
from kml_utils import build_kml_feature_name, build_kml_balloon

st.set_page_config(page_title="MappingKML", layout="wide")

# ---- UI Header ----
st.title("MappingKML")
st.caption("Search parcels and export KML — NSW supports **Lot/Section/Plan** (e.g. `3//DP753311`).")

# Session keys
if "current_geojson" not in st.session_state:
    st.session_state["current_geojson"] = None
if "current_layer_name" not in st.session_state:
    st.session_state["current_layer_name"] = None

# ---- Sidebar: Downloads ----
with st.sidebar:
    st.header("Downloads")
    if st.session_state.get("current_geojson"):
        fc = st.session_state["current_geojson"]
        # KML download
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

# ---- Tabs ----
tabs = st.tabs(["QLD", "NSW", "SA", "Map"])

# QLD TAB (placeholder – keep your existing QLD logic here if you have it)
with tabs[0]:
    st.subheader("QLD search")
    st.caption("Use your existing QLD tool here.")
    st.text_input("QLD input", key="qld_input", placeholder="e.g. 3SP181800")

# NSW TAB (active)
with tabs[1]:
    st.subheader("NSW Parcel Search (Lot / Section / Plan)")
    st.caption("Examples: `3//DP753311` (no section), `3/2/DP753311` (with section), or `Lot 3 DP753311`.")

    nsw_raw = st.text_input("Enter NSW lot/section/plan", placeholder="3//DP753311", key="nsw_input_lsp")
    col_a, col_b = st.columns([1, 2])
    with col_a:
        go = st.button("Search NSW", type="primary")
    with col_b:
        st.write("")

    if go:
        if not nsw_raw.strip():
            st.warning("Please enter a value like `3//DP753311`.")
        else:
            try:
                nsw_fc = query_nsw_lsp(nsw_raw)
                st.session_state["current_layer_name"] = "NSW"
                st.session_state["current_geojson"] = nsw_fc
                count = len(nsw_fc.get("features", []))
                if "note" in nsw_fc:
                    st.info(nsw_fc["note"])
                st.success(f"NSW: {count} feature(s) loaded.")
            except NSWQueryError as e:
                st.error(str(e))
            except Exception as e:
                st.exception(e)

# SA TAB (placeholder – keep your existing SA logic here)
with tabs[2]:
    st.subheader("SA search")
    st.caption("Use your existing SA tool here.")
    st.text_input("SA input", key="sa_input")

# MAP TAB
with tabs[3]:
    st.subheader("Map")
    fc = st.session_state.get("current_geojson")
    if not fc or not fc.get("features"):
        st.info("No features loaded yet. Run a search in QLD/NSW/SA tab.")
    else:
        st.pydeck_chart(make_deck(fc))


# -----------------------
# Helpers: Map + KML
# -----------------------

def make_deck(feature_collection: dict) -> pdk.Deck:
    """Render a GeoJSON FeatureCollection (WGS84) with pydeck."""
    layer = pdk.Layer(
        "GeoJsonLayer",
        feature_collection,
        pickable=True,
        stroked=True,
        filled=True,
        extruded=False,
        lineWidthMinPixels=1,
        get_line_color=[0, 0, 0, 255],
        get_fill_color=[0, 128, 255, 60],
    )
    # Compute a reasonable view (fallback to AU if no bbox)
    view_state = pdk.ViewState(latitude=-25.2744, longitude=133.7751, zoom=4)
    try:
        # Attempt to fit to first feature's centroid (lightweight)
        f0 = feature_collection["features"][0]
        geom = f0.get("geometry", {})
        lon, lat = _approx_centroid(geom)
        if lon is not None and lat is not None:
            view_state = pdk.ViewState(latitude=lat, longitude=lon, zoom=14)
    except Exception:
        pass

    tooltip = {"html": "<b>{name}</b>", "style": {"backgroundColor": "white", "color": "black"}}
    return pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip, map_style=None)


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
    Build a simple KML from a GeoJSON FeatureCollection.
    Uses build_kml_feature_name and build_kml_balloon for labels.
    """
    from xml.sax.saxutils import escape

    def coord_str(geom: dict) -> str:
        def ring_to_kml(ring):
            return " ".join([f"{x},{y},0" for x, y in ring])
        if geom["type"] == "Polygon":
            rings = geom["coordinates"]
            outer = ring_to_kml(rings[0])
            inner = "".join([f"<innerBoundaryIs><LinearRing><coordinates>{ring_to_kml(r)}</coordinates></LinearRing></innerBoundaryIs>"
                             for r in rings[1:]])
            return f"<outerBoundaryIs><LinearRing><coordinates>{outer}</coordinates></LinearRing></outerBoundaryIs>{inner}"
        elif geom["type"] == "MultiPolygon":
            parts = []
            for poly in geom["coordinates"]:
                rings = poly
                outer = ring_to_kml(rings[0])
                inner = "".join([f"<innerBoundaryIs><LinearRing><coordinates>{ring_to_kml(r)}</coordinates></LinearRing></innerBoundaryIs>"
                                 for r in rings[1:]])
                parts.append(f"<Polygon><extrude>0</extrude><altitudeMode>clampToGround</altitudeMode>"
                             f"<outerBoundaryIs><LinearRing><coordinates>{outer}</coordinates></LinearRing></outerBoundaryIs>{inner}</Polygon>")
            return "".join(parts)
        else:
            return ""

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
        if geom.get("type") == "Polygon":
            poly = f"<Polygon><extrude>0</extrude><altitudeMode>clampToGround</altitudeMode>{coord_str(geom)}</Polygon>"
            geom_kml = poly
        elif geom.get("type") == "MultiPolygon":
            geom_kml = coord_str(geom)
        else:
            # skip non-polygon for now
            continue
        kml_parts.append(
            f"<Placemark><name>{escape(str(name))}</name>"
            f"<description><![CDATA[{balloon}]]></description>"
            f"{geom_kml}"
            f"</Placemark>"
        )

    kml_parts.append("</Document></kml>")
    return "\n".join(kml_parts).encode("utf-8")
