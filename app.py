"""
Lot/Plan  ➜  styled KML   (full-screen Leaflet UI)
--------------------------------------------------
• paste Lot/Plan IDs in the sidebar
• pick basemap in the sidebar (Esri Imagery / Esri Topo / OSM)
• ArcGIS address search + orange ruler on the map
• map fills the entire browser window
• download a colour-styled KML of the parcels
"""

import io, re, requests
import streamlit as st
from streamlit_folium import st_folium
import folium, simplekml
from folium.plugins import Geocoder, Fullscreen
from shapely.geometry import shape, mapping
from shapely.ops import unary_union, transform
from pyproj import Transformer

# ─── ArcGIS cadastral endpoints ────────────────────────────────
QLD_URL = ("https://spatial-gis.information.qld.gov.au/arcgis/rest/services/"
           "PlanningCadastre/LandParcelPropertyFramework/MapServer/4/query")
NSW_URL = ("https://maps.six.nsw.gov.au/arcgis/rest/services/public/"
           "NSW_Cadastre/MapServer/9/query")

def fetch_merged_geom(lotplan: str):
    """Return one merged Shapely geometry (WGS-84) or None."""
    is_qld = bool(re.match(r"^\d+[A-Z]{1,3}\d+$", lotplan, re.I))
    url, fld = (QLD_URL, "lotplan") if is_qld else (NSW_URL, "lotidstring")
    js = requests.get(url, params={
        "where": f"{fld}='{lotplan}'",
        "returnGeometry": "true",
        "f": "geojson"},
        timeout=15).json()

    feats = js.get("features", [])
    if not feats:
        return None

    parts = []
    for f in feats:
        g   = shape(f["geometry"])
        wkid = f["geometry"].get("spatialReference", {}).get("wkid", 4326)
        if wkid != 4326:                           # re-project to WGS-84
            g = transform(Transformer.from_crs(wkid, 4326, always_xy=True).transform, g)
        parts.append(g)

    return unary_union(parts)


def kml_colour(hex_rgb: str, pct: int) -> str:
    """Convert '#rrggbb' + opacity% ➜ KML colour 'aabbggrr'."""
    r, g, b = hex_rgb[1:3], hex_rgb[3:5], hex_rgb[5:7]
    a = int(round(255 * pct / 100))
    return f"{a:02x}{b}{g}{r}"


# ─── Streamlit layout: 100 % map + retractable sidebar −──────────────
st.set_page_config(page_title="Lot/Plan → KML", layout="wide")

with st.sidebar:
    st.title("≡ Controls")
    lot_text  = st.text_area("Lot/Plan IDs", height=160,
                             placeholder="6RP702264\n5//DP123456")
    basemap_choice = st.selectbox(
        "Basemap",
        {
            "Esri Imagery (satellite)": "ESRI_IMG",
            "Esri Topo":                "ESRI_TOPO",
            "OpenStreetMap":            "OSM",
        }
    )
    poly_hex  = st.color_picker("Fill colour", "#ff6600")
    poly_op   = st.number_input("Fill opacity (0-100 %)", 0, 100, 70)
    line_hex  = st.color_picker("Outline colour", "#2e2e2e")
    line_w    = st.number_input("Outline width (px)", 0.5, 6.0, 1.2, step=0.1)
    folder    = st.text_input("Folder name in KML", "Parcels")
    do_search = st.button("🔍 Search lots", use_container_width=True)

# ─── fetch geometries if requested ───────────────────────────────────
if do_search and lot_text.strip():
    ids   = [i.strip() for i in lot_text.splitlines() if i.strip()]
    geoms = {};   missing = []
    with st.spinner("Fetching parcels…"):
        for lp in ids:
            g = fetch_merged_geom(lp)
            (geoms if g else missing.append(lp)) and (geoms.update({lp: g}) if g else None)

    if missing:
        st.sidebar.warning("Not found: " + ", ".join(missing))

    st.session_state["lot_geoms"] = geoms
    st.session_state["style"] = dict(
        fill=poly_hex, op=poly_op, line=line_hex,
        w=line_w, folder=folder or "Parcels",
        basemap=basemap_choice,
    )

# ─── build full-screen Leaflet map ───────────────────────────────────
m = folium.Map(location=[-25, 145], zoom_start=5,
               control_scale=True, width="100%", height="100vh")

# --- basemap selected in sidebar ---
choice = st.session_state.get("style", {}).get("basemap", "ESRI_IMG")
if choice == "ESRI_IMG":
    folium.TileLayer(
        tiles=("https://services.arcgisonline.com/ArcGIS/rest/services/"
               "World_Imagery/MapServer/tile/{z}/{y}/{x}"),
        attr="© Esri", name="Esri Imagery").add_to(m)
elif choice == "ESRI_TOPO":
    folium.TileLayer(
        tiles=("https://services.arcgisonline.com/ArcGIS/rest/services/"
               "World_Topo_Map/MapServer/tile/{z}/{y}/{x}"),
        attr="© Esri", name="Esri Topo").add_to(m)
else:
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap").add_to(m)

# address search & full-screen buttons
Geocoder(collapsed=False, add_marker=True, provider="esri",
         position="topleft").add_to(m)
Fullscreen(position="topleft").add_to(m)

# --- prettier measurement (Leaflet-EasyMeasure) ---
folium.JavascriptLink(
    "https://cdn.jsdelivr.net/npm/leaflet-easymeasure@2.4.0/dist/leaflet-easymeasure.min.js"
).add_to(m)
folium.CssLink(
    "https://cdn.jsdelivr.net/npm/leaflet-easymeasure@2.4.0/dist/leaflet-easymeasure.min.css"
).add_to(m)
folium.Element("""
<script>
L.control.measure({
  primaryLengthUnit:'kilometers',
  primaryAreaUnit:'hectares',
  activeColor:'#e83015',
  completedColor:'#e83015'}).addTo({{this._parent.get_name()}});
</script>
""").add_to(m)

# --- add parcels ---
if "lot_geoms" in st.session_state and st.session_state["lot_geoms"]:
    s = st.session_state["style"]
    style_fn = lambda _:{'fillColor': s['fill'],
                         'color':     s['line'],
                         'weight':    s['w'],
                         'fillOpacity': s['op']/100}
    for lp, g in st.session_state["lot_geoms"].items():
        folium.GeoJson(mapping(g),
                       style_function=style_fn,
                       name=lp).add_child(folium.Popup(lp)).add_to(m)

# optional layer list (shows parcels & current basemap)
folium.LayerControl(position="topright").add_to(m)

# --- render map ---
st_folium(m, height=750, use_container_width=True)

# ─── KML download ────────────────────────────────────────────────────
if ("lot_geoms" in st.session_state and st.session_state["lot_geoms"]
    and st.sidebar.button("📥 Download KML", use_container_width=True)):
    s   = st.session_state["style"]
    kml = simplekml.Kml();  fld = kml.newfolder(name=s["folder"])
    fill_k = kml_colour(s["fill"], s["op"])
    line_k = kml_colour(s["line"], 100)
    for lp, g in st.session_state["lot_geoms"].items():
        p = fld.newpolygon(name=lp,
                           outerboundaryis=mapping(g)["coordinates"][0])
        p.style.polystyle.color = fill_k
        p.style.linestyle.color = line_k
        p.style.linestyle.width = float(s["w"])
    st.sidebar.download_button(
        "Save KML",
        io.BytesIO(kml.kml().encode()).getvalue(),
        "parcels.kml",
        "application/vnd.google-earth.kml+xml",
        use_container_width=True,
    )
