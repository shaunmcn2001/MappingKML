import io, re, requests, streamlit as st
from collections import defaultdict
from streamlit_folium import st_folium
import folium, simplekml
from shapely.geometry import shape, mapping
from shapely.ops import unary_union, transform
from pyproj import Transformer

# ─── ArcGIS parcel services ──────────────────────────────────────────
QLD_URL = (
    "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/"
    "PlanningCadastre/LandParcelPropertyFramework/MapServer/4/query"
)
NSW_URL = (
    "https://maps.six.nsw.gov.au/arcgis/rest/services/public/"
    "NSW_Cadastre/MapServer/9/query"
)

# ─── Helpers ─────────────────────────────────────────────────────────
def fetch_geoms(lotplans):
    """Return {lotplan: merged_geometry}  and  missing_ids list."""
    grouped = defaultdict(list)
    missing = []

    def is_qld(lp):   # simple pattern: digits + letters + digits
        return bool(re.match(r"^\d+[A-Z]{1,3}\d+$", lp, re.I))

    for lp in lotplans:
        url, fld = (QLD_URL, "lotplan") if is_qld(lp) else (NSW_URL, "lotidstring")
        try:
            js = requests.get(
                url,
                params={
                    "where": f"{fld}='{lp}'",
                    "returnGeometry": "true",
                    "f": "geojson",
                },
                timeout=12,
            ).json()

            feats = js.get("features", [])
            if not feats:
                missing.append(lp)
                continue

            # ── determine WKID ──
            wkid = js.get("spatialReference", {}).get("wkid")
            if wkid is None:
                wkid = feats[0]["geometry"].get("spatialReference", {}).get("wkid", 4326)
            tfm = (Transformer.from_crs(wkid, 4326, always_xy=True).transform
                   if wkid and wkid != 4326 else None)

            for feat in feats:
                geom = shape(feat["geometry"])
                grouped[lp].append(transform(tfm, geom) if tfm else geom)

        except Exception:
            missing.append(lp)

    merged = {lp: unary_union(geoms) for lp, geoms in grouped.items()}
    return merged, missing


def kml_colour(hex_rgb, pct):
    r, g, b = hex_rgb[1:3], hex_rgb[3:5], hex_rgb[5:7]
    a = int(round(255 * pct / 100))
    return f"{a:02x}{b}{g}{r}"  # KML expects aabbggrr

# ─── Streamlit page ─────────────────────────────────────────────────
st.set_page_config(page_title="Lot/Plan → KML", layout="wide")

with st.sidebar:
    st.title("≡ Controls")
    lot_text = st.text_area("Lot/Plan IDs", height=140,
                            placeholder="6RP702264\n5//DP123456")
    fill_hex = st.color_picker("Fill colour", "#ff6600")
    fill_op  = st.number_input("Fill opacity %", 0, 100, 70)
    line_hex = st.color_picker("Outline colour", "#2e2e2e")
    line_w   = st.number_input("Outline width px", 0.5, 6.0, 1.2, step=0.1)
    folder   = st.text_input("Folder name in KML", "Parcels")
    run_btn  = st.button("🔍 Search lots", use_container_width=True)

# ─── Fetch & merge parcels ───────────────────────────────────────────
if run_btn and lot_text.strip():
    ids = [i.strip() for i in lot_text.splitlines() if i.strip()]
    with st.spinner("Fetching & merging parcels…"):
        geoms, missing = fetch_geoms(ids)

    if missing:
        st.sidebar.warning("Not found: " + ", ".join(missing))
    st.sidebar.info(f"Loaded {len(geoms)} parcel"
                    f"{'' if len(geoms)==1 else 's'}.")

    st.session_state["geoms"] = geoms
    st.session_state["style"] = dict(fill=fill_hex, op=fill_op,
                                     line=line_hex, w=line_w,
                                     folder=folder)

# ─── Build map ───────────────────────────────────────────────────────
m = folium.Map(location=[-25, 145], zoom_start=5,
               control_scale=True, width="100%", height="100vh")

# Base layers
folium.TileLayer(
    tiles="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    name="OpenStreetMap", attr="© OpenStreetMap"
).add_to(m)
folium.TileLayer(
    tiles=("https://services.arcgisonline.com/ArcGIS/rest/services/"
           "World_Imagery/MapServer/tile/{z}/{y}/{x}"),
    name="Esri Imagery", attr="© Esri"
).add_to(m)
folium.TileLayer(
    tiles=("https://services.arcgisonline.com/ArcGIS/rest/services/"
           "World_Topo_Map/MapServer/tile/{z}/{y}/{x}"),
    name="Esri Topo", attr="© Esri"
).add_to(m)

# Parcel polygons
if "geoms" in st.session_state and st.session_state["geoms"]:
    s = st.session_state["style"]
    sty = lambda _:{'fillColor': s['fill'],
                    'color':     s['line'],
                    'weight':    s['w'],
                    'fillOpacity': s['op']/100}
    for lp, g in st.session_state["geoms"].items():
        folium.GeoJson(mapping(g), style_function=sty,
                       name=lp).add_child(folium.Popup(lp)).add_to(m)

# Layer switcher (top-right)
folium.LayerControl(position="topright", collapsed=False).add_to(m)

# Render map with stable key to avoid stale-event warnings
st_folium(m, height=700, use_container_width=True, key="main_map")

# ─── Download KML ────────────────────────────────────────────────────
# ─── Download KML ─────────────────────────────────────────
if (
    "geoms" in st.session_state
    and st.session_state["geoms"]
    and st.sidebar.button("📥 Download KML", use_container_width=True)
):
    from shapely.geometry import Polygon, MultiPolygon
    from shapely.geometry.polygon import orient

    sty   = st.session_state["style"]
    kml   = simplekml.Kml()
    root  = kml.newfolder(name=sty["folder"])
    fillk = kml_colour(sty["fill"], sty["op"])
    linek = kml_colour(sty["line"], 100)

    for lp, geom in st.session_state["geoms"].items():
        # Ensure CCW orientation (KML spec)
        geom = orient(geom, sign=1.0)

        if isinstance(geom, Polygon):
            parts = [geom]
        else:                              # MultiPolygon
            parts = list(geom.geoms)

        lp_folder = root.newfolder(name=lp) if len(parts) > 1 else root

        for idx, poly in enumerate(parts, start=1):
            name = f"{lp} ({idx})" if len(parts) > 1 else lp
            p = lp_folder.newpolygon(
                name=name,
                outerboundaryis=[(x, y) for x, y in poly.exterior.coords],
            )
            # holes (if any)
            for ring in poly.interiors:
                p.innerboundaryis.append([(x, y) for x, y in ring.coords])

            p.style.polystyle.color = fillk
            p.style.linestyle.color = linek
            p.style.linestyle.width = float(sty["w"])

    st.sidebar.download_button(
        "Save KML",
        data=io.BytesIO(kml.kml().encode()).getvalue(),
        file_name="parcels.kml",
        mime="application/vnd.google-earth.kml+xml",
        use_container_width=True,
    )
