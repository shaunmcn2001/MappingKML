import io, re, requests, streamlit as st
from collections import defaultdict
from streamlit_folium import st_folium
import folium, simplekml
from shapely.geometry import shape, mapping
from shapely.ops import unary_union, transform
from pyproj import Transformer, Geod

# ─── ArcGIS parcel endpoints ─────────────────────────────────────────
QLD_URL = ("https://spatial-gis.information.qld.gov.au/arcgis/rest/services/"
           "PlanningCadastre/LandParcelPropertyFramework/MapServer/4/query")
NSW_URL = ("https://maps.six.nsw.gov.au/arcgis/rest/services/public/"
           "NSW_Cadastre/MapServer/9/query")

geod = Geod(ellps="WGS84")

# ─── helpers ─────────────────────────────────────────────────────────
def fetch_geoms(lotplans):
    grouped, missing = defaultdict(list), []
    is_qld = lambda lp: bool(re.match(r"^\d+[A-Z]{1,3}\d+$", lp, re.I))

    for lp in lotplans:
        url, fld = (QLD_URL, "lotplan") if is_qld(lp) else (NSW_URL, "lotidstring")
        try:
            js = requests.get(url, params={
                "where": f"{fld}='{lp}'",
                "returnGeometry": "true", "f": "geojson"},
                timeout=12).json()

            feats = js.get("features", [])
            if not feats:
                missing.append(lp)
                continue

            wkid = js.get("spatialReference", {}).get("wkid") \
                   or feats[0]["geometry"].get("spatialReference", {}).get("wkid", 4326)
            tfm = (Transformer.from_crs(wkid, 4326, always_xy=True).transform
                   if wkid != 4326 else None)

            for feat in feats:
                g = shape(feat["geometry"])
                grouped[lp].append(transform(tfm, g) if tfm else g)

        except Exception:
            missing.append(lp)

    merged = {lp: unary_union(gs) for lp, gs in grouped.items()}
    return merged, missing


def kml_colour(hex_rgb, pct):
    r, g, b = hex_rgb[1:3], hex_rgb[3:5], hex_rgb[5:7]
    a = int(round(255 * pct / 100))
    return f"{a:02x}{b}{g}{r}"

# ─── Streamlit UI ────────────────────────────────────────────────────
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

# ─── Fetch / merge lots ─────────────────────────────────────────────
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

# ─── Build map ──────────────────────────────────────────────────────
m = folium.Map(location=[-25, 145], zoom_start=5,
               control_scale=True, width="100%", height="100vh")

# Basemap layers
folium.TileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
                 name="OpenStreetMap", attr="© OpenStreetMap").add_to(m)
folium.TileLayer(
    "https://services.arcgisonline.com/ArcGIS/rest/services/"
    "World_Imagery/MapServer/tile/{z}/{y}/{x}",
    name="Esri Imagery", attr="© Esri").add_to(m)
folium.TileLayer(
    "https://services.arcgisonline.com/ArcGIS/rest/services/"
    "World_Topo_Map/MapServer/tile/{z}/{y}/{x}",
    name="Esri Topo", attr="© Esri").add_to(m)

# Parcels
if "geoms" in st.session_state and st.session_state["geoms"]:
    s = st.session_state["style"]
    sty = lambda _:{'fillColor':s['fill'],'color':s['line'],
                    'weight':s['w'],'fillOpacity':s['op']/100}

    bounds = []  # collect bounds for auto-zoom
    for lp, g in st.session_state["geoms"].items():
        folium.GeoJson(mapping(g), name=lp,
                       style_function=sty).add_child(folium.Popup(lp)).add_to(m)
        bounds.append(g.bounds)

    # ─── Auto-zoom to all parcels ───
    minx = min(b[0] for b in bounds); miny = min(b[1] for b in bounds)
    maxx = max(b[2] for b in bounds); maxy = max(b[3] for b in bounds)
    m.fit_bounds([[miny, minx], [maxy, maxx]])

# Layer switcher
folium.LayerControl(position="topright", collapsed=False).add_to(m)
st_folium(m, height=700, use_container_width=True, key="main_map")

# ─── Download KML ───────────────────────────────────────────────────
if ("geoms" in st.session_state and st.session_state["geoms"]
    and st.sidebar.button("📥 Download KML", use_container_width=True)):
    from shapely.geometry import Polygon, MultiPolygon
    from shapely.geometry.polygon import orient

    s, kml = st.session_state["style"], simplekml.Kml()
    root   = kml.newfolder(name=s["folder"])
    fill_k, line_k = kml_colour(s["fill"], s["op"]), kml_colour(s["line"], 100)

    for lp, geom in st.session_state["geoms"].items():
        polys = [geom] if isinstance(geom, Polygon) else list(geom.geoms)
        for idx, poly in enumerate(polys, start=1):
            poly = orient(poly, sign=1.0)  # CCW
            name = f"{lp} ({idx})" if len(polys) > 1 else lp
            area_ha = abs(geod.geometry_area_perimeter(poly)[0]) / 1e4
            desc = f"Lot/Plan: {lp}<br>Area: {area_ha:,.2f} ha"

            p = root.newpolygon(
                name=name,
                description=desc,
                outerboundaryis=[(x, y) for x, y in poly.exterior.coords],
            )
            for ring in poly.interiors:
                p.innerboundaryis.append([(x, y) for x, y in ring.coords])

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
