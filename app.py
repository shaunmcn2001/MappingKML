#!/usr/bin/env python3
# LAWD Parcel Toolkit  · 2025-07-12

import io, pathlib, requests, tempfile, zipfile, re
import streamlit as st
from streamlit_option_menu import option_menu
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import folium, simplekml, geopandas as gpd, pandas as pd
from streamlit_folium import st_folium
from shapely.geometry import shape, mapping, Polygon
from shapely.ops import unary_union, transform
from pyproj import Transformer, Geod

# ───────── STATIC CONFIG ─────────────────────────────
CFG = pathlib.Path("layers.yaml")
try:
    import yaml
    cfg = yaml.safe_load(CFG.read_text()) if CFG.exists() else {}
except ImportError:
    cfg = {}
for k in ("basemaps", "overlays"):
    cfg.setdefault(k, [])

# ───────── STREAMLIT SHELL & NAVIGATION ──────────────
st.set_page_config("Lot/Plan Toolkit", "📍", layout="wide",
                   initial_sidebar_state="collapsed")
st.markdown(
    "<div style='background:#ff6600;color:#fff;font-size:20px;"
    "padding:6px 20px;border-radius:8px;margin-bottom:6px'>"
    "LAWD – Parcel Toolkit</div>",
    unsafe_allow_html=True
)
with st.sidebar:
    tab = option_menu(None, ["Query", "Layers", "Downloads"],
                      icons=["search", "layers", "download"],
                      default_index=0,
                      styles={"container":{"padding":"0","background":"#262730"},
                              "nav-link-selected":{"background":"#ff6600"}})

if cfg["basemaps"]:
    st.session_state.setdefault("basemap", cfg["basemaps"][0]["name"])
st.session_state.setdefault("ov_state", {o["name"]: False for o in cfg["overlays"]})

# ───────── CADASTRE SERVICES ──────────────────────────
QLD = (
    "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/"
    "PlanningCadastre/LandParcelPropertyFramework/MapServer/4/query"
)
NSW = (
    "https://maps.six.nsw.gov.au/arcgis/rest/services/public/"
    "NSW_Cadastre/MapServer/9/query"
)
GEOD = Geod(ellps="WGS84")

def fetch_parcels(ids):
    out, miss = {}, []
    for lp in ids:
        url, fld = (QLD, "lotplan") if re.match(r"^\d+[A-Z]{1,3}\d+$", lp, re.I) \
                   else (NSW, "lotidstring")
        try:
            js = requests.get(url, params={
                "where": f"{fld}='{lp}'",
                "outFields": "*",
                "returnGeometry": "true",
                "f": "geojson"
            }, timeout=15).json()
            feats = js.get("features", [])
            if not feats:
                miss.append(lp)
                continue
            wkid = feats[0]["geometry"].get("spatialReference", {}).get("wkid", 4326)
            tfm = Transformer.from_crs(wkid, 4326, always_xy=True).transform if wkid != 4326 else None
            polys, props = [], {}
            for ft in feats:
                g = shape(ft["geometry"])
                polys.append(transform(tfm, g) if tfm else g)
                props = ft["properties"]
            out[lp] = {"geom": unary_union(polys), "props": props}
        except Exception:
            miss.append(lp)
    return out, miss

def kml_colour(hexrgb, pct):
    r, g, b = hexrgb[1:3], hexrgb[3:5], hexrgb[5:7]
    a = int(round(255 * pct / 100))
    return f"{a:02x}{b}{g}{r}"

# ───────── TAB: QUERY ──────────────────────────────────
if tab == "Query":
    ids_txt = st.sidebar.text_area("Lot/Plan IDs", height=120,
                                   placeholder="6RP702264\n5//DP123456")
    with st.sidebar.expander("Style & KML"):
        c1, c2 = st.columns(2, gap="small")
        with c1:
            fx = st.color_picker("Fill", "#ff6600", label_visibility="collapsed")
            lx = st.color_picker("Outline", "#2e2e2e", label_visibility="collapsed")
        with c2:
            fo = st.slider("Opacity %", 0, 100, 70, label_visibility="collapsed")
            lw = st.slider("Width px", 0.5, 6.0, 1.2, 0.1, label_visibility="collapsed")
        folder = st.text_input("KML folder", "Parcels")

    if st.sidebar.button("🔍 Search", use_container_width=True) and ids_txt.strip():
        ids = [s.strip() for s in ids_txt.splitlines() if s.strip()]
        with st.spinner("Fetching parcels…"):
            recs, miss = fetch_parcels(ids)
        if miss:
            st.sidebar.warning("Not found: " + ", ".join(miss))

        rows = []
        for lp, rec in recs.items():
            props = rec["props"]
            ltype = props.get("lottype") or props.get("PURPOSE") or "n/a"
            area = abs(GEOD.geometry_area_perimeter(rec["geom"])[0]) / 1e4
            rows.append({"Lot/Plan": lp, "Lot Type": ltype, "Area (ha)": round(area, 2)})

        st.session_state.update(
            parcels=recs,
            table=pd.DataFrame(rows),
            style=dict(fill=fx, op=fo, line=lx, w=lw, folder=folder)
        )
        st.success(f"{len(recs)} parcel{'s'*(len(recs)!=1)} loaded.")

# ───────── TAB: LAYERS ─────────────────────────────────
if tab == "Layers":
    if cfg["basemaps"]:
        st.sidebar.subheader("Basemap")
        names = [b["name"] for b in cfg["basemaps"]]
        st.session_state["basemap"] = st.sidebar.radio(
            "", names, index=names.index(st.session_state["basemap"])
        )
    st.sidebar.subheader("Static overlays")
    for o in cfg["overlays"]:
        st.session_state["ov_state"][o["name"]] = st.sidebar.checkbox(
            o["name"], value=st.session_state["ov_state"][o["name"]]
        )

# ───────── TABLE + ACTION BUTTONS ──────────────────────
if "table" in st.session_state and not st.session_state["table"].empty:
    st.subheader("Query Results")

    # build GeoDataFrame
    gdf = gpd.GeoDataFrame(
        st.session_state["table"],
        geometry=[r["geom"] for r in st.session_state["parcels"].values()],
        crs=4326
    )

    gob = GridOptionsBuilder.from_dataframe(gdf.drop(columns="geometry"))
    gob.configure_selection("multiple", use_checkbox=True)
    grid_resp = AgGrid(
        gdf.drop(columns="geometry"),
        gridOptions=gob.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        height=250,
    )

    sel = grid_resp.get("selected_rows", [])
    ids = [r["Lot/Plan"] for r in sel]

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        if st.button("🔍 Zoom to selection", disabled=not ids):
            bb = gpd.GeoSeries([st.session_state["parcels"][i]["geom"] for i in ids]).total_bounds
            st.session_state["zoom_to"] = [[bb[1], bb[0]], [bb[3], bb[2]]]

    with c2:
        if st.button("💾 Export selection (KML)", disabled=not ids):
            s = st.session_state["style"]
            fk, lk = kml_colour(s["fill"], s["op"]), kml_colour(s["line"], 100)
            kml = simplekml.Kml()
            for lp in ids:
                geom = st.session_state["parcels"][lp]["geom"]
                poly = kml.newpolygon(
                    name=lp,
                    outerboundaryis=(
                        geom.exterior.coords if isinstance(geom, Polygon)
                        else list(geom.geoms)[0].exterior.coords
                    )
                )
                poly.style.polystyle.color = fk
                poly.style.linestyle.color = lk
                poly.style.linestyle.width = float(s["w"])
            st.download_button(
                "Download selection.kml",
                io.BytesIO(kml.kml().encode()),
                "selection.kml",
                "application/vnd.google-earth.kml+xml"
            )

    with c3:
        if st.button("🗑️ Remove selection", disabled=not ids):
            for lp in ids:
                st.session_state["parcels"].pop(lp, None)
            st.session_state["table"] = st.session_state["table"][
                ~st.session_state["table"]["Lot/Plan"].isin(ids)
            ]
            st.session_state.pop("zoom_to", None)

    with c4:
        if st.button("📦 Export ALL (KML)", disabled=st.session_state["table"].empty):
            s = st.session_state["style"]
            fk, lk = kml_colour(s["fill"], s["op"]), kml_colour(s["line"], 100)
            kml = simplekml.Kml()
            fld = kml.newfolder(name=s["folder"])
            for lp, rec in st.session_state["parcels"].items():
                geom = rec["geom"]
                polys = [geom] if isinstance(geom, Polygon) else list(geom.geoms)
                for i, p in enumerate(polys, 1):
                    nm = f"{lp} ({i})" if len(polys) > 1 else lp
                    poly = fld.newpolygon(name=nm, outerboundaryis=p.exterior.coords)
                    for ring in p.interiors:
                        poly.innerboundaryis.append(ring.coords)
                    poly.style.polystyle.color = fk
                    poly.style.linestyle.color = lk
                    poly.style.linestyle.width = float(s["w"])
            st.download_button(
                "Download parcels.kml",
                io.BytesIO(kml.kml().encode()),
                "parcels.kml",
                "application/vnd.google-earth.kml+xml"
            )

    with c5:
        if st.button("📦 Export ALL (SHP)", disabled=st.session_state["table"].empty):
            tmp = tempfile.mkdtemp()
            gpd.GeoDataFrame(
                st.session_state["table"],
                geometry=[r["geom"] for r in st.session_state["parcels"].values()],
                crs=4326
            ).to_file(tmp + "/all.shp")
            zname = pathlib.Path(tmp, "all.zip")
            with zipfile.ZipFile(zname, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in pathlib.Path(tmp).glob("all.*"):
                    zf.write(f, f.name)
            st.download_button(
                "Download parcels.zip",
                open(zname, "rb").read(),
                "parcels.zip",
                "application/zip"
            )

# ───────── MAP BUILD & ZOOM ───────────────────────────
# build after actions so zoom_to flag is applied immediately
m = folium.Map(location=[-25, 145], zoom_start=5,
               control_scale=True, width="100%", height="100vh")
if "zoom_to" in st.session_state:
    m.fit_bounds(st.session_state.pop("zoom_to"))

# basemap
if cfg["basemaps"]:
    base = next(b for b in cfg["basemaps"] if b["name"] == st.session_state["basemap"])
    folium.TileLayer(base["url"], name=base["name"], attr=base["attr"],
                     overlay=False, control=True, show=True).add_to(m)

# overlays
for o in cfg["overlays"]:
    if st.session_state["ov_state"][o["name"]]:
        try:
            if o["type"] == "wms":
                folium.raster_layers.WmsTileLayer(
                    o["url"], layers=str(o["layers"]), transparent=True,
                    fmt=o.get("fmt", "image/png"), version="1.1.1",
                    name=o["name"], attr=o["attr"]
                ).add_to(m)
            else:
                folium.TileLayer(o["url"], name=o["name"], attr=o["attr"]).add_to(m)
        except Exception as e:
            st.warning(f"{o['name']} failed: {e}")

# parcels overlay
if "parcels" in st.session_state:
    fg = folium.FeatureGroup(name="Parcels", show=True).add_to(m)
    for lp, rec in st.session_state["parcels"].items():
        geom, prop = rec["geom"], rec["props"]
        html = (
            f"<b>Lot/Plan:</b> {lp}<br>"
            f"<b>Lot Type:</b> {prop.get('lottype') or prop.get('PURPOSE') or 'n/a'}<br>"
            f"<b>Area:</b> {abs(GEOD.geometry_area_perimeter(geom)[0])/1e4:,.2f} ha"
        )
        folium.GeoJson(
            {"type":"Feature","properties":{"name":lp},"geometry":mapping(geom)},
            style_function=lambda f, s=st.session_state["style"]: {
                "fillColor": s["fill"], "color": s["line"],
                "weight": s["w"], "fillOpacity": s["op"]/100
            },
            tooltip=lp,
            popup=html
        ).add_to(fg)

# render map
st_folium(m, height=550, use_container_width=True, key="map")
