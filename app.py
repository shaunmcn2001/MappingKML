import streamlit as st
from streamlit_option_menu import option_menu

st.set_page_config(page_title="Parcel Viewer", layout="wide")

with open("style.css") as css_file:
    st.markdown(f"<style>{css_file.read()}</style>", unsafe_allow_html=True)

import requests, folium, pandas as pd, re
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from st_aggrid.shared import ColumnsAutoSizeMode

from kml_utils import (
    _hex_to_kml_color,
    generate_kml,
    generate_shapefile,
    get_bounds,
)

# Layout with side panel similar to kepler.gl
side_col, map_col = st.columns([1, 3], gap="small")

with side_col:
    st.markdown('<div class="side-panel">', unsafe_allow_html=True)
    selected_tab = option_menu(
        None,
        ["Layers", "Filters", "Search", "Interactions", "Basemap"],
        icons=["layers", "funnel-fill", "search", "cursor", "map"],
        default_index=2,
        orientation="vertical",
    )

    if selected_tab == "Search":
        with st.expander("Search Parcels", expanded=True):
            with st.form("search_form"):
                bulk_query = st.text_area(
                    "Parcel search (bulk):",
                    "",
                    help="Enter Lot/Plan (QLD) or Lot/Section/Plan (NSW) one per line.",
                )
                submit = st.form_submit_button("Search")
        if submit:
            inputs = [line.strip() for line in bulk_query.splitlines() if line.strip()]
            all_feats = []
            all_regions = []
            with st.spinner("Searching..."):
                for user_input in inputs:
                    lot_str = sec_str = plan_str = ""
                    if "/" in user_input:
                        region = "NSW"
                        parts = user_input.split("/")
                        if len(parts) == 3:
                            lot_str, sec_str, plan_str = (
                                parts[0].strip(),
                                parts[1].strip(),
                                parts[2].strip(),
                            )
                        elif len(parts) == 2:
                            lot_str, sec_str, plan_str = (
                                parts[0].strip(),
                                "",
                                parts[1].strip(),
                            )
                        else:
                            lot_str, sec_str, plan_str = "", "", ""
                    if sec_str == "" and "//" in user_input:
                        lot_str, plan_str = user_input.split("//")
                        sec_str = ""
                    plan_num = "".join(filter(str.isdigit, plan_str))
                    if lot_str == "" or plan_num == "":
                        continue
                    where_clauses = [f"lotnumber='{lot_str}'"]
                    if sec_str:
                        where_clauses.append(f"sectionnumber='{sec_str}'")
                    else:
                        where_clauses.append(
                            "(sectionnumber IS NULL OR sectionnumber = '')"
                        )
                    where_clauses.append(f"plannumber={plan_num}")
                    where = " AND ".join(where_clauses)
                    url = "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_Cadastre/MapServer/9/query"
                    params = {
                        "where": where,
                        "outFields": "lotnumber,sectionnumber,planlabel",
                        "outSR": "4326",
                        "f": "geoJSON",
                    }
                    try:
                        res = requests.get(url, params=params, timeout=10)
                        data = res.json()
                    except Exception as e:
                        data = {}
                    feats = data.get("features", []) or []
                    for feat in feats:
                        all_feats.append(feat)
                        all_regions.append("NSW")
                    region = "QLD"
                    inp = user_input.replace(" ", "").upper()
                    match = re.match(r"^(\d+)([A-Z].+)$", inp)
                    if not match:
                        continue
                    lot_str = match.group(1)
                    plan_str = match.group(2)
                    url = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/PlanningCadastre/LandParcelPropertyFramework/MapServer/4/query"
                    params = {
                        "where": f"lot='{lot_str}' AND plan='{plan_str}'",
                        "outFields": "lot,plan,lotplan,locality",
                        "outSR": "4326",
                        "f": "geoJSON",
                    }
                    try:
                        res = requests.get(url, params=params, timeout=10)
                        data = res.json()
                    except Exception as e:
                        data = {}
                    feats = data.get("features", []) or []
                    for feat in feats:
                        all_feats.append(feat)
                        all_regions.append("QLD")
            st.session_state["features"] = all_feats
            st.session_state["regions"] = all_regions
            st.success(f"Found {len(all_feats)} parcels.")

        if st.session_state.get("features"):
            with st.expander("Styling Options", expanded=True):
                fc_col, fo_col = st.columns([2, 1])
                with fc_col:
                    fill_color = st.color_picker(
                        "Fill color", "#FF0000", key="fill_color"
                    )
                with fo_col:
                    fill_opacity = st.number_input(
                        "Opacity",
                        min_value=0.0,
                        max_value=1.0,
                        value=0.5,
                        step=0.01,
                        key="fill_opacity",
                    )

                oc_col, ow_col = st.columns([2, 1])
                with oc_col:
                    outline_color = st.color_picker(
                        "Outline color", "#000000", key="outline_color"
                    )
                with ow_col:
                    outline_weight = st.number_input(
                        "Weight",
                        min_value=1,
                        max_value=10,
                        value=2,
                        step=1,
                        key="outline_weight",
                    )
            with st.expander("Export Options", expanded=True):
                folder_name = st.text_input(
                    "KML Folder Name", value="Parcels", key="folder_name"
                )
                data = []
                for i, feat in enumerate(st.session_state["features"]):
                    props = feat.get("properties", {})
                    if st.session_state["regions"][i] == "QLD":
                        data.append(
                            {"Lot": props.get("lot"), "Plan": props.get("plan")}
                        )
                    else:
                        data.append(
                            {
                                "Lot": props.get("lotnumber"),
                                "Plan": props.get("planlabel", ""),
                            }
                        )
                df = pd.DataFrame(data)
                gb = GridOptionsBuilder.from_dataframe(df)
                gb.configure_column("Lot", headerName="Lot", editable=False)
                gb.configure_column("Plan", headerName="Plan", editable=False)
                gb.configure_selection(selection_mode="multiple", use_checkbox=True)
                gb.configure_pagination(paginationAutoPageSize=True)
                gridOptions = gb.build()
                grid_resp = AgGrid(
                    df,
                    gridOptions=gridOptions,
                    height=300,
                    update_mode=GridUpdateMode.SELECTION_CHANGED,
                    theme="streamlit",
                    fit_columns_on_grid_load=True,
                    columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
                )
                sel_rows = grid_resp.get("selected_rows", [])
                selected_features = []
                for sel in sel_rows:
                    for i, feat in enumerate(st.session_state["features"]):
                        props = feat.get("properties", {})
                        if st.session_state["regions"][i] == "QLD":
                            if (
                                props.get("lot") == sel["Lot"]
                                and props.get("plan") == sel["Plan"]
                            ):
                                selected_features.append(feat)
                                break
                        else:
                            if (
                                props.get("lotnumber") == sel["Lot"]
                                and props.get("planlabel") == sel["Plan"]
                            ):
                                selected_features.append(feat)
                                break
                export_region = (
                    "QLD"
                    if "QLD" in st.session_state["regions"]
                    else ("NSW" if "NSW" in st.session_state["regions"] else "QLD")
                )
                with st.spinner("Preparing KML..."):
                    st.download_button(
                        "Download KML",
                        data=generate_kml(
                            selected_features or st.session_state["features"],
                            export_region,
                            fill_color,
                            fill_opacity,
                            outline_color,
                            outline_weight,
                            folder_name,
                        ),
                        file_name="parcels.kml",
                    )
                with st.spinner("Preparing SHP..."):
                    st.download_button(
                        "Download SHP",
                        data=generate_shapefile(
                            selected_features or st.session_state["features"],
                            export_region,
                        ),
                        file_name="parcels.zip",
                    )
    else:
        st.info(f"{selected_tab} options go here.")
    st.markdown('</div>', unsafe_allow_html=True)

with map_col:
    base_map = folium.Map(
        location=[-23.5, 143.0], zoom_start=5, tiles=None, zoomControl=True
    )
    folium.TileLayer(
        "OpenStreetMap", name="OpenStreetMap", control=True
    ).add_to(base_map)
    folium.TileLayer(
        "CartoDB positron", name="CartoDB Positron", control=True
    ).add_to(base_map)
    folium.TileLayer(
        "CartoDB dark_matter", name="CartoDB Dark", control=True
    ).add_to(base_map)
    folium.TileLayer(
        tiles="https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
        attr="Google",
        name="Google Satellite",
        control=True,
    ).add_to(base_map)
    if st.session_state.get("features") and st.session_state["features"]:
        features = st.session_state["features"]
        fill_color = st.session_state.get("fill_color", "#FF0000")
        outline_color = st.session_state.get("outline_color", "#000000")
        opacity = st.session_state.get("fill_opacity", 0.5)
        weight = st.session_state.get("outline_weight", 2)
        folium.GeoJson(
            data={"type": "FeatureCollection", "features": features},
            name="Parcels",
            style_function=lambda feat: {
                "fillColor": fill_color,
                "color": outline_color,
                "weight": weight,
                "fillOpacity": opacity,
            },
        ).add_to(base_map)
        bounds = get_bounds(features)
        base_map.fit_bounds(bounds)
    else:
        base_map.fit_bounds([[-39, 137], [-9, 155]])
    folium.LayerControl(collapsed=False).add_to(base_map)
    map_html = base_map._repr_html_()
    st.components.v1.html(map_html, height=700, width=None, scrolling=True)
