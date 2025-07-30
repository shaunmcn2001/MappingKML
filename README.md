# MappingKML — Streamlit Mapbox Viewer

This version displays query results on a **Mapbox** map using `pydeck`.  The sidebar lets you search for Lot/Plan polygons and export the results as KML.

## Install & Run
```bash
pip install -r requirements.txt
streamlit run app.py
```

### Usage

* Enter a Lot/Plan pattern (e.g. `169-173, 203 // DP753311` or `1RP912949`).
* Implement `run_lotplan_query()` in `app.py` to call your cadastral service and return a GeoJSON `FeatureCollection`.
* Query results are rendered on the map and can be downloaded as KML.

---

