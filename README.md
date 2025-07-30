# MappingKML — Streamlit Mapbox Viewer

This version uses **Mapbox** via `pydeck` to display query results on an interactive map. The sidebar provides a Lot/Plan search bar and simple export options.

## Install & Run
```bash
pip install -r requirements.txt
streamlit run app.py
```

Using the Query bar
•Paste your Lot/Plan pattern (e.g., 169-173, 203, 220, 246, 329//DP753311 or 1RP912949).
•Wire your existing ArcGIS/Qld cadastral query inside run_lotplan_query() to return a GeoJSON FeatureCollection (polygons preferred).
Notes
•Searches call `run_lotplan_query()` which should return a GeoJSON FeatureCollection.
•Results appear on the map and can be exported as KML.

---

### 6) Commit and push
```bash
git add -A
git commit -m "feat: Kepler.gl layout in Streamlit + Query bar; KML upload; dataset plumbing"
git push -u origin feat/kepler-layout-with-query
```
