# app.py — MappingKML
# NSW: layer 9, query ONLY by lotidstring (e.g. 13//DP1246224); separate BULK mode (parallel).
# QLD: NEW bulk mode by LOTPLAN string (e.g. 13SP181800). Per-line QLD still supported.
# SA : planparcel OR title (volume/folio in any order) (unchanged)
# Exports: GeoJSON / KML / KMZ — Google Earth balloons show ALL attributes.

import io
import json
import math
import re
import time
import zipfile
from typing import Dict, List, Optional, Tuple

import concurrent.futures
import requests
import streamlit as st
import pydeck as pdk
from kml_utils import generate_shapefile

# Optional KML export
try:
    import simplekml
    HAVE_SIMPLEKML = True
except Exception:
    HAVE_SIMPLEKML = False

# --------------------- App Config ---------------------

st.set_page_config(page_title="MappingKML", layout="wide")

ENDPOINTS = {
    "QLD": "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Cadastre/LandParcels/MapServer/0/query",
    "NSW": "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_Cadastre/MapServer/9/query",  # Lot layer
    "SA":  "https://dpti.geohub.sa.gov.au/server/rest/services/Hosted/Reference_WFL1/FeatureServer/1/query",
}

DEFAULT_VIEW = pdk.ViewState(latitude=-24.8, longitude=134.0, zoom=4.6, pitch=0, bearing=0)

# Keep UI responsive
REQUEST_TIMEOUT = 12
REQUEST_RETRIES = 0
MAX_WORKERS_NSW = 6   # parallel NSW bulk fetches
MAX_WORKERS_QLD = 6   # parallel QLD bulk fetches

SESSION = requests.Session()  # TCP reuse

# --------------------- Geometry Helpers ---------------------

def _as_fc(fc_like):
    if fc_like is None: return None
    if isinstance(fc_like, str):
        try: fc_like = json.loads(fc_like)
        except Exception: return None
    if not isinstance(fc_like, dict): return None
    t = fc_like.get("type")
    if t == "FeatureCollection":
        feats = fc_like.get("features", [])
        return {"type":"FeatureCollection","features":feats if isinstance(feats, list) else []}
    if t == "Feature": return {"type":"FeatureCollection","features":[fc_like]}
    if t in {"Point","MultiPoint","LineString","MultiLineString","Polygon","MultiPolygon"}:
        return {"type":"FeatureCollection","features":[{"type":"Feature","geometry":fc_like,"properties":{}}]}
    return None

def _iter_coords(geom):
    g = geom or {}; t = g.get("type"); c = g.get("coordinates")
    if t == "Point":
        if isinstance(c,(list,tuple)) and len(c)>=2: yield c[:2]
    elif t in ("MultiPoint","LineString"):
        for p in c or []:
            if isinstance(p,(list,tuple)) and len(p)>=2: yield p[:2]
    elif t in ("MultiLineString","Polygon"):
        for part in c or []:
            for p in part or []:
                if isinstance(p,(list,tuple)) and len(p)>=2: yield p[:2]
    elif t == "MultiPolygon":
        for poly in c or []:
            for ring in poly or []:
                for p in ring or []:
                    if isinstance(p,(list,tuple)) and len(p)>=2: yield p[:2]

def _geom_bbox(geom):
    minx=miny=math.inf; maxx=maxy=-math.inf; found=False
    for x,y in _iter_coords(geom):
        if not (isinstance(x,(int,float)) and isinstance(y,(int,float))): continue
        found=True
        minx=min(minx,x); maxx=max(maxx,x)
        miny=min(miny,y); maxy=max(maxy,y)
    return (minx,miny,maxx,maxy) if found else None

def _merge_bbox(b1,b2):
    if b1 is None: return b2
    if b2 is None: return b1
    return (min(b1[0],b2[0]), min(b1[1],b2[1]), max(b1[2],b2[2]), max(b1[3],b2[3]))

def _bbox_to_viewstate(bbox, pad=0.12):
    if not bbox: return DEFAULT_VIEW
    minx,miny,maxx,maxy=bbox
    dx=(maxx-minx)*pad; dy=(maxy-miny)*pad
    minx-=dx; maxx+=dx; miny-=dy; maxy+=dy
    cx=(minx+maxx)/2; cy=(miny+maxy)/2
    extent=max(maxx-minx,maxy-miny)
    if extent<=0 or not math.isfinite(extent): return pdk.ViewState(latitude=cy,longitude=cx,zoom=14)
    if extent > 20: zoom = 5
    elif extent > 10: zoom = 6
    elif extent > 5: zoom = 7
    elif extent > 2: zoom = 8
    elif extent > 1: zoom = 9
    elif extent > 0.5: zoom = 10
    elif extent > 0.25: zoom = 11
    elif extent > 0.1: zoom = 12
    else: zoom = 13
    return pdk.ViewState(latitude=cy, longitude=cx, zoom=zoom)

def _fit_view(fc_like):
    fc=_as_fc(fc_like)
    if not fc or not fc.get("features"):
        return DEFAULT_VIEW
    bbox=None
    for f in fc["features"]:
        bbox=_merge_bbox(bbox,_geom_bbox(f.get("geometry") or {}))
    return _bbox_to_viewstate(bbox)

# --------------------- Parsing ---------------------

# NSW lotidstring OR one-slash; normalized to LOT//PLAN (uppercase)
RE_NSW_LOTID = re.compile(r"^\s*(?P<lotid>\d+//[A-Za-z]{1,6}\d+)\s*$")
RE_NSW_ONE_SLASH = re.compile(r"^\s*(?P<lot>\d+)\s*/\s*(?P<plan>[A-Za-z]{1,6}\d+)\s*$")

# QLD input formats (we'll normalize to a single LOTPLAN string like '13SP181800')
RE_LOTPLAN_SLASH = re.compile(
    r"^\s*(?P<lot>\d+)\s*(?:/(?P<section>\d+))?\s*/\s*(?P<plan_type>[A-Za-z]{1,6})\s*(?P<plan_number>\d+)\s*$"
)
RE_COMPACT = re.compile(r"^\s*(?P<lot>\d+)\s*(?P<plan_type>[A-Za-z]{1,6})\s*(?P<plan_number>\d+)\s*$")
RE_VERBOSE = re.compile(
    r"^\s*Lot\s+(?P<lot>\d+)\s+on\s+(?P<plan_label>(Registered|Survey)\s+Plan)\s+(?P<plan_number>\d+)\s*$",
    re.IGNORECASE
)

# SA
RE_SA_PLANPARCEL = re.compile(r"^\s*(?P<planparcel>[A-Za-z]{1,2}\d+[A-Za-z]{1,2}\d+)\s*$")
RE_SA_TITLEPAIR  = re.compile(r"^\s*(?P<a>\d{1,6})\s*/\s*(?P<b>\d{1,6})\s*$")

def _nsw_normalize_lotid(raw: str) -> str:
    s = (raw or "").strip().upper().replace(" ", "")
    if "//" in s: return s
    m = RE_NSW_ONE_SLASH.match(s)
    return f"{m.group('lot')}//{m.group('plan')}" if m else s

def _qld_normalize_lotplan(raw: str) -> Optional[str]:
    """
    Normalize user input to a single QLD LOTPLAN token like '13SP181800'.
    Accepts:
      - '13SP181800'
      - '13/DP1242624' or '13//DP1242624'  -> '13DP1242624'
      - 'Lot 3 on Survey Plan 181800'      -> '13SP181800'
    """
    if not raw:
        return None
    s = (str(raw) or "").strip().upper()
    s = re.sub(r"\s+", " ", s)

    # Pure compact: 13SP181800
    m = RE_COMPACT.match(s.replace(" ", ""))
    if m:
        return f"{m.group('lot')}{(m.group('plan_type') or '').upper()}{m.group('plan_number')}"

    # Slash formats: 13/DP1242624  or  13//DP1242624
    s2 = s.replace(" ", "")
    m = RE_LOTPLAN_SLASH.match(s2)
    if m:
        return f"{m.group('lot')}{(m.group('plan_type') or '').upper()}{m.group('plan_number')}"

    # One-slash NSW style '13/DP124...' counts as same for normalization
    m = RE_NSW_ONE_SLASH.match(s2)
    if m:
        return f"{m.group('lot')}{m.group('plan').upper()}"

    # Verbose: Lot 3 on Survey Plan 181800
    m = RE_VERBOSE.match(s)
    if m:
        plan_type = "SP" if "SURVEY" in (m.group('plan_label') or "").upper() else "RP"
        return f"{m.group('lot')}{plan_type}{m.group('plan_number')}"

    # Already like '13SP181800' but with spaces e.g. '13 SP 181800'
    m = re.match(r"^\s*(\d+)\s*([A-Z]{1,6})\s*(\d+)\s*$", s)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"

    return None

def parse_queries(multiline: str) -> List[Dict]:
    items=[]
    for raw in [x.strip() for x in (multiline or "").splitlines() if x.strip()]:
        # NSW first
        m = RE_NSW_LOTID.match(raw) or RE_NSW_ONE_SLASH.match(raw)
        if m:
            items.append({"raw": raw, "nsw_lotid": _nsw_normalize_lotid(raw)})
            continue
        # QLD various forms captured for legacy per-line mode
        m = RE_LOTPLAN_SLASH.match(raw)
        if m:
            items.append({"raw":raw,"lot":m.group("lot"),"section":m.group("section"),
                          "plan_type":(m.group("plan_type") or "").upper(),"plan_number":m.group("plan_number")})
            continue
        m = RE_COMPACT.match(raw.replace(" ", ""))
        if m:
            items.append({"raw":raw,"lot":m.group("lot"),"section":None,
                          "plan_type":(m.group("plan_type") or "").upper(),"plan_number":m.group("plan_number")})
            continue
        m = RE_VERBOSE.match(raw)
        if m:
            plan_type = "SP" if "Survey" in (m.group("plan_label") or "") else "RP"
            items.append({"raw":raw,"lot":m.group("lot"),"section":None,
                          "plan_type":plan_type,"plan_number":m.group("plan_number")})
            continue
        m = RE_SA_PLANPARCEL.match(raw)
        if m:
            items.append({"raw":raw,"sa_planparcel":m.group("planparcel").upper()}); continue
        m = RE_SA_TITLEPAIR.match(raw)
        if m:
            a,b=m.group("a"),m.group("b"); items.append({"raw":raw,"sa_titlepair":(a,b)}); continue
        items.append({"raw":raw,"unparsed":True})
    return items

# --------------------- HTTP / ArcGIS ---------------------

def _http_get_json(url: str, params: Dict, retries: int = REQUEST_RETRIES, timeout: int = REQUEST_TIMEOUT) -> Dict:
    last=None
    for attempt in range(retries+1):
        try:
            base=dict(f="json", outSR=4326, returnGeometry="true", geometryPrecision=6, returnExceededLimitFeatures="false")
            r = SESSION.get(url, params={**base, **params}, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last=e
            if attempt<retries: time.sleep(0.4)
    raise last if last else RuntimeError("Unknown request error")

def _arcgis_to_fc(data: Dict) -> Dict:
    feats=[]
    for g in data.get("features", []):
        geom=g.get("geometry"); attrs=g.get("attributes", {})
        if not geom: continue
        if "rings" in geom:
            geo={"type":"Polygon","coordinates":geom["rings"]}
        elif "paths" in geom:
            geo={"type":"MultiLineString","coordinates":geom["paths"]}
        elif "x" in geom and "y" in geom:
            geo={"type":"Point","coordinates":[geom["x"],geom["y"]]}
        else:
            continue
        feats.append({"type":"Feature","geometry":geo,"properties":attrs})
    return {"type":"FeatureCollection","features":feats}

def _arcgis_query(url: str, where: str, out_fields: str = "*") -> Dict:
    data = _http_get_json(url, {"where": where, "outFields": out_fields})
    return _arcgis_to_fc(data)

# --------------------- Fetchers ---------------------

# QLD (legacy per-line)
def fetch_qld(lot: str, plan_type: str, plan_number: str) -> Dict:
    url = ENDPOINTS["QLD"]
    plan_full = f"{plan_type}{plan_number}".upper()
    where = f"(PLAN='{plan_full}') AND (LOT='{lot}')"
    return _arcgis_query(url, where)

# SA
def fetch_sa_by_planparcel(planparcel_str: str) -> Dict:
    url = ENDPOINTS["SA"]
    where = f"planparcel='{planparcel_str.upper()}'"
    return _arcgis_query(url, where)

def fetch_sa_by_title(volume: str, folio: str) -> Dict:
    url = ENDPOINTS["SA"]
    where = f"(volume='{volume}') AND (folio='{folio}')"
    return _arcgis_query(url, where)

# NSW one-shot & bulk by lotidstring
def nsw_fetch_one(lotid: str) -> Dict:
    url = ENDPOINTS["NSW"]
    lotid_norm = _nsw_normalize_lotid(lotid)
    params = {"where": f"lotidstring='{lotid_norm}'", "outFields": "*"}
    data = _http_get_json(url, params, timeout=REQUEST_TIMEOUT, retries=REQUEST_RETRIES)
    return _arcgis_to_fc(data)

def nsw_fetch_bulk(lotids: List[str], max_workers: int = MAX_WORKERS_NSW) -> Dict:
    lotids_norm = [ _nsw_normalize_lotid(x) for x in lotids if x and x.strip() ]
    if not lotids_norm:
        return {"type":"FeatureCollection","features":[]}
    features: List[Dict] = []
    errors: List[str] = []
    def _task(lid: str):
        try:
            return lid, nsw_fetch_one(lid)
        except Exception as e:
            return lid, {"error": str(e)}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        for lid, res in ex.map(_task, lotids_norm):
            if "error" in res:
                errors.append(f"{lid}: {res['error']}")
            else:
                features.extend(res.get("features", []))
    if errors:
        st.warning("NSW bulk had issues:\n- " + "\n- ".join(errors[:10]), icon="⚠️")
        if len(errors) > 10: st.caption(f"... plus {len(errors) - 10} more.")
    # de-dup
    seen=set(); uniq=[]
    for f in features:
        props=f.get("properties") or {}
        sig=(props.get("objectid"), props.get("lotidstring"))
        if sig not in seen:
            seen.add(sig); uniq.append(f)
    return {"type":"FeatureCollection","features":uniq}

# ------------- NEW: QLD bulk by LOTPLAN (lot+plan as one token) -------------

def qld_fetch_one_lotplan(lotplan: str) -> Dict:
    """
    One-shot QLD by LOTPLAN token, e.g. '13SP181800'.
    Strategy:
        1) Prefer WHERE LOTPLAN='13SP181800' (if the service supports LOTPLAN field)
        2) Fallback: split to LOT='13' AND PLAN='SP181800'
    """
    url = ENDPOINTS["QLD"]
    lp = (lotplan or "").strip().upper()
    if not lp:
        return {"type":"FeatureCollection","features":[]}

    # Try LOTPLAN directly
    try:
        fc = _arcgis_query(url, f"LOTPLAN='{lp}'")
        if fc.get("features"):
            return fc
    except Exception:
        # service might reject unknown field, fall back
        pass

    # Fallback: split LOT + PLAN
    m = re.match(r"^(?P<lot>\d+)(?P<plan_type>[A-Z]{1,6})(?P<plan_num>\d+)$", lp)
    if not m:
        return {"type":"FeatureCollection","features":[]}
    lot = m.group("lot")
    plan_full = f"{m.group('plan_type')}{m.group('plan_num')}"
    where = f"(PLAN='{plan_full}') AND (LOT='{lot}')"
    return _arcgis_query(url, where)

def qld_fetch_bulk_lotplan(tokens: List[str], max_workers: int = MAX_WORKERS_QLD) -> Dict:
    """
    Parallel QLD fetch by LOTPLAN tokens and merge features.
    Accepts inputs in many forms and normalizes to '13SP181800'.
    """
    norm: List[str] = []
    for t in tokens:
        lp = _qld_normalize_lotplan(t)
        if lp:
            norm.append(lp)

    if not norm:
        return {"type":"FeatureCollection","features":[]}

    features: List[Dict] = []
    errors: List[str] = []

    def _task(lp: str):
        try:
            return lp, qld_fetch_one_lotplan(lp)
        except Exception as e:
            return lp, {"error": str(e)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        for lp, res in ex.map(_task, norm):
            if "error" in res:
                errors.append(f"{lp}: {res['error']}")
            else:
                features.extend(res.get("features", []))

    if errors:
        st.warning("QLD bulk had issues:\n- " + "\n- ".join(errors[:10]), icon="⚠️")
        if len(errors) > 10: st.caption(f"... plus {len(errors) - 10} more.")

    # de-dup: objectid + LOT + PLAN
    seen=set(); uniq=[]
    for f in features:
        props=f.get("properties") or {}
        sig=(props.get("OBJECTID") or props.get("objectid"),
             props.get("LOT") or props.get("lot"),
             props.get("PLAN") or props.get("plan"))
        if sig not in seen:
            seen.add(sig); uniq.append(f)

    return {"type":"FeatureCollection","features":uniq}

# --------------------- Exports ---------------------

def features_to_geojson(fc: Dict) -> bytes:
    return json.dumps(fc, ensure_ascii=False).encode("utf-8")

def features_to_kml_kmz(fc: Dict, as_kmz: bool = False) -> Tuple[str, bytes]:
    if not HAVE_SIMPLEKML:
        raise RuntimeError("simplekml is not installed; cannot create KML/KMZ.")
    kml = simplekml.Kml()
    for feat in fc.get("features", []):
        props = (feat.get("properties") or {}).copy()
        name = (
            props.get("lotidstring")
            or props.get("LOTPLAN") or props.get("lotplan")
            or props.get("planparcel")
            or props.get("planlabel") or props.get("PLAN_LABEL")
            or props.get("PLAN") or props.get("plan")
            or "parcel"
        )
        lines=[f"{k}: {v}" for k,v in sorted(props.items(), key=lambda kv: kv[0].lower()) if v not in (None,"")]
        desc="\n".join(lines) if lines else "No attributes"

        geom = feat.get("geometry") or {}
        t = geom.get("type")
        if t == "Polygon":
            coords=[[(lng,lat) for lng,lat in ring] for ring in geom.get("coordinates",[])]
            poly=kml.newpolygon(name=name, description=desc)
            if coords:
                poly.outerboundaryis=coords[0]
                if len(coords)>1: poly.innerboundaryis=coords[1:]
        elif t == "MultiLineString":
            for path in geom.get("coordinates",[]):
                ls=kml.newlinestring(name=name, description=desc)
                ls.coords=[(lng,lat) for lng,lat in path]
        elif t == "LineString":
            ls=kml.newlinestring(name=name, description=desc)
            ls.coords=[(lng,lat) for lng,lat in geom.get("coordinates",[])]
        elif t == "Point":
            pt=kml.newpoint(name=name, description=desc)
            lng,lat=(geom.get("coordinates") or [None,None])[:2]
            if lng is not None and lat is not None: pt.coords=[(lng,lat)]

    if as_kmz:
        buf=io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("doc.kml", kml.kml())
        return ("application/vnd.google-earth.kmz", buf.getvalue())
    else:
        return ("application/vnd.google-earth.kml+xml", kml.kml().encode("utf-8"))

# --------------------- UI ---------------------

st.title("MappingKML — Parcel Finder")

with st.sidebar:
    st.subheader("Search scope")
    sel_qld = st.checkbox("Queensland (QLD)", value=True)
    sel_nsw = st.checkbox("New South Wales (NSW)", value=True)
    sel_sa  = st.checkbox("South Australia (SA)", value=True)

    st.markdown("**Input formats:**")
    st.markdown(
        "- **NSW (lotidstring only):** `13//DP1246224` (also accepts `13/DP1246224`)  \n"
        "- **QLD (LOTPLAN for bulk):** `13SP181800`, `13DP1242624`  \n"
        "- **QLD (per-line still accepted):** `13/DP1242624`, `3SP181800`, `Lot 3 on Survey Plan 181800`  \n"
        "- **SA:** `D10001AL12`  •  `FOLIO/VOLUME` or `VOLUME/FOLIO` (e.g., `1234/5678`)"
    )

    st.markdown("---")
    # NSW bulk toggle
    nsw_bulk_mode = st.checkbox("NSW bulk mode (lotidstring list)", value=False)
    if nsw_bulk_mode:
        nsw_bulk_text = st.text_area(
            "NSW lotidstrings (one per line or comma-separated)",
            height=120,
            placeholder="13//DP1246224\n12//DP1246224\n101//DP123456\n..."
        )
    else:
        nsw_bulk_text = ""

    # QLD bulk toggle
    qld_bulk_mode = st.checkbox("QLD bulk mode (LOTPLAN list)", value=False)
    if qld_bulk_mode:
        qld_bulk_text = st.text_area(
            "QLD LOTPLAN tokens (one per line or comma-separated)",
            height=120,
            placeholder="13SP181800\n13DP1242624\n5RP912345\n..."
        )
    else:
        qld_bulk_text = ""

examples = (
    "13//DP1246224  # NSW lotidstring\n"
    "13SP181800     # QLD LOTPLAN (bulk)\n"
    "13/DP1242624   # QLD per-line\n"
    "D10001AL12     # SA planparcel\n"
    "1234/5678      # SA title\n"
)
queries_text = st.text_area("Enter one query per line (for QLD per-line, SA, and NSW per-line)", height=170, placeholder=examples)

c1, c2 = st.columns([1,1])
with c1: run_btn = st.button("Search", type="primary")
with c2: clear_btn = st.button("Clear")

if clear_btn:
    st.experimental_rerun()

parsed = parse_queries(queries_text)
unparsed = [p["raw"] for p in parsed if p.get("unparsed")]
if unparsed:
    st.info("Could not parse these lines (ignored):\n- " + "\n- ".join(unparsed))

if run_btn and not (sel_qld or sel_nsw or sel_sa):
    st.warning("Please tick at least one state to search.", icon="⚠️")

accum_features: List[Dict] = []
state_counts = {"NSW":0, "QLD":0, "SA":0}
state_warnings: List[str] = []

def _add_features(fc):
    for f in (fc or {}).get("features", []):
        accum_features.append(f)

# --------------------- Run ---------------------

if run_btn and (sel_qld or sel_nsw or sel_sa):
    with st.spinner("Querying selected states..."):

        # --- NSW (bulk or per-line) ---
        if sel_nsw:
            if nsw_bulk_mode and nsw_bulk_text.strip():
                raw_items = [x.strip() for part in nsw_bulk_text.splitlines() for x in part.split(",")]
                lotids = [x for x in raw_items if x]
                st.caption(f"NSW bulk: {len(lotids)} lotidstring(s)")
                fc_bulk = nsw_fetch_bulk(lotids)
                c = len(fc_bulk.get("features", [])); state_counts["NSW"] += c
                if c == 0: st.warning("NSW bulk: no parcels found.", icon="⚠️")
                else: st.success(f"NSW bulk: found {c} feature(s).")
                _add_features(fc_bulk)
            else:
                for p in parsed:
                    if p.get("unparsed"): continue
                    if "nsw_lotid" in p:
                        lotid = _nsw_normalize_lotid(p["nsw_lotid"])
                        st.caption(f"NSW where: lotidstring='{lotid}'")
                        try:
                            fc = nsw_fetch_one(lotid)
                        except requests.exceptions.Timeout:
                            state_warnings.append("NSW request timed out.")
                            fc = {"type":"FeatureCollection","features":[]}
                        except Exception as e:
                            state_warnings.append(f"NSW error for {p.get('raw')}: {e}")
                            fc = {"type":"FeatureCollection","features":[]}
                        c = len(fc.get("features", [])); state_counts["NSW"] += c
                        if c == 0: state_warnings.append(f"NSW: No parcels for lotidstring '{lotid}'.")
                        _add_features(fc)

        # --- QLD (bulk or per-line) ---
        if sel_qld:
            if qld_bulk_mode and qld_bulk_text.strip():
                raw_items = [x.strip() for part in qld_bulk_text.splitlines() for x in part.split(",")]
                lotplans = [x for x in raw_items if x]
                st.caption(f"QLD bulk: {len(lotplans)} LOTPLAN token(s)")
                fc_bulk = qld_fetch_bulk_lotplan(lotplans)
                c = len(fc_bulk.get("features", [])); state_counts["QLD"] += c
                if c == 0: st.warning("QLD bulk: no parcels found.", icon="⚠️")
                else: st.success(f"QLD bulk: found {c} feature(s).")
                _add_features(fc_bulk)
            else:
                for p in parsed:
                    if p.get("unparsed") or p.get("nsw_lotid") or p.get("sa_planparcel") or p.get("sa_titlepair"):
                        continue
                    pt = (p.get("plan_type") or "").upper()
                    if pt:
                        try:
                            fc = fetch_qld(p.get("lot"), pt, p.get("plan_number"))
                        except requests.exceptions.Timeout:
                            state_warnings.append("QLD request timed out.")
                            fc = {"type":"FeatureCollection","features":[]}
                        except Exception as e:
                            state_warnings.append(f"QLD error for {p.get('raw')}: {e}")
                            fc = {"type":"FeatureCollection","features":[]}
                        c = len(fc.get("features", [])); state_counts["QLD"] += c
                        if c == 0:
                            state_warnings.append(f"QLD: No parcels for lot '{p.get('lot')}', plan '{pt}{p.get('plan_number')}'.")
                        _add_features(fc)

        # --- SA (unchanged) ---
        if sel_sa:
            for p in parsed:
                if p.get("unparsed"): continue
                try:
                    if "sa_planparcel" in p:
                        fc = fetch_sa_by_planparcel(p["sa_planparcel"])
                        c = len(fc.get("features", [])); state_counts["SA"] += c
                        if c == 0: state_warnings.append(f"SA: No parcels for planparcel '{p['sa_planparcel']}'.")
                        _add_features(fc); 
                        continue

                    if "sa_titlepair" in p:
                        a,b = p["sa_titlepair"]
                        fc1 = fetch_sa_by_title(volume=a, folio=b)
                        fc2 = fetch_sa_by_title(volume=b, folio=a)
                        seen=set(); merged={"type":"FeatureCollection","features":[]}
                        for fc_try in (fc1, fc2):
                            for feat in fc_try.get("features", []):
                                pid = (feat.get("properties") or {}).get("parcel_id") or json.dumps(feat.get("geometry", {}), sort_keys=True)
                                if pid in seen: 
                                    continue
                                seen.add(pid); merged["features"].append(feat)
                        c = len(merged["features"]); state_counts["SA"] += c
                        if c == 0:
                            state_warnings.append(f"SA: No parcels for title inputs '{a}/{b}'. (Tried both volume/folio and folio/volume.)")
                        _add_features(merged)
                except requests.exceptions.Timeout:
                    state_warnings.append("SA request timed out.")
                except Exception as e:
                    state_warnings.append(f"SA error for {p.get('raw')}: {e}")

# --------------------- Map ---------------------

fc_all = {"type":"FeatureCollection","features":accum_features}

if state_warnings:
    for w in state_warnings:
        st.warning(w, icon="⚠️")

if run_btn and (sel_qld or sel_nsw or sel_sa):
    st.success(f"Found — NSW: {state_counts['NSW']}  |  QLD: {state_counts['QLD']}  |  SA: {state_counts['SA']}")

layers=[]
if accum_features:
    layers.append(
        pdk.Layer(
            "GeoJsonLayer",
            fc_all,
            pickable=True,
            stroked=True,
            filled=True,
            wireframe=True,
            get_line_width=2,
        )
    )

tooltip_html = """
<div style="font-family:Arial,sans-serif;">
  <b>{planlabel}</b><br/>
  LotID: <b>{lotidstring}</b><br/>
  Lot {lotnumber}{sectionnumber}<br/>
  QLD: LOT {LOT} PLAN {PLAN}<br/>
  SA Title: Vol {volume} / Fol {folio}
</div>
"""

view_state=_fit_view(fc_all if accum_features else None)
deck=pdk.Deck(layers=layers, initial_view_state=view_state, map_style=None, tooltip={"html":tooltip_html})
st.pydeck_chart(deck, use_container_width=True)

# --- minimal helper to ensure Streamlit always gets raw bytes ---
def _as_bytes(x):
    try:
        if x is None:
            return b""
        if isinstance(x, (bytes, bytearray)):
            return bytes(x)
        if hasattr(x, "getvalue"):
            return x.getvalue()
        if isinstance(x, memoryview):
            return x.tobytes()
        if isinstance(x, str):
            return x.encode("utf-8")
        return b""
    except Exception:
        return b""

# --------------------- Downloads ---------------------

st.subheader("Downloads")
d1, d2, d3, d4 = st.columns(4)

with d1:
    geojson_data = features_to_geojson(fc_all) if accum_features else None
    st.download_button(
        label="Download GeoJSON",
        data=geojson_data,
        file_name="parcels.geojson",
        mime="application/geo+json",
        type="secondary",
        use_container_width=True,
        disabled=not bool(geojson_data),
    )

with d2:
    kml_bytes = b""
    if HAVE_SIMPLEKML and accum_features:
        try:
            _, kml_bytes = features_to_kml_kmz(fc_all, as_kmz=False)
        except Exception as e:
            kml_bytes = b""
            st.error(f"KML export error: {e}")
    _kml_data = _as_bytes(kml_bytes)
    st.download_button(
        label="Download KML",
        data=_kml_data,
        file_name="parcels.kml",
        mime="application/vnd.google-earth.kml+xml",
        type="primary",
        use_container_width=True,
        disabled=not bool(_kml_data),
    )
    if not HAVE_SIMPLEKML:
        st.caption("Install `simplekml` for KML/KMZ: pip install simplekml")

with d3:
    kmz_bytes = None
    if HAVE_SIMPLEKML and accum_features:
        _, kmz_bytes = features_to_kml_kmz(fc_all, as_kmz=True)
    st.download_button(
        label="Download KMZ",
        data=kmz_bytes,
        file_name="parcels.kmz",
        mime="application/vnd.google-earth.kmz",
        type="secondary",
        use_container_width=True,
        disabled=not bool(kmz_bytes),
    )

with d4:
    shp_zip = b""
    if accum_features:
        try:
            shp_zip = generate_shapefile(accum_features, "MULTI")
        except Exception as e:
            shp_zip = b""
            st.error(f"Shapefile export error: {e}")
    _shp_data = _as_bytes(shp_zip)
    st.download_button(
        label="Download Shapefile (.zip)",
        data=_shp_data,
        file_name="parcels_shapefile.zip",
        mime="application/zip",
        type="secondary",
        use_container_width=True,
        disabled=not bool(_shp_data),
    )
