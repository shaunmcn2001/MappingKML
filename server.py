from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import re

app = Flask(__name__)
CORS(app)


@app.route('/search', methods=['POST'])
def search():
    data = request.get_json(force=True)
    queries = data.get('queries', [])
    if isinstance(queries, str):
        queries = [queries]
    features = []
    regions = []
    for user_input in queries:
        lot_str = sec_str = plan_str = ""
        if '/' in user_input:
            parts = user_input.split('/')
            if len(parts) == 3:
                lot_str, sec_str, plan_str = parts[0].strip(), parts[1].strip(), parts[2].strip()
            elif len(parts) == 2:
                lot_str, sec_str, plan_str = parts[0].strip(), '', parts[1].strip()
            else:
                lot_str = sec_str = plan_str = ''
        if sec_str == '' and '//' in user_input:
            lot_str, plan_str = user_input.split('//')
            sec_str = ''
        plan_num = ''.join(filter(str.isdigit, plan_str))
        if lot_str and plan_num:
            where = [f"lotnumber='{lot_str}'"]
            if sec_str:
                where.append(f"sectionnumber='{sec_str}'")
            else:
                where.append("(sectionnumber IS NULL OR sectionnumber = '')")
            where.append(f"plannumber={plan_num}")
            url = 'https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_Cadastre/MapServer/9/query'
            params = {
                'where': ' AND '.join(where),
                'outFields': 'lotnumber,sectionnumber,planlabel',
                'outSR': '4326',
                'f': 'geoJSON'
            }
            try:
                res = requests.get(url, params=params, timeout=10)
                data = res.json()
            except Exception:
                data = {}
            for feat in data.get('features', []) or []:
                features.append(feat)
                regions.append('NSW')
        inp = user_input.replace(' ', '').upper()
        m = re.match(r'^(\d+)([A-Z].+)$', inp)
        if not m:
            continue
        lot_str = m.group(1)
        plan_str = m.group(2)
        url = 'https://spatial-gis.information.qld.gov.au/arcgis/rest/services/PlanningCadastre/LandParcelPropertyFramework/MapServer/4/query'
        params = {
            'where': f"lot='{lot_str}' AND plan='{plan_str}'",
            'outFields': 'lot,plan,lotplan,locality',
            'outSR': '4326',
            'f': 'geoJSON'
        }
        try:
            res = requests.get(url, params=params, timeout=10)
            data = res.json()
        except Exception:
            data = {}
        for feat in data.get('features', []) or []:
            features.append(feat)
            regions.append('QLD')
    return jsonify({'features': features, 'regions': regions})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
