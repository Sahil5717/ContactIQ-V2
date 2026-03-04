"""
ContactIQ — Intelligent Contact Center Optimization Platform (v8 Infrastructure)
Phase 1 Infrastructure: Authentication, File Upload, Data Management, API Layer.
All existing engine routes preserved. New: login, file management, data source switching.
"""
import json
import os
import tempfile
import copy
import traceback
from flask import Flask, jsonify, request, render_template, send_file, session, redirect, g
from engines.data_loader import run_etl, set_path_overrides
from engines.diagnostic import run_diagnostic
from engines.maturity import run_maturity
from engines.readiness import compute_readiness, STRATEGIC_DRIVERS
from engines.waterfall import score_initiatives, run_waterfall
from engines.risk import run_risk
from engines.workforce import run_workforce
from engines.channel_strategy import run_channel_strategy
from engines.recommendations import get_recommendations, get_initiative_linkage, get_available_industries, get_industry_config
from infrastructure.database import init_db, validate_session, load_overrides, save_overrides
from infrastructure.auth import init_auth, login_user, logout_user, get_current_user, require_role
from infrastructure.file_manager import (
    FILE_REGISTRY, get_file_status, get_active_file_path, get_upload_summary,
    save_uploaded_file, clear_uploaded_file, clear_all_uploads, generate_template,
    UPLOAD_DIR, ensure_dirs
)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'contactiq-dev-key-change-in-prod')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB upload limit

STATE = {
    'data': None, 'diagnostic': None, 'maturity': None,
    'readiness': None, 'initiatives': None, 'waterfall': None,
    'risk': None, 'workforce': None, 'channelStrategy': None,
    'overrides': {}, 'loaded': False,
}

# ── Initialize Infrastructure ────────────────────────────────
init_db()
init_auth(app)


def _sanitize_for_json(obj):
    if isinstance(obj, set): return sorted(list(obj))
    elif isinstance(obj, dict): return {k: _sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list): return [_sanitize_for_json(v) for v in obj]
    return obj


def _apply_file_overrides():
    """Check for uploaded files and set path overrides before running ETL."""
    ensure_dirs()
    overrides = {}
    for cat in FILE_REGISTRY:
        upload_path = os.path.join(UPLOAD_DIR, f"{cat}.xlsx")
        if os.path.exists(upload_path):
            overrides[cat] = upload_path
    set_path_overrides(overrides)


def _run_all():
    _apply_file_overrides()
    data = run_etl()
    if 'totalCost' not in data:
        data['totalCost'] = sum(r['headcount'] * r['costPerFTE'] for r in data['roles'])
    if 'avgCPC' not in data:
        annual_vol = data.get('totalVolumeAnnual', data.get('totalVolume', 1))
        data['avgCPC'] = round(data['totalCost'] / max(annual_vol, 1), 2)
    STATE['data'] = data
    STATE['diagnostic'] = run_diagnostic(data)
    STATE['maturity'] = run_maturity(data, STATE['diagnostic'])
    STATE['readiness'] = compute_readiness(data, STATE['diagnostic'], STATE['maturity'])
    STATE['initiatives'] = score_initiatives(data, STATE['diagnostic'], STATE['readiness'])
    _apply_all_overrides()
    STATE['waterfall'] = run_waterfall(data, STATE['initiatives'])
    STATE['risk'] = run_risk(STATE['initiatives'], data)
    STATE['workforce'] = run_workforce(data, STATE['waterfall'], STATE['initiatives'])
    STATE['channelStrategy'] = run_channel_strategy(data, STATE['diagnostic'], STATE['initiatives'])
    STATE['subIntentAnalysis'] = STATE['diagnostic'].get('subIntentAnalysis', [])
    STATE['loaded'] = True
    return True


def _recompute_downstream():
    data = STATE['data']
    STATE['waterfall'] = run_waterfall(data, STATE['initiatives'])
    STATE['risk'] = run_risk(STATE['initiatives'], data)
    STATE['workforce'] = run_workforce(data, STATE['waterfall'], STATE['initiatives'])


def _recompute_all_from_diagnostic():
    data = STATE['data']
    STATE['diagnostic'] = run_diagnostic(data)
    STATE['maturity'] = run_maturity(data, STATE['diagnostic'])
    STATE['readiness'] = compute_readiness(data, STATE['diagnostic'], STATE['maturity'])
    STATE['initiatives'] = score_initiatives(data, STATE['diagnostic'], STATE['readiness'])
    _apply_all_overrides()
    _recompute_downstream()
    STATE['channelStrategy'] = run_channel_strategy(data, STATE['diagnostic'], STATE['initiatives'])


def _apply_all_overrides():
    # Initiative overrides
    for init in STATE['initiatives']:
        iid = init['id']
        ok = f"init_enabled_{iid}"
        if ok in STATE['overrides']: init['enabled'] = STATE['overrides'][ok]
        for rk in ('rampYear1','rampYear2','rampYear3'):
            rkey = f"init_{rk}_{iid}"
            if rkey in STATE['overrides']: init[rk] = STATE['overrides'][rkey]
        field_key = f"init_fields_{iid}"
        if field_key in STATE['overrides']:
            for fk, fv in STATE['overrides'][field_key].items():
                init[fk] = fv
    # v12-#53: Benchmark overrides — re-apply saved benchmark values after recalculation
    for key, val in STATE['overrides'].items():
        if key.startswith('benchmark_'):
            metric = key.replace('benchmark_', '')
            bm = STATE['data'].get('benchmarks', {})
            if metric in bm:
                if isinstance(bm[metric], dict):
                    bm[metric]['global'] = val
                else:
                    bm[metric] = val


def _build_demo_object(overrides=None):
    ov = overrides or {}
    data = STATE['data']; diag = STATE['diagnostic']; mat = STATE['maturity']
    wf = ov.get('waterfall', STATE['waterfall'])
    rsk = ov.get('risk', STATE['risk'])
    wkf = ov.get('workforce', STATE['workforce'])
    chs = STATE['channelStrategy']; roles = data['roles']
    inits = ov.get('initiatives', STATE['initiatives'])
    return {
        'queues': data['queues'], 'roles': roles, 'params': data['params'],
        'benchmarks': data.get('benchmarks', {}),
        'totalVolume': data['totalVolume'], 'totalFTE': data['totalFTE'],
        'totalCost': data['totalCost'],
        'totalVolumeAnnual': data.get('totalVolumeAnnual', data['totalVolume']),
        'annualContactsPerFTE': round(data.get('totalVolumeAnnual', data['totalVolume']) / max(data['totalFTE'], 1)),
        'volumeAnnualizationFactor': data.get('volumeAnnualizationFactor', 12),
        'totalMonthlyCost': sum(r['headcount']*r['costPerFTE']/12 for r in roles),
        'avgCSAT': data['avgCSAT'], 'avgAHT': round(data['avgAHT']*60, 0),
        'avgAHT_min': round(data['avgAHT'], 1),
        'avgFCR': data.get('avgFCR', 0), 'avgCPC': data.get('avgCPC', 0),
        'avgEscalation': data.get('avgEscalation', _weighted_avg(data['queues'], 'escalation')),
        'avgRepeat': data.get('avgRepeat', _weighted_avg(data['queues'], 'repeat')),
        'avgCES': data.get('avgCES', _weighted_avg(data['queues'], 'ces')),
        'channelMix': _build_channel_mix(data['queues']),
        'buMix': _build_bu_mix(data['queues']),
        'intentMix': _build_intent_mix(data['queues']),
        'healthScores': diag.get('queueScores', []),
        'overallHealth': diag.get('summary', {}).get('avgScore', 0),
        'healthRag': 'green' if diag.get('summary',{}).get('avgScore',0) >= 70 else 'amber' if diag.get('summary',{}).get('avgScore',0) >= 40 else 'red',
        'problemAreas': diag.get('problemAreas', []),
        'rootCauses': diag.get('rootCauses', []),
        'costAnalysis': diag.get('costAnalysis', {}),
        'channelSummary': diag.get('channelSummary', []),
        'subIntentAnalysis': diag.get('subIntentAnalysis', []),
        'mismatch': diag.get('mismatch', []),
        'mismatchSummary': diag.get('mismatch', []),
        'mismatchDetail': diag.get('mismatch', []),
        'maturityScores': mat.get('dimensions', {}),
        'maturityOverall': mat.get('overall', 0),
        'maturityLevel': mat.get('overallLevel', 1),
        'maturityLabel': mat.get('levelInfo', {}).get('label', ''),
        'maturityGaps': mat.get('gaps', []),
        'maturityRadar': mat.get('radar', {}),
        'maturityRecommendations': mat.get('gaps', []),
        'initiatives': inits,
        'enabledCount': sum(1 for i in inits if i.get('enabled')),
        'waterfall': wf,
        'yearlyProjections': wf.get('yearly', []),
        'scenarios': wf.get('scenarios', {}),
        'sensitivity': wf.get('sensitivity', []),
        'roleImpact': wf.get('roleImpact', {}),
        'investmentItems': wf.get('investmentItems', []),
        'investmentYearly': wf.get('investmentYearly', []),
        'investmentSummary': wf.get('investmentSummary', {}),
        'financials': {
            'totalNPV': wf.get('totalNPV',0), 'totalSaving': wf.get('totalSaving',0),
            'totalInvestment': wf.get('totalInvestment',0), 'roi': wf.get('roi',0),
            'roiGross': wf.get('roiGross',0), 'payback': wf.get('payback',0),
            'irr': wf.get('irr',0), 'techInvestment': wf.get('techInvestment',0),
            'annualMaintenance': wf.get('annualMaintenance',0),
        },
        'riskRegister': rsk.get('initiatives', []),
        'riskSummary': rsk.get('summary', {}),
        'topRisks': sorted(rsk.get('initiatives',[]), key=lambda x: x.get('overallRisk',0), reverse=True)[:5],
        'riskByLayer': rsk.get('riskByLayer', {}),
        'riskByBU': rsk.get('riskByBU', {}),
        'riskDependencies': rsk.get('dependencies', {}),
        'workforceTransition': wkf.get('transitions', []),
        'workforceSummary': wkf.get('summary', {}),
        'reskillMatrix': wkf.get('reskillMatrix', {}),
        'reskillDemand': wkf.get('reskillDemand', {}),
        'workforceByLocation': wkf.get('byLocation', {}),
        'workforceByBU': wkf.get('byBU', {}),
        'heatmapData': _build_heatmap(data['queues']),
        'costBreakdown': _build_cost_breakdown(data),
        'channelRecommendations': chs.get('recommendations', []),
        'channelSankey': chs.get('sankey', {}),
        'currentDigitalPct': chs.get('currentDigitalPct', 0),
        'targetDigitalPct': chs.get('targetDigitalPct', 0),
        'channelScorecard': chs.get('recommendations', []),
        'channelIntroductions': [r for r in chs.get('recommendations',[]) if r.get('decision')=='invest'],
        'channelRetirements': [r for r in chs.get('recommendations',[]) if r.get('decision') in ('sunset','migrate_from')],
        'channelOptimalMix': {'currentDigital': chs.get('currentDigitalPct',0), 'targetDigital': chs.get('targetDigitalPct',0)},
        'intentMatrix': chs.get('intentMatrix', []),
        'targetMix': chs.get('targetMix', {}),
        'channelMigrations': chs.get('migrations', []),
        'migrationReadiness': chs.get('migrationReadiness', {}),
        'frictionSignals': chs.get('frictionSignals', []),
        'cxSafeguards': chs.get('cxSafeguards', {}),
        'channelCostAnalysis': chs.get('costAnalysis', {}),
        'migrationSavings': chs.get('migrationSavings', {}),
        'readiness': _sanitize_for_json(STATE.get('readiness', {})),
        'automationReadiness': STATE.get('readiness', {}).get('automationReadiness', 0),
        'opModelGap': STATE.get('readiness', {}).get('opModelGap', 0),
        'locationScore': STATE.get('readiness', {}).get('locationScore', 0),
        'strategicDriver': STATE.get('readiness', {}).get('strategicDriver', 'cost_optimization'),
        'strategicDrivers': STRATEGIC_DRIVERS,
        'layerFTE': wf.get('layerFTE', {}),
        'layerSaving': wf.get('layerSaving', {}),
        'poolUtilization': wf.get('poolUtilization', {}),
        'poolSummary': wf.get('poolSummary', {}),
        'auditTrail': wf.get('auditTrail', []),
        'clientName': data['params'].get('clientName','Client'),
        'industry': data['params'].get('industry','Custom'),
        'currency': data['params'].get('currency','USD'),
        'horizon': data['params'].get('horizon',3),
        # P2-1: Dimensional engine fields
        'locations': data.get('locations', ['Onshore']),
        'sourcingTypes': data.get('sourcingTypes', ['In-house']),
        'locationCostMatrix': data.get('locationCostMatrix', {}),
        'buSummary': wf.get('buSummary', {}),
        'bus': data.get('bus', []),
        # v12-#35: Sub-intent data for downstream pages
        'subIntentEnriched': _enrich_sub_intents_for_downstream(
            diag.get('subIntentAnalysis', []), inits, wf),
    }


# ══════════════════════════════════════════════════════════════
#  AUTHENTICATION ROUTES
# ══════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════
#  HEALTH CHECK (Railway deployment)
# ══════════════════════════════════════════════════════════════

@app.route('/api/health')
def api_health():
    return jsonify({'status': 'ok', 'version': 'v2.0', 'service': 'ContactIQ'})


@app.route('/login')
def login_page():
    if session.get('auth_token') and validate_session(session['auth_token']):
        return redirect('/')
    return render_template('login.html')


@app.route('/api/auth/login', methods=['POST'])
def api_login():
    body = request.get_json(force=True)
    username = body.get('username', '').strip()
    password = body.get('password', '')
    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400
    success, result, token = login_user(username, password, ip_address=request.remote_addr)
    if not success:
        return jsonify({'error': result}), 401
    session['auth_token'] = token
    return jsonify({
        'status': 'ok',
        'user': {'id': result['id'], 'username': result['username'],
                 'role': result['role'], 'display_name': result['display_name']},
        'redirect': '/',
    })


@app.route('/api/auth/logout', methods=['POST'])
def api_logout():
    logout_user()
    return jsonify({'status': 'ok', 'redirect': '/login'})


@app.route('/api/auth/me')
def api_auth_me():
    user = get_current_user()
    if not user: return jsonify({'error': 'Not authenticated'}), 401
    return jsonify({'id': user['id'], 'username': user['username'],
                    'role': user['role'], 'display_name': user['display_name']})


# ══════════════════════════════════════════════════════════════
#  DATA MANAGEMENT ROUTES
# ══════════════════════════════════════════════════════════════

@app.route('/api/data-management/status')
def api_data_status():
    return jsonify(get_upload_summary())


@app.route('/api/data-management/upload', methods=['POST'])
@require_role('admin')
def api_upload_file():
    category = request.form.get('category')
    if not category or category not in FILE_REGISTRY:
        return jsonify({'error': f'Valid category required'}), 400
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    if not file.filename or not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({'error': 'Only .xlsx files accepted'}), 400
    success, message, filepath = save_uploaded_file(file, category)
    if not success:
        return jsonify({'error': message}), 400
    return jsonify({'status': 'ok', 'message': message, 'category': category,
                    'label': FILE_REGISTRY[category]['label']})


@app.route('/api/data-management/clear', methods=['POST'])
@require_role('admin')
def api_clear_upload():
    body = request.get_json(force=True)
    category = body.get('category')
    if category == 'all':
        cleared = clear_all_uploads()
        return jsonify({'status': 'ok', 'message': f'Cleared {cleared} uploads'})
    if not category or category not in FILE_REGISTRY:
        return jsonify({'error': 'Valid category required'}), 400
    success, message = clear_uploaded_file(category)
    return jsonify({'status': 'ok', 'message': message})


@app.route('/api/data-management/template/<category>')
def api_download_template(category):
    if category not in FILE_REGISTRY:
        return jsonify({'error': f'Unknown category: {category}'}), 404
    template_path = generate_template(category)
    if not template_path:
        return jsonify({'error': 'Could not generate template'}), 500
    return send_file(template_path, as_attachment=True,
                     download_name=f'{category}_template.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/api/data-management/recalculate', methods=['POST'])
def api_data_recalculate():
    try:
        STATE['loaded'] = False
        STATE['_load_error'] = None
        saved_overrides = dict(STATE['overrides'])
        _run_all()
        STATE['overrides'] = saved_overrides
        _apply_all_overrides()
        _recompute_downstream()
        return jsonify({'status': 'ok', 'message': 'All engines recalculated',
                        'data': _build_demo_object(), 'initiatives': STATE['initiatives'],
                        'waterfall': STATE['waterfall']})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ══════════════════════════════════════════════════════════════
#  MAIN ROUTES
# ══════════════════════════════════════════════════════════════

@app.route('/api/recommendations/<page_context>')
def api_recommendations(page_context):
    """Get contextual recommendations for a specific page."""
    if not STATE['loaded']: return jsonify({'error': 'Not loaded'}), 503
    recs = get_recommendations(page_context, STATE['data'], STATE['diagnostic'],
                                STATE['initiatives'], STATE['waterfall'], maturity=STATE.get('maturity'))
    return jsonify(recs)

@app.route('/api/initiative-linkage/<page_context>')
def api_initiative_linkage(page_context):
    """v12-#23: Get initiative linkage table for a diagnostic page (roadmap-aligned)."""
    if not STATE['loaded']: return jsonify({'error': 'Not loaded'}), 503
    linkage = get_initiative_linkage(page_context, STATE['data'], STATE['diagnostic'],
                                     STATE['initiatives'], STATE['waterfall'])
    return jsonify(linkage)

@app.route('/api/industries')
def api_industries():
    """Get available industry configurations."""
    return jsonify({'industries': get_available_industries()})

@app.route('/api/industry/<key>')
def api_industry_config(key):
    """Get specific industry configuration."""
    config = get_industry_config(key)
    if not config: return jsonify({'error': f'Unknown industry: {key}'}), 404
    return jsonify(config)


# ══════════════════════════════════════════════════════════════
#  MAIN PAGE ROUTES
# ══════════════════════════════════════════════════════════════

@app.route('/')
def index():
    if not STATE['loaded'] and not STATE.get('_load_error'):
        try:
            _run_all()
            print("[OK] ContactIQ engines loaded successfully")
        except Exception as e:
            err_msg = f"{type(e).__name__}: {e}"
            STATE['_load_error'] = err_msg
            print(f"\n{'='*60}\n[!] ENGINE LOAD FAILED\n[!] Error: {err_msg}\n{'='*60}\n")
            traceback.print_exc()

    user = get_current_user()
    user_info = {'username': user.get('username','admin'), 'display_name': user.get('display_name','Administrator'),
                 'role': user.get('role','supervisor')} if user else {'username':'admin','display_name':'Administrator','role':'supervisor'}
    data_summary = get_upload_summary()

    if STATE['loaded']:
        server_data = {'demo': _build_demo_object(), 'initiatives': STATE['initiatives'], 'waterfall': STATE['waterfall']}
        return render_template('index.html', server_data=json.dumps(server_data, default=str),
                               load_error=None, user_info=json.dumps(user_info),
                               data_summary=json.dumps(data_summary))
    return render_template('index.html', server_data=None, load_error=STATE.get('_load_error', None),
                           user_info=json.dumps(user_info), data_summary=json.dumps(data_summary))


# ══════════════════════════════════════════════════════════════
#  ENGINE API ROUTES (preserved from v7)
# ══════════════════════════════════════════════════════════════

@app.route('/api/data')
def api_data():
    if not STATE['loaded']:
        return jsonify({'error': 'Data not loaded', 'reason': STATE.get('_load_error', 'Unknown')}), 503
    bu = request.args.get('bu')
    if bu and bu != 'all':
        import copy
        demo = copy.deepcopy(_build_demo_object())
        if 'queues' in demo:
            demo['queues'] = [q for q in demo['queues'] if q.get('bu') == bu]
        # Recompute totals from filtered queues
        filtered_q = demo.get('queues', [])
        if filtered_q:
            demo['totalVolumeAnnual'] = sum(q.get('volume', 0) for q in filtered_q)
            demo['totalVolume'] = demo['totalVolumeAnnual']
        return jsonify(demo)
    return jsonify(_build_demo_object())

@app.route('/api/diagnostic')
def api_diagnostic():
    if not STATE['loaded']: return jsonify({'error':'Not loaded'}), 503
    bu = request.args.get('bu')
    if bu and bu != 'all':
        import copy
        diag = copy.deepcopy(STATE['diagnostic'])
        # Filter queue-level data by BU
        if 'queueScores' in diag:
            diag['queueScores'] = [q for q in diag['queueScores'] if q.get('bu') == bu]
        if 'subIntentAnalysis' in diag:
            diag['subIntentAnalysis'] = [s for s in diag['subIntentAnalysis']
                                          if any(q.get('bu') == bu for q in STATE['diagnostic'].get('queueScores', [])
                                                 if q.get('intent') == s.get('intent'))]
        # Recompute summary from filtered queues
        qs = diag.get('queueScores', [])
        if qs:
            scores = [q.get('overallScore', 0) for q in qs]
            diag['summary'] = {
                'total': len(qs),
                'avgScore': round(sum(scores) / len(scores), 1) if scores else 0,
                'red': sum(1 for q in qs if q.get('rating') == 'red'),
                'amber': sum(1 for q in qs if q.get('rating') == 'amber'),
                'green': sum(1 for q in qs if q.get('rating') == 'green'),
            }
        return jsonify(diag)
    return jsonify(STATE['diagnostic'])

@app.route('/api/maturity')
def api_maturity():
    if not STATE['loaded']: return jsonify({'error':'Not loaded'}), 503
    return jsonify(STATE['maturity'])

@app.route('/api/channel-strategy')
def api_channel_strategy():
    if not STATE['loaded']: return jsonify({'error':'Not loaded'}), 503
    return jsonify(STATE['channelStrategy'])

@app.route('/api/initiatives')
def api_initiatives():
    if not STATE['loaded']: return jsonify({'error':'Not loaded'}), 503
    return jsonify({'initiatives': STATE['initiatives'],
                    'enabledCount': sum(1 for i in STATE['initiatives'] if i.get('enabled')),
                    'totalCount': len(STATE['initiatives'])})

@app.route('/api/waterfall')
def api_waterfall():
    if not STATE['loaded']: return jsonify({'error':'Not loaded'}), 503
    bu = request.args.get('bu')
    if bu and bu != 'all':
        import copy
        wf = copy.deepcopy(STATE['waterfall'])
        # Scope to BU summary if available
        bu_data = wf.get('buSummary', {}).get(bu, {})
        if bu_data:
            wf['_buScoped'] = bu
            wf['_buData'] = bu_data
        return jsonify(wf)
    return jsonify(STATE['waterfall'])

@app.route('/api/risk')
def api_risk():
    if not STATE['loaded']: return jsonify({'error':'Not loaded'}), 503
    return jsonify(STATE['risk'])

@app.route('/api/workforce')
def api_workforce():
    if not STATE['loaded']: return jsonify({'error':'Not loaded'}), 503
    bu = request.args.get('bu')
    if bu and bu != 'all':
        import copy
        wk = copy.deepcopy(STATE['workforce'])
        # Filter byBU to scoped BU
        if 'byBU' in wk and isinstance(wk['byBU'], dict):
            scoped = {k: v for k, v in wk['byBU'].items() if k == bu}
            wk['byBU'] = scoped
            wk['_buScoped'] = bu
        return jsonify(wk)
    return jsonify(STATE['workforce'])

@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    try:
        STATE['overrides'] = {}; STATE['loaded'] = False; STATE['_load_error'] = None
        _run_all()
        return jsonify({'status':'ok', 'data': _build_demo_object(),
                        'initiatives': STATE['initiatives'], 'waterfall': STATE['waterfall']})
    except Exception as e:
        return jsonify({'status':'error','message':str(e)}), 500

@app.route('/api/recalculate', methods=['POST'])
def api_recalculate():
    try:
        body = request.get_json(force=True) if request.is_json else {}
        data = STATE['data']
        for key, value in body.get('params', {}).items():
            if key in data['params']:
                data['params'][key] = value; STATE['overrides'][key] = value
        if 'strategicDriver' in body.get('params', {}):
            data['params']['strategicDriver'] = body['params']['strategicDriver']
        _recompute_all_from_diagnostic()
        active_layer = body.get('activeLayer')
        if active_layer and active_layer != 'All Layers':
            scoped_inits = copy.deepcopy(STATE['initiatives'])
            for init in scoped_inits:
                if init.get('layer') != active_layer: init['enabled'] = False
            sw = run_waterfall(data, scoped_inits)
            sr = run_risk(scoped_inits, data)
            swf = run_workforce(data, sw, scoped_inits)
            return jsonify({'status':'ok','scoped':True,'activeLayer':active_layer,
                            'data':_build_demo_object(overrides={'waterfall':sw,'risk':sr,'workforce':swf,'initiatives':scoped_inits}),
                            'initiatives':scoped_inits,'waterfall':sw,'risk':sr,'workforce':swf,
                            'enabledCount':sum(1 for i in scoped_inits if i.get('enabled'))})
        return jsonify({'status':'ok','scoped':False,'data':_build_demo_object(),
                        'initiatives':STATE['initiatives'],'waterfall':STATE['waterfall'],
                        'risk':STATE['risk'],'workforce':STATE['workforce'],
                        'enabledCount':sum(1 for i in STATE['initiatives'] if i.get('enabled'))})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status':'error','message':str(e)}), 500

@app.route('/api/initiative/toggle', methods=['POST'])
@require_role('supervisor')
def api_toggle_initiative():
    body = request.get_json(force=True)
    init_id = body.get('id'); enabled = body.get('enabled')
    if not init_id or enabled is None: return jsonify({'error':'id and enabled required'}), 400
    STATE['overrides'][f"init_enabled_{init_id}"] = bool(enabled)
    for init in STATE['initiatives']:
        if init['id'] == init_id: init['enabled'] = bool(enabled); break
    _recompute_downstream()
    return jsonify({'status':'ok','enabledCount':sum(1 for i in STATE['initiatives'] if i.get('enabled')),
                    'waterfall':STATE['waterfall'],'risk':STATE['risk'],
                    'workforce':STATE['workforce'],'initiatives':STATE['initiatives']})

@app.route('/api/initiative/update', methods=['POST'])
@require_role('supervisor')
def api_update_initiative():
    body = request.get_json(force=True)
    init_id = body.get('id'); fields = body.get('fields', {})
    if not init_id: return jsonify({'error':'id required'}), 400
    fk = f"init_fields_{init_id}"
    if fk not in STATE['overrides']: STATE['overrides'][fk] = {}
    STATE['overrides'][fk].update(fields)
    for init in STATE['initiatives']:
        if init['id'] == init_id:
            for k, v in fields.items(): init[k] = v
            break
    _recompute_downstream()
    return jsonify({'status':'ok','enabledCount':sum(1 for i in STATE['initiatives'] if i.get('enabled')),
                    'waterfall':STATE['waterfall'],'risk':STATE['risk'],
                    'workforce':STATE['workforce'],'initiatives':STATE['initiatives']})

@app.route('/api/override', methods=['POST'])
@require_role('supervisor')
def api_override():
    body = request.get_json(force=True)
    key = body.get('key'); value = body.get('value')
    if not key: return jsonify({'error':'key required'}), 400
    STATE['overrides'][key] = value
    if key in STATE['data']['params']: STATE['data']['params'][key] = value
    _recompute_all_from_diagnostic()
    return jsonify({'status':'ok','data':_build_demo_object(),
                    'initiatives':STATE['initiatives'],'waterfall':STATE['waterfall']})

@app.route('/api/maturity/override', methods=['POST'])
def api_maturity_override():
    body = request.get_json(force=True)
    dimension = body.get('dimension'); score = body.get('score')
    if not dimension or score is None: return jsonify({'error':'dimension and score required'}), 400
    score = max(1.0, min(5.0, float(score)))
    STATE['overrides'][f"maturity_{dimension}"] = score
    mat = STATE['maturity']; dims = mat.get('dimensions', {})
    if dimension in dims:
        dims[dimension]['score'] = score; dims[dimension]['level'] = min(5, max(1, round(score)))
    if dims:
        from engines.maturity import DIMENSIONS, MATURITY_LEVELS
        overall = sum(dims[k]['score'] * dims[k].get('weight', 0.20) for k in dims if isinstance(dims[k], dict))
        mat['overall'] = round(overall, 2)
        mat['overallLevel'] = min(5, max(1, round(overall)))
        mat['levelInfo'] = MATURITY_LEVELS.get(mat['overallLevel'], {})
    return jsonify({'status':'ok','maturity': mat})

@app.route('/api/benchmarks/override', methods=['POST'])
@require_role('supervisor')
def api_benchmark_override():
    """Save benchmark overrides persistently across sessions."""
    body = request.get_json(force=True)
    overrides = body.get('benchmarks', {})
    if not overrides: return jsonify({'error': 'benchmarks object required'}), 400
    # Store each benchmark override
    for metric, value in overrides.items():
        STATE['overrides'][f"benchmark_{metric}"] = value
        # Apply to live benchmarks
        if metric in STATE['data'].get('benchmarks', {}):
            if isinstance(STATE['data']['benchmarks'][metric], dict):
                STATE['data']['benchmarks'][metric]['global'] = value
            else:
                STATE['data']['benchmarks'][metric] = value
    _recompute_all_from_diagnostic()
    return jsonify({'status': 'ok', 'message': f'{len(overrides)} benchmark(s) saved',
                    'data': _build_demo_object(), 'waterfall': STATE['waterfall']})

@app.route('/api/benchmarks/overrides', methods=['GET'])
def api_benchmark_overrides_get():
    """Retrieve saved benchmark overrides."""
    bm_overrides = {k.replace('benchmark_', ''): v for k, v in STATE['overrides'].items() if k.startswith('benchmark_')}
    return jsonify({'benchmarks': bm_overrides})

@app.route('/api/investment')
def api_investment():
    if not STATE['loaded']: return jsonify({'error':'Not loaded'}), 503
    wf = STATE['waterfall']
    return jsonify({'items':wf.get('investmentItems',[]),'summary':wf.get('investmentSummary',{}),'yearly':wf.get('investmentYearly',[])})

@app.route('/api/initiatives/batch', methods=['POST'])
@require_role('supervisor')
def api_batch_initiatives():
    body = request.get_json(force=True)
    for upd in body.get('updates', []):
        iid = upd.get('id')
        for init in STATE['initiatives']:
            if init['id'] == iid:
                if 'enabled' in upd:
                    init['enabled'] = bool(upd['enabled']); STATE['overrides'][f"init_enabled_{iid}"] = bool(upd['enabled'])
                for rk in ('rampYear1','rampYear2','rampYear3'):
                    if rk in upd: init[rk] = float(upd[rk]); STATE['overrides'][f"init_{rk}_{iid}"] = float(upd[rk])
                if 'priority' in upd: init['priority'] = upd['priority']
                break
    _recompute_downstream()
    return jsonify({'status':'ok','initiatives':STATE['initiatives'],'waterfall':STATE['waterfall']})

@app.route('/api/export')
def api_export():
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        wb = openpyxl.Workbook()
        hf = Font(bold=True, color='FFFFFF', size=11)
        hfill = PatternFill(start_color='2E2E38', end_color='2E2E38', fill_type='solid')
        tb = Border(left=Side(style='thin'),right=Side(style='thin'),top=Side(style='thin'),bottom=Side(style='thin'))
        def ws_write(ws, headers, rows):
            for c, h in enumerate(headers, 1):
                cell = ws.cell(row=1, column=c, value=h)
                cell.font=hf; cell.fill=hfill; cell.alignment=Alignment(horizontal='center'); cell.border=tb
            for r, row in enumerate(rows, 2):
                for c, val in enumerate(row, 1):
                    cell = ws.cell(row=r, column=c, value=val); cell.border=tb
            for col in ws.columns:
                ml = max(len(str(cell.value or '')) for cell in col)
                ws.column_dimensions[col[0].column_letter].width = min(ml+2, 40)
        data=STATE['data']; diag=STATE['diagnostic']; wf=STATE['waterfall']
        ws=wb.active; ws.title='Executive Summary'
        ws_write(ws, ['Metric','Value'], [
            ['Client', data['params'].get('clientName','')],['Industry', data['params'].get('industry','')],
            ['Total Volume (raw)', data['totalVolume']],
            ['Total Cost', f"${data['totalCost']:,.0f}"],['NPV', f"${wf.get('totalNPV',0):,.0f}"],
            ['IRR', f"{wf.get('irr',0):.1f}%"],['Total Investment', f"${wf.get('totalInvestment',0):,.0f}"],
            ['ROI', f"{wf.get('roi',0):.1f}%"],['Payback', f"{wf.get('payback',0):.1f} months"],
            ['Annual Saving (Year 3)', f"${wf.get('yearly',[-1])[-1].get('annualSaving',0) if wf.get('yearly') else 0:,.0f}"],['Enabled Initiatives', len(wf.get('enabledInits',[]))],
        ])
        # ── Initiatives ──
        ws5 = wb.create_sheet('Initiatives')
        ws_write(ws5, ['ID','Name','Layer','Lever','Enabled','Score','Annual Saving',
                       'Impl Risk','CX Risk','Ops Risk','Overall Risk','Rating'], [
            [i['id'],i['name'],i['layer'],i['lever'],'Yes' if i.get('enabled') else 'No',
             f"{i.get('matchScore',0):.1f}",f"${i.get('_annualSaving',0):,.0f}",
             i.get('implRisk',''),i.get('cxRisk',''),i.get('opsRisk',''),
             i.get('overallRisk',''),i.get('riskRating','')]
            for i in STATE['initiatives']
        ])
        # ── Waterfall ──
        ws6 = wb.create_sheet('Waterfall')
        ws_write(ws6, ['Year','Annual Saving','Cum Saving','NPV'], [
            [y['year'],f"${y['annualSaving']:,.0f}",
             f"${y['cumSaving']:,.0f}",f"${y['npv']:,.0f}"] for y in wf.get('yearly', [])
        ])
        # ── P2-6: BU Impact ──
        bu_summary = wf.get('buSummary', {})
        if bu_summary:
            wsbu = wb.create_sheet('BU Impact')
            bu_rows = []
            for bu, bd in bu_summary.items():
                for yr_idx, yr_data in enumerate(bd.get('yearly', [])):
                    bu_rows.append([bu, yr_idx+1,
                                    f"${yr_data.get('annualSaving', 0):,.0f}"])
            ws_write(wsbu, ['Business Unit','Year','Annual Saving'], bu_rows)
        # ── P2-6: Risk Register ──
        risk_data = STATE.get('risk', {})
        risk_inits = risk_data.get('initiatives', [])
        if risk_inits:
            wsrisk = wb.create_sheet('Risk Register')
            ws_write(wsrisk, ['ID','Name','Layer','Impl Risk','CX Risk','Ops Risk','Overall','Rating',
                              'Annual Saving','Mitigations'], [
                [r['id'],r['name'],r['layer'],r['implRisk'],r['cxRisk'],r['opsRisk'],
                 r['overallRisk'],r['rating'],f"${r['annualSaving']:,.0f}",
                 '; '.join(r.get('mitigations',[])) ]
                for r in risk_inits
            ])
        # ── P2-6: Workforce Transition ──
        wkf = STATE.get('workforce', {})
        transitions = wkf.get('transitions', [])
        if transitions:
            wswf = wb.create_sheet('Workforce Transition')
            ws_write(wswf, ['Role','Location','Sourcing','Year','Reduction','Attrited','Redeployed',
                            'Separated','Contract Adj','Transition Cost'], [
                [t.get('role',''),t.get('location',''),t.get('sourcing',''),
                 t.get('year',0),f"{t.get('reduction',0):.1f}",f"{t.get('attrited',0):.1f}",f"{t.get('redeployed',0):.1f}",
                 f"{t.get('separated',0):.1f}",f"{t.get('contractAdjustment',0):.1f}",
                 f"${t.get('totalTransitionCost',0):,.0f}"]
                for t in transitions
            ])
        # ── P2-6: Location Mix ──
        loc_matrix = data.get('locationCostMatrix', {})
        if loc_matrix:
            wsloc = wb.create_sheet('Location Cost Matrix')
            loc_rows = []
            for loc, srcs in loc_matrix.items():
                for src, costs in srcs.items():
                    loc_rows.append([loc, src, f"${costs.get('costPerFTE',0):,.0f}",
                                     f"${costs.get('hiringCost',0):,.0f}",
                                     f"{costs.get('attritionRate',0):.1%}"])
            ws_write(wsloc, ['Location','Sourcing','Cost/FTE','Hiring Cost','Attrition Rate'], loc_rows)

        fd, export_path = tempfile.mkstemp(suffix='.xlsx')
        os.close(fd)
        wb.save(export_path)
        return send_file(export_path, as_attachment=True, download_name='ContactIQ_Export.xlsx')
    except ImportError as e:
        return jsonify({'error': f'Export dependency missing: {e}. Install with: pip install openpyxl'}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error':str(e)}), 500


# ══════════════════════════════════════════════════════════════
#  LAYER FILTER — Server-side recompute (Option A)
# ══════════════════════════════════════════════════════════════

@app.route('/api/waterfall/layer', methods=['POST'])
def api_waterfall_by_layer():
    """Recompute waterfall with only initiatives from specified layer(s)."""
    if not STATE['loaded']: return jsonify({'error':'Not loaded'}), 503
    try:
        body = request.get_json(force=True)
        active_layer = body.get('layer', 'All Layers')
        
        if active_layer == 'All Layers':
            return jsonify({
                'status': 'ok', 'scoped': False, 'layer': 'All Layers',
                'waterfall': STATE['waterfall'],
                'initiatives': STATE['initiatives'],
                'enabledCount': sum(1 for i in STATE['initiatives'] if i.get('enabled')),
            })
        
        scoped_inits = copy.deepcopy(STATE['initiatives'])
        for init in scoped_inits:
            if init.get('layer') != active_layer:
                init['enabled'] = False
        
        data = STATE['data']
        sw = run_waterfall(data, scoped_inits)
        
        return jsonify({
            'status': 'ok', 'scoped': True, 'layer': active_layer,
            'waterfall': sw,
            'initiatives': scoped_inits,
            'enabledCount': sum(1 for i in scoped_inits if i.get('enabled')),
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# ══════════════════════════════════════════════════════════════
#  PDF EXPORT
# ══════════════════════════════════════════════════════════════

@app.route('/api/export/pdf')
def api_export_pdf():
    """Generate PDF report of current analysis."""
    try:
        from fpdf import FPDF
        
        data = STATE['data']; wf = STATE['waterfall']; diag = STATE['diagnostic']
        params = data['params']
        
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=20)
        
        # ── Cover Page ──
        pdf.add_page()
        pdf.set_fill_color(46, 46, 56)  # EY dark
        pdf.rect(0, 0, 210, 297, 'F')
        pdf.set_text_color(255, 230, 0)  # EY yellow
        pdf.set_font('Helvetica', 'B', 32)
        pdf.set_y(80)
        pdf.cell(0, 15, 'Contact Centre', ln=True, align='C')
        pdf.cell(0, 15, 'Transformation', ln=True, align='C')
        pdf.cell(0, 15, 'Business Case', ln=True, align='C')
        pdf.set_font('Helvetica', '', 14)
        pdf.set_text_color(255, 255, 255)
        pdf.ln(20)
        pdf.cell(0, 10, params.get('clientName', 'Client'), ln=True, align='C')
        pdf.cell(0, 10, params.get('industry', 'Industry'), ln=True, align='C')
        pdf.set_font('Helvetica', '', 10)
        pdf.ln(10)
        pdf.cell(0, 8, 'Generated by ContactIQ — Intelligent Contact Center Optimization Platform', ln=True, align='C')
        
        # ── Executive Summary ──
        pdf.add_page()
        pdf.set_text_color(46, 46, 56)
        pdf.set_font('Helvetica', 'B', 18)
        pdf.cell(0, 12, 'Executive Summary', ln=True)
        pdf.set_draw_color(255, 230, 0)
        pdf.set_line_width(1)
        pdf.line(10, pdf.get_y(), 80, pdf.get_y())
        pdf.ln(8)
        
        # Hero metrics
        metrics = [
            ('Total 3-Year Savings', f"${wf.get('totalSaving', 0):,.0f}"),
            ('Net Present Value', f"${wf.get('totalNPV', 0):,.0f}"),
            ('Return on Investment', f"{wf.get('roi', 0):.0f}%"),
            ('Payback Period', f"{wf.get('payback', 0):.1f} years"),
            ('Internal Rate of Return', f"{wf.get('irr', 0):.1f}%"),
            ('Total Investment', f"${wf.get('totalInvestment', 0):,.0f}"),
        ]
        pdf.set_font('Helvetica', '', 11)
        for label, value in metrics:
            pdf.set_font('Helvetica', 'B', 11)
            pdf.cell(90, 8, label, border='B')
            pdf.set_font('Helvetica', '', 11)
            pdf.cell(0, 8, value, border='B', ln=True)
        
        # ── Yearly Projections ──
        pdf.ln(8)
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 10, 'Yearly Projections', ln=True)
        pdf.set_font('Helvetica', 'B', 9)
        headers = ['Year', 'Annual Saving', 'Cumulative', 'NPV']
        col_w = [30, 45, 45, 45]
        pdf.set_fill_color(46, 46, 56)
        pdf.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            pdf.cell(col_w[i], 8, h, border=1, fill=True, align='C')
        pdf.ln()
        pdf.set_text_color(46, 46, 56)
        pdf.set_font('Helvetica', '', 9)
        for y in wf.get('yearly', []):
            pdf.cell(col_w[0], 7, str(y.get('year', '')), border=1, align='C')
            pdf.cell(col_w[1], 7, f"${y.get('annualSaving', 0):,.0f}", border=1, align='C')
            pdf.cell(col_w[2], 7, f"${y.get('cumSaving', 0):,.0f}", border=1, align='C')
            pdf.cell(col_w[3], 7, f"${y.get('npv', 0):,.0f}", border=1, align='C')
            pdf.ln()
        
        # ── Layer Breakdown ──
        pdf.ln(8)
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 10, 'Impact by Layer', ln=True)
        pdf.set_font('Helvetica', 'B', 9)
        lh = ['Layer', 'Annual Saving']
        lw = [100, 80]
        pdf.set_fill_color(46, 46, 56)
        pdf.set_text_color(255, 255, 255)
        for i, h in enumerate(lh):
            pdf.cell(lw[i], 8, h, border=1, fill=True, align='C')
        pdf.ln()
        pdf.set_text_color(46, 46, 56)
        pdf.set_font('Helvetica', '', 9)
        for layer, fte in wf.get('layerFTE', {}).items():
            saving = wf.get('layerSaving', {}).get(layer, 0)
            pdf.cell(lw[0], 7, layer, border=1)
            pdf.cell(lw[1], 7, f"${saving:,.0f}", border=1, align='C')
            pdf.ln()
        
        # ── Top Initiatives ──
        pdf.add_page()
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 10, 'Top Initiatives by Impact', ln=True)
        
        enabled = [i for i in STATE['initiatives'] if i.get('enabled') and i.get('_annualSaving', 0) > 0]
        enabled.sort(key=lambda x: x.get('_annualSaving', 0), reverse=True)
        
        pdf.set_font('Helvetica', 'B', 8)
        ih = ['#', 'Initiative', 'Layer', 'Lever', 'Annual Saving']
        iw = [8, 65, 35, 40, 40]
        pdf.set_fill_color(46, 46, 56)
        pdf.set_text_color(255, 255, 255)
        for i, h in enumerate(ih):
            pdf.cell(iw[i], 7, h, border=1, fill=True, align='C')
        pdf.ln()
        pdf.set_text_color(46, 46, 56)
        pdf.set_font('Helvetica', '', 8)
        for idx, init in enumerate(enabled[:20], 1):
            pdf.cell(iw[0], 6, str(idx), border=1, align='C')
            name = init['name'][:32] + '..' if len(init['name']) > 34 else init['name']
            pdf.cell(iw[1], 6, name, border=1)
            pdf.cell(iw[2], 6, init.get('layer', '')[:18], border=1, align='C')
            pdf.cell(iw[3], 6, init.get('lever', '').replace('_', ' ')[:20], border=1, align='C')
            pdf.cell(iw[4], 6, f"${init.get('_annualSaving', 0):,.0f}", border=1, align='C')
            pdf.ln()
        
        # ── Pool Utilization ──
        pdf.ln(8)
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 10, 'Opportunity Pool Utilization', ln=True)
        pools = wf.get('poolUtilization', wf.get('poolSummary', {}))
        if pools:
            pdf.set_font('Helvetica', 'B', 9)
            ph = ['Pool', 'Ceiling', 'Consumed', 'Remaining', 'Utilization']
            pw = [45, 35, 35, 35, 35]
            pdf.set_fill_color(46, 46, 56)
            pdf.set_text_color(255, 255, 255)
            for i, h in enumerate(ph):
                pdf.cell(pw[i], 8, h, border=1, fill=True, align='C')
            pdf.ln()
            pdf.set_text_color(46, 46, 56)
            pdf.set_font('Helvetica', '', 9)
            pool_items = pools.items() if isinstance(pools, dict) else enumerate(pools)
            for key, pool in pool_items:
                if isinstance(pool, dict):
                    ceil_val = pool.get('ceiling_fte', pool.get('ceiling', 0))
                    cons_val = pool.get('consumed_fte', pool.get('consumed', 0))
                    rem_val = pool.get('remaining_fte', pool.get('remaining', ceil_val - cons_val))
                    util_pct = pool.get('utilization_pct', round(cons_val / max(ceil_val, 1) * 100, 1))
                    pdf.cell(pw[0], 7, str(key).replace('_', ' ').title(), border=1)
                    pdf.cell(pw[1], 7, f"{ceil_val:,.1f}", border=1, align='C')
                    pdf.cell(pw[2], 7, f"{cons_val:,.1f}", border=1, align='C')
                    pdf.cell(pw[3], 7, f"{rem_val:,.1f}", border=1, align='C')
                    pdf.cell(pw[4], 7, f"{util_pct:.1f}%", border=1, align='C')
                    pdf.ln()
        
        # Footer
        pdf.ln(10)

        # ── Risk Assessment ──
        pdf.add_page()
        pdf.set_font('Helvetica', 'B', 18)
        pdf.set_text_color(46, 46, 56)
        pdf.cell(0, 12, 'Risk Assessment', ln=True)
        pdf.set_draw_color(255, 230, 0)
        pdf.set_line_width(1)
        pdf.line(10, pdf.get_y(), 80, pdf.get_y())
        pdf.ln(8)
        risk_data = STATE.get('risk', {})
        risk_summary = risk_data.get('summary', {})
        overall_risk = risk_summary.get('avgRisk', risk_summary.get('overallScore', 0))
        if overall_risk == 0 and risk_data.get('initiatives'):
            # Compute from initiatives if summary is empty
            risk_inits_list = risk_data.get('initiatives', [])
            if risk_inits_list:
                overall_risk = sum(r.get('overallRisk', 0) for r in risk_inits_list) / len(risk_inits_list)
        risk_level = 'Low' if overall_risk < 2 else ('Medium' if overall_risk < 3.5 else 'High')
        pdf.set_font('Helvetica', 'B', 12)
        pdf.cell(0, 8, f'Overall Risk Score: {overall_risk:.1f}/5 ({risk_level})', ln=True)
        pdf.ln(4)
        risk_dims = risk_data.get('dimensions', {})
        if risk_dims:
            pdf.set_font('Helvetica', 'B', 9)
            rh = ['Dimension', 'Score', 'Level', 'Mitigation']
            rw = [45, 25, 25, 95]
            pdf.set_fill_color(46, 46, 56)
            pdf.set_text_color(255, 255, 255)
            for i, h in enumerate(rh):
                pdf.cell(rw[i], 8, h, border=1, fill=True, align='C')
            pdf.ln()
            pdf.set_text_color(46, 46, 56)
            pdf.set_font('Helvetica', '', 8)
            for dim_name, dim_data in risk_dims.items():
                if isinstance(dim_data, dict):
                    s = dim_data.get('score', 0)
                    lvl = 'Low' if s < 2 else ('Medium' if s < 3.5 else 'High')
                    mit = dim_data.get('mitigation', dim_data.get('recommendation', ''))[:60]
                    pdf.cell(rw[0], 7, str(dim_name).replace('_', ' ').title(), border=1)
                    pdf.cell(rw[1], 7, f'{s:.1f}', border=1, align='C')
                    pdf.cell(rw[2], 7, lvl, border=1, align='C')
                    pdf.cell(rw[3], 7, mit, border=1)
                    pdf.ln()

        # ── Maturity Assessment ──
        pdf.ln(10)
        pdf.set_font('Helvetica', 'B', 18)
        pdf.cell(0, 12, 'Maturity Assessment', ln=True)
        pdf.set_draw_color(255, 230, 0)
        pdf.line(10, pdf.get_y(), 80, pdf.get_y())
        pdf.ln(8)
        mat = STATE.get('maturity', {})
        mat_overall = mat.get('overall', 0)
        mat_level = mat.get('overallLevel', 0)
        level_info = mat.get('levelInfo', {})
        pdf.set_font('Helvetica', 'B', 12)
        pdf.cell(0, 8, f'Overall Maturity: {mat_overall:.1f}/5 (Level {mat_level}: {level_info.get("name", "")})', ln=True)
        pdf.ln(4)
        mat_dims = mat.get('dimensions', {})
        if mat_dims:
            pdf.set_font('Helvetica', 'B', 9)
            mh = ['Dimension', 'Score', 'Level', 'Weight']
            mw = [55, 25, 25, 25]
            pdf.set_fill_color(46, 46, 56)
            pdf.set_text_color(255, 255, 255)
            for i, h in enumerate(mh):
                pdf.cell(mw[i], 8, h, border=1, fill=True, align='C')
            pdf.ln()
            pdf.set_text_color(46, 46, 56)
            pdf.set_font('Helvetica', '', 9)
            for dim, dd in mat_dims.items():
                if isinstance(dd, dict):
                    pdf.cell(mw[0], 7, str(dim).replace('_', ' ').title(), border=1)
                    pdf.cell(mw[1], 7, f'{dd.get("score", 0):.1f}', border=1, align='C')
                    pdf.cell(mw[2], 7, str(dd.get('level', '')), border=1, align='C')
                    pdf.cell(mw[3], 7, f'{dd.get("weight", 0.2):.0%}', border=1, align='C')
                    pdf.ln()

        # ── Channel Mix Summary ──
        pdf.add_page()
        pdf.set_font('Helvetica', 'B', 18)
        pdf.cell(0, 12, 'Channel Strategy', ln=True)
        pdf.set_draw_color(255, 230, 0)
        pdf.line(10, pdf.get_y(), 80, pdf.get_y())
        pdf.ln(8)
        cs = STATE.get('channel_strategy', {})
        channel_mix = cs.get('channelMix', [])
        if channel_mix:
            pdf.set_font('Helvetica', 'B', 9)
            ch_headers = ['Channel', 'Volume', 'Share %', 'Avg CSAT', 'Avg AHT', 'Avg CPC']
            ch_widths = [35, 30, 25, 25, 30, 30]
            pdf.set_fill_color(46, 46, 56)
            pdf.set_text_color(255, 255, 255)
            for i, h in enumerate(ch_headers):
                pdf.cell(ch_widths[i], 8, h, border=1, fill=True, align='C')
            pdf.ln()
            pdf.set_text_color(46, 46, 56)
            pdf.set_font('Helvetica', '', 9)
            for ch in channel_mix:
                pdf.cell(ch_widths[0], 7, ch.get('channel', ''), border=1)
                pdf.cell(ch_widths[1], 7, f"{ch.get('volume', 0):,}", border=1, align='C')
                pdf.cell(ch_widths[2], 7, f"{ch.get('pct', 0):.1f}%", border=1, align='C')
                pdf.cell(ch_widths[3], 7, f"{ch.get('avgCSAT', 0):.2f}", border=1, align='C')
                pdf.cell(ch_widths[4], 7, f"{ch.get('avgAHT_min', ch.get('avgAHT', 0)):.0f}s", border=1, align='C')
                pdf.cell(ch_widths[5], 7, f"${ch.get('avgCPC', 0):.2f}", border=1, align='C')
                pdf.ln()

        # ── Diagnostic Highlights ──
        pdf.ln(10)
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 10, 'Diagnostic Highlights', ln=True)
        pdf.set_font('Helvetica', '', 9)
        diag_text_items = [
            f"Total monthly volume: {data.get('totalVolume', 0):,} contacts across {len(data.get('queues', []))} queues",
            f"Business units: {', '.join(data.get('bus', []))}",
            f"Active channels: {', '.join(data.get('channels', []))}",
            f"Enabled initiatives: {sum(1 for i in STATE['initiatives'] if i.get('enabled'))} of {len(STATE['initiatives'])}",
        ]
        for item in diag_text_items:
            pdf.cell(0, 6, item, ln=True)

        # ── Disclaimer ──
        pdf.ln(6)
        pdf.set_font('Helvetica', 'I', 8)
        pdf.set_text_color(128, 128, 128)
        pdf.cell(0, 6, 'This report was generated by ContactIQ — Intelligent Contact Center Optimization Platform. Cap methodology: ContactIQ industry benchmarks.', ln=True, align='C')
        pdf.cell(0, 6, 'Secondary lever impacts weighted at 50%. Pool-based netting prevents double-counting.', ln=True, align='C')
        
        fd, export_path = tempfile.mkstemp(suffix='.pdf')
        os.close(fd)
        pdf.output(export_path)
        return send_file(export_path, as_attachment=True, download_name='ContactIQ_Report.pdf')
    except ImportError as e:
        return jsonify({'error': f'PDF export dependency missing: {e}. Install with: pip install fpdf2'}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════

def _build_channel_mix(queues):
    mix = {}
    for q in queues:
        ch = q['channel']
        if ch not in mix: mix[ch] = {'channel':ch,'volume':0,'csat_w':0,'aht_w':0,'cpc_w':0}
        mix[ch]['volume'] += q['volume']; mix[ch]['csat_w'] += q.get('csat',0)*q['volume']
        mix[ch]['aht_w'] += q.get('aht',0)*q['volume']; mix[ch]['cpc_w'] += q.get('cpc',0)*q['volume']
    total = sum(m['volume'] for m in mix.values()) or 1
    result = []
    for m in mix.values():
        v = m['volume']; aht_min = m['aht_w']/max(v,1)
        result.append({'channel':m['channel'],'volume':v,'pct':round(v/total*100,1),
                       'avgCSAT':round(m['csat_w']/max(v,1),2),'avgAHT':round(aht_min*60,0),
                       'avgAHT_min':round(aht_min,1),'avgCPC':round(m['cpc_w']/max(v,1),2)})
    result.sort(key=lambda x: x['volume'], reverse=True)
    return result


def _weighted_avg(queues, field):
    """Volume-weighted average of a queue-level metric."""
    total_vol = sum(q.get('volume', 0) for q in queues) or 1
    return round(sum(q.get(field, 0) * q.get('volume', 0) for q in queues) / total_vol, 4)

def _build_bu_mix(queues):
    mix = {}
    for q in queues:
        bu = q.get('bu','Unknown')
        if bu not in mix: mix[bu] = {'bu':bu,'volume':0,'csat_w':0}
        mix[bu]['volume'] += q['volume']; mix[bu]['csat_w'] += q.get('csat',0)*q['volume']
    total = sum(m['volume'] for m in mix.values()) or 1
    return sorted([{'bu':m['bu'],'volume':m['volume'],'pct':round(m['volume']/total*100,1),
                    'avgCSAT':round(m['csat_w']/max(m['volume'],1),2)} for m in mix.values()],
                  key=lambda x: x['volume'], reverse=True)

def _build_intent_mix(queues):
    mix = {}
    for q in queues:
        intent = q.get('intent','Unknown')
        if intent not in mix: mix[intent] = {'intent':intent,'volume':0,'csat_w':0}
        mix[intent]['volume'] += q['volume']; mix[intent]['csat_w'] += q.get('csat',0)*q['volume']
    total = sum(m['volume'] for m in mix.values()) or 1
    return sorted([{'intent':m['intent'],'volume':m['volume'],'pct':round(m['volume']/total*100,1),
                    'avgCSAT':round(m['csat_w']/max(m['volume'],1),2)} for m in mix.values()],
                  key=lambda x: x['volume'], reverse=True)

def _build_heatmap(queues):
    return [{'bu':q.get('bu',''),'intent':q.get('intent',''),'channel':q['channel'],
             'volume':q['volume'],'csat':q.get('csat',0),'aht':q.get('aht',0),
             'fcr':q.get('fcr',0),'cpc':q.get('cpc',0)} for q in queues]


def _enrich_sub_intents_for_downstream(sub_intent_analysis, initiatives, waterfall):
    """
    v12-#35: Enrich sub-intent data with initiative mapping and pool consumption.
    This data feeds into: Opportunity Buckets, Self-Service Feasibility, Channel Strategy.
    """
    enabled = [i for i in initiatives if i.get('enabled')]
    enriched = []
    for intent_data in sub_intent_analysis:
        intent = intent_data.get('intent', '')
        sub_intents = []
        for si in intent_data.get('subIntents', []):
            # Map initiatives to this sub-intent based on complexity and channel match
            complexity = si.get('complexity', 'moderate')
            auto_potential = si.get('automationPotential', 0)
            matched_initiatives = []
            for init in enabled:
                lever = init.get('lever', '')
                if complexity == 'simple' and lever in ('deflection', 'automation', 'self_service'):
                    matched_initiatives.append({'id': init['id'], 'name': init['name'], 'lever': lever})
                elif complexity == 'moderate' and lever in ('aht_reduction', 'automation', 'process_improvement'):
                    matched_initiatives.append({'id': init['id'], 'name': init['name'], 'lever': lever})
                elif complexity == 'complex' and lever in ('agent_assist', 'escalation_reduction', 'knowledge_mgmt'):
                    matched_initiatives.append({'id': init['id'], 'name': init['name'], 'lever': lever})

            # Feasibility score for self-service (used by Page 9)
            feasibility = round(auto_potential * 100)
            if complexity == 'complex': feasibility = min(feasibility, 30)
            elif complexity == 'moderate': feasibility = min(feasibility, 70)

            # Deflectable channels (used by Channel Strategy)
            deflectable_channels = []
            if auto_potential > 0.7: deflectable_channels.extend(['IVR', 'App/Self-Service', 'Chat'])
            elif auto_potential > 0.4: deflectable_channels.extend(['Chat', 'App/Self-Service'])
            elif auto_potential > 0.2: deflectable_channels.append('Chat')

            sub_intents.append({
                **si,
                'initiatives': matched_initiatives[:5],
                'feasibilityScore': feasibility,
                'deflectableChannels': deflectable_channels,
                'capturedFTE': 0,  # Will be populated by waterfall consumption
                'untappedFTE': 0,
            })

        enriched.append({
            'intent': intent,
            'totalVolume': intent_data.get('totalVolume', 0),
            'automationPotential': intent_data.get('automationPotential', 0),
            'totalDeflectable': intent_data.get('totalDeflectable', 0),
            'subIntents': sub_intents,
        })
    return enriched

def _build_cost_breakdown(data):
    total = data.get('totalCost',1) or 1
    return sorted([{'role':r['role'],'headcount':r['headcount'],'costPerFTE':r['costPerFTE'],
                    'totalCost':round(r['headcount']*r['costPerFTE']),
                    'pct':round(r['headcount']*r['costPerFTE']/total*100,1)} for r in data['roles']],
                  key=lambda x: x['totalCost'], reverse=True)


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
