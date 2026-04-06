"""
ContactIQ — Readiness Scoring Engine

Computes 3 readiness scores from diagnostic data:
  1. Automation Readiness Score
  2. Operating Model Gap Score
  3. Location Optimization Score

Also provides trigger gate rules for initiative qualification
and strategic driver alignment matrices.

Based on EY Contact Centre Transformation consulting methodology:
  Transformation Outcome = Automation & AI + Operating Model Design + Location Strategy
"""

import math

# ══════════════════════════════════════════════════════════════
#  STRATEGIC DRIVER ALIGNMENT MATRICES
# ══════════════════════════════════════════════════════════════

STRATEGIC_DRIVERS = {
    'cost_optimization': {
        'label': 'Cost Optimization',
        'description': 'Reduce cost per contact, optimize location mix, maximize automation ROI',
        'alignment': {
            'deflection':           0.90,
            'aht_reduction':        0.70,
            'escalation_reduction': 0.50,
            'repeat_reduction':     0.60,
            'shrinkage_reduction':  0.80,
            'cost_reduction':       1.00,
        },
    },
    'experience': {
        'label': 'Experience Improvement',
        'description': 'Increase FCR, CSAT, reduce friction and escalations',
        'alignment': {
            'deflection':           0.50,
            'aht_reduction':        0.70,
            'escalation_reduction': 0.90,
            'repeat_reduction':     1.00,
            'shrinkage_reduction':  0.30,
            'cost_reduction':       0.20,
        },
    },
    'growth': {
        'label': 'Growth & Revenue',
        'description': 'Revenue retention, proactive service, customer insights',
        'alignment': {
            'deflection':           0.60,
            'aht_reduction':        0.50,
            'escalation_reduction': 0.70,
            'repeat_reduction':     0.80,
            'shrinkage_reduction':  0.30,
            'cost_reduction':       0.40,
        },
    },
}


# ══════════════════════════════════════════════════════════════
#  TRIGGER GATE RULES (Hard Qualification Gates)
# ══════════════════════════════════════════════════════════════

def _build_trigger_rules():
    """
    Each lever has a hard trigger condition.
    Initiative MUST pass its lever trigger to be considered.
    Returns dict of lever -> { check_fn, pass_reason_template, fail_reason_template }
    """
    return {
        'deflection': {
            'check': lambda ctx: ctx['repeatableIntentPct'] > 0.30,
            'pass_reason': 'Repeatable intent at {repeatableIntentPct:.0%} exceeds 30% threshold — deflection viable',
            'fail_reason': 'Repeatable intent at {repeatableIntentPct:.0%} below 30% — insufficient deflection opportunity',
        },
        'aht_reduction': {
            'check': lambda ctx: ctx['avgAHT'] > ctx['benchmarkAHT'] * 0.90,
            'pass_reason': 'AHT ({avgAHT:.0f}s) above 90% of benchmark ({benchmarkAHT:.0f}s) — AHT reduction opportunity exists',
            'fail_reason': 'AHT ({avgAHT:.0f}s) already near/below benchmark ({benchmarkAHT:.0f}s) — limited AHT opportunity',
        },
        'escalation_reduction': {
            'check': lambda ctx: ctx['avgEscalation'] > 0.10,
            'pass_reason': 'Escalation rate ({avgEscalation:.1%}) exceeds 10% threshold — reduction opportunity exists',
            'fail_reason': 'Escalation rate ({avgEscalation:.1%}) already healthy below 10% — limited escalation opportunity',
        },
        'repeat_reduction': {
            'check': lambda ctx: ctx['avgFCR'] < ctx['fcrTarget'],
            'pass_reason': 'FCR ({avgFCR:.1%}) below target ({fcrTarget:.1%}) — repeat reduction needed',
            'fail_reason': 'FCR ({avgFCR:.1%}) at or above target ({fcrTarget:.1%}) — repeats not a priority issue',
        },
        'shrinkage_reduction': {
            'check': lambda ctx: ctx['avgUtilization'] < 0.75,
            'pass_reason': 'Utilization ({avgUtilization:.0%}) below 75% — shrinkage/efficiency opportunity exists',
            'fail_reason': 'Utilization ({avgUtilization:.0%}) healthy above 75% — limited shrinkage opportunity',
        },
        'cost_reduction': {
            'check': lambda ctx: ctx['locationScore'] > 1.0,
            'pass_reason': 'Cost ratio ({locationScore:.2f}x benchmark) indicates cost reduction opportunity',
            'fail_reason': 'Cost ratio ({locationScore:.2f}x benchmark) at or below market — limited cost opportunity',
        },
    }

TRIGGER_RULES = _build_trigger_rules()


# ══════════════════════════════════════════════════════════════
#  COMPLEXITY & RISK MAPPINGS
# ══════════════════════════════════════════════════════════════

COMPLEXITY_MAP = {
    'low':    1.5,
    'medium': 3.0,
    'high':   5.0,
}

EFFORT_TO_IMPL_RISK = {
    'low':    1.0,
    'medium': 2.5,
    'high':   4.0,
}


# ══════════════════════════════════════════════════════════════
#  STAGE CLASSIFICATION
# ══════════════════════════════════════════════════════════════

STAGE_THRESHOLDS = [
    # (max_readiness, stage_label, month_range, description)
    (0.40, 'AI Base',     (1, 6),   'Foundational — self-service bots, agent assist, quick wins'),
    (0.70, 'AI Enhanced', (4, 12),  'Orchestration — workflow automation, predictive routing'),
    (1.01, 'Autonomous',  (9, 18),  'Self-healing — closed-loop AI, autonomous workflows'),
]

EFFORT_MONTH_OFFSET = {'low': 0, 'medium': 2, 'high': 4}


# ══════════════════════════════════════════════════════════════
#  MAIN: COMPUTE READINESS CONTEXT
# ══════════════════════════════════════════════════════════════

def compute_readiness(data, diagnostic, maturity=None):
    """
    Compute the 3 EY readiness scores + derived context from client data.
    v12-#43: Maturity scores now modulate readiness per layer.
    This context object is used by both initiative scoring and channel strategy.

    Returns dict with all computed values.
    """
    queues = data.get('queues', [])
    roles = data.get('roles', [])
    params = data.get('params', {})
    benchmarks = data.get('benchmarks', {})
    total_volume = data.get('totalVolume', 1)
    total_fte = data.get('totalFTE', 1)
    total_cost = data.get('totalCost', 0)
    avg_aht_min = data.get('avgAHT', 5.0)  # data_loader outputs AHT in MINUTES
    avg_aht = avg_aht_min * 60  # convert to seconds for capacity calculations
    avg_fcr = data.get('avgFCR', 0.70)
    avg_csat = data.get('avgCSAT', 3.5)

    # ── Derived metrics ──

    # Repeatable intent % = volume where complexity < 0.35 / total
    repeatable_vol = sum(q['volume'] for q in queues if q.get('complexity', 0.5) < 0.35)
    repeatable_intent_pct = repeatable_vol / max(total_volume, 1)

    # Low complexity % = volume where complexity < 0.40
    low_complexity_vol = sum(q['volume'] for q in queues if q.get('complexity', 0.5) < 0.40)
    low_complexity_pct = low_complexity_vol / max(total_volume, 1)

    # Average escalation rate (volume-weighted)
    total_esc_vol = sum(q['volume'] * q.get('escalation', 0) for q in queues)
    avg_escalation = total_esc_vol / max(total_volume, 1)

    # Utilization = totalVolume / (totalFTE * monthlyCapacityPerAgent)
    # monthlyCapacity = (160 work hrs * 3600s) / avgAHT_seconds
    monthly_capacity = (160 * 3600) / max(avg_aht, 60)
    avg_utilization = total_volume / max(total_fte * monthly_capacity, 1)
    avg_utilization = min(1.0, avg_utilization)  # cap at 100%

    # ACW proxy = AHT_seconds * 0.25 (industry standard)
    acw_proxy = avg_aht * 0.25

    # Location cost ratio
    avg_cost_per_fte = total_cost / max(total_fte, 1) if total_cost > 0 else 55000
    benchmark_cost = benchmarks.get('costPerFTE', 50000)
    location_score = avg_cost_per_fte / max(benchmark_cost, 1)

    # CR-FIX-BENCH: Use resolve_benchmark for consistent benchmark access
    from engines.data_loader import resolve_benchmark
    _bm_aht, _, _ = resolve_benchmark(benchmarks, 'AHT')
    _bm_acw_raw, _, _ = resolve_benchmark(benchmarks, 'ACW')
    _bm_esc, _, _ = resolve_benchmark(benchmarks, 'Escalation')
    benchmark_aht = (_bm_aht or 5.7) * 60   # benchmark is in minutes, convert to seconds
    benchmark_acw = (_bm_acw_raw or 1.4) * 60 if _bm_acw_raw else 85  # fallback 85 sec
    benchmark_escalation = _bm_esc or 0.10
    csat_target = params.get('csatTarget', 4.2)
    fcr_target = params.get('fcrTarget', 0.82)

    # Channel volumes
    channel_volumes = {}
    for q in queues:
        ch = q['channel']
        channel_volumes[ch] = channel_volumes.get(ch, 0) + q['volume']

    channels_used = set(channel_volumes.keys())
    role_names = set(r['role'] for r in roles)

    # ── Automation Readiness Score (0.0 – 1.0) ──
    auto_comp1 = repeatable_intent_pct * 0.4
    auto_comp2 = min(1.0, acw_proxy / max(benchmark_acw, 1)) * 0.3
    auto_comp3 = min(1.0, avg_aht / max(benchmark_aht, 1)) * 0.3
    automation_readiness = round(auto_comp1 + auto_comp2 + auto_comp3, 4)

    # ── Operating Model Gap Score (0.0 – 1.0) ──
    opmodel_comp1 = min(1.0, avg_escalation / max(benchmark_escalation, 0.01)) * 0.5
    opmodel_comp2 = max(0, (1.0 - avg_utilization)) * 0.5
    opmodel_gap = round(opmodel_comp1 + opmodel_comp2, 4)

    # ── Location Optimization Score (0.0 – 2.0+) ──
    # >1.0 means overpaying vs market
    loc_score = round(location_score, 4)

    # ── Problem levers from diagnostic ──
    problem_levers = set()
    if diagnostic:
        for pa in diagnostic.get('problemAreas', []):
            m = pa.get('metric', '').lower()
            if m in ('aht',):
                problem_levers.add('aht_reduction')
            elif m in ('fcr',):
                problem_levers.add('repeat_reduction')
            elif m in ('escalation',):
                problem_levers.add('escalation_reduction')
            elif m in ('cpc',):
                problem_levers.update(['cost_reduction', 'deflection'])

    # ── Build context object ──
    ctx = {
        # Raw metrics
        'avgAHT': avg_aht,
        'avgFCR': avg_fcr,
        'avgCSAT': avg_csat,
        'avgEscalation': avg_escalation,
        'avgUtilization': avg_utilization,
        'repeatableIntentPct': repeatable_intent_pct,
        'lowComplexityPct': low_complexity_pct,
        'acwProxy': acw_proxy,
        'totalVolume': total_volume,
        'totalFTE': total_fte,
        'totalCost': total_cost,
        'avgCostPerFTE': avg_cost_per_fte,

        # Benchmarks
        'benchmarkAHT': benchmark_aht,
        'benchmarkACW': benchmark_acw,
        'benchmarkEscalation': benchmark_escalation,
        'benchmarkCostPerFTE': benchmark_cost,
        'csatTarget': csat_target,
        'fcrTarget': fcr_target,

        # 3 Readiness Scores
        'automationReadiness': automation_readiness,
        'opModelGap': opmodel_gap,
        'locationScore': loc_score,

        # Score components (for transparency/display)
        'automationComponents': {
            'repeatableIntent': round(auto_comp1, 4),
            'acwGap': round(auto_comp2, 4),
            'ahtGap': round(auto_comp3, 4),
        },
        'opModelComponents': {
            'escalationGap': round(opmodel_comp1, 4),
            'utilizationGap': round(opmodel_comp2, 4),
        },

        # Lookups
        'channelVolumes': channel_volumes,
        'channelsUsed': channels_used,
        'roleNames': role_names,
        'problemLevers': problem_levers,

        # Strategic driver (from params, default cost_optimization)
        'strategicDriver': params.get('strategicDriver', 'cost_optimization'),

        # Readiness map by layer
        'readinessMap': {
            'AI & Automation': automation_readiness,
            'Operating Model': opmodel_gap,
            'Location Strategy': min(1.0, loc_score / 2.0),  # normalize 0-2 to 0-1
        },
    }

    # v12-#43: Modulate readiness by maturity scores
    if maturity:
        mat_dims = maturity.get('dimensions', {})
        overall_mat = maturity.get('overall', 3.0)
        # Map maturity dimensions to layers:
        # AI & Automation ← technology + data dimensions
        # Operating Model ← process + people dimensions
        # Location Strategy ← unaffected (geography-driven)
        tech_mat = mat_dims.get('technology', {}).get('score', overall_mat)
        data_mat = mat_dims.get('data', {}).get('score', overall_mat)
        proc_mat = mat_dims.get('process', {}).get('score', overall_mat)
        people_mat = mat_dims.get('people', {}).get('score', overall_mat)

        # Maturity multiplier: score/5 ranges 0.2-1.0
        # Low maturity (1-2) → 0.2-0.4x → dampens AI readiness
        # High maturity (4-5) → 0.8-1.0x → full readiness preserved
        ai_mat_factor = ((tech_mat + data_mat) / 2) / 5.0
        om_mat_factor = ((proc_mat + people_mat) / 2) / 5.0

        ctx['readinessMap']['AI & Automation'] = round(
            ctx['readinessMap']['AI & Automation'] * (0.4 + 0.6 * ai_mat_factor), 4)
        ctx['readinessMap']['Operating Model'] = round(
            ctx['readinessMap']['Operating Model'] * (0.4 + 0.6 * om_mat_factor), 4)

        # Store maturity context for downstream use
        ctx['maturityOverall'] = overall_mat
        ctx['maturityLevel'] = maturity.get('overallLevel', 2)
        ctx['maturityDimensions'] = {k: v.get('score', 3.0) for k, v in mat_dims.items()}
        ctx['maturityFactors'] = {
            'AI & Automation': round(ai_mat_factor, 3),
            'Operating Model': round(om_mat_factor, 3),
        }

    return ctx


def check_trigger(lever, ctx):
    """
    Check if a lever's trigger condition is met.
    Returns (passed: bool, reason: str)
    """
    rule = TRIGGER_RULES.get(lever)
    if not rule:
        return True, 'No trigger rule defined for this lever'

    try:
        passed = rule['check'](ctx)
        template = rule['pass_reason'] if passed else rule['fail_reason']
        reason = template.format(**ctx)
        return passed, reason
    except Exception as e:
        return True, f'Trigger check error ({e}) — defaulting to pass'


def get_alignment(strategic_driver, lever):
    """Get alignment score (0-1) for a lever under the given strategic driver."""
    driver = STRATEGIC_DRIVERS.get(strategic_driver, STRATEGIC_DRIVERS['cost_optimization'])
    return driver['alignment'].get(lever, 0.5)


def classify_stage(layer_readiness, effort):
    """
    Classify initiative into AI Base / Enhanced / Autonomous based on readiness.
    Returns (stage, start_month, description)
    """
    offset = EFFORT_MONTH_OFFSET.get(effort, 2)
    for max_r, stage, month_range, desc in STAGE_THRESHOLDS:
        if layer_readiness < max_r:
            return stage, month_range[0] + offset, desc
    return 'Autonomous', 9 + offset, 'Advanced autonomous capability'


def compute_risk(init):
    """
    Compute composite risk score (1.0 – 5.0) for an initiative.
    """
    effort = init.get('effort', 'medium')
    impl_risk = EFFORT_TO_IMPL_RISK.get(effort, 2.5)

    # CX risk: initiatives that HURT CSAT get penalized
    csat_impact = init.get('csatImpact', 0)
    if csat_impact >= 0:
        cx_risk = 1.0
    else:
        cx_risk = min(5.0, 1.0 + abs(csat_impact) * 20)

    # Ops risk: longer ramp = higher risk
    ramp = init.get('ramp', 6)
    ops_risk = min(5.0, 1.0 + ramp / 6.0)

    composite = (impl_risk * 0.4) + (cx_risk * 0.3) + (ops_risk * 0.3)
    return round(composite, 2)
