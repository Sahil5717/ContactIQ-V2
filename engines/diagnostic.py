"""
ContactIQ — Diagnostic Engine
CR-009: Root cause analysis with fallback for no-red queues
CR-011: Cost analysis with missing field handling
CR-016: Mismatch summary with unique channel rationales
v12-#8: Fix friction/health scoring — escalation gets 2x gap amplifier, wasted spend includes friction waste
v12-#13: Wasted spend now computed from both CPC excess AND friction-flagged red queues
"""
import math

# v12-#8: Escalation weight increased from 0.15 to 0.20 (taken from AHT/CSAT)
HEALTH_WEIGHTS = {'aht':0.22,'fcr':0.20,'csat':0.23,'escalation':0.20,'cpc':0.15}
METRIC_DIRECTIONS = {'aht':'lower','fcr':'higher','csat':'higher','escalation':'lower','cpc':'lower'}

# v12-#8: Gap amplifiers — escalation gaps are amplified because a 22% escalation rate
# vs a 12% benchmark is much more severe than the raw percentage suggests
GAP_AMPLIFIERS = {'aht': 1.0, 'fcr': 1.0, 'csat': 1.0, 'escalation': 1.8, 'cpc': 1.0}

def run_diagnostic(data):
    queues = data['queues']; roles = data['roles']; params = data['params']
    benchmarks = data.get('benchmarks',{})
    total_vol = data['totalVolume']; total_fte = data['totalFTE']
    avg_csat = data['avgCSAT']

    # ── Per-queue scoring ──
    queue_scores = []
    for q in queues:
        scores = {}; details = {}
        for m in HEALTH_WEIGHTS:
            val = q.get(m); bench = _get_bench(benchmarks, q['channel'], m, params)
            # CR-FIX-AHT: Queue AHT is stored in MINUTES (from ETL).
            # Benchmarks are in SECONDS (360=6min, 420=7min, etc.).
            # Convert to seconds at the diagnostic boundary for correct comparison.
            if m == 'aht' and val is not None:
                val = val * 60  # minutes → seconds
            if val is None or bench is None:
                scores[m] = 50; details[m] = {'value':0,'benchmark':0,'gap':0,'score':50,'rating':'grey'}; continue
            gap = _calc_gap(val, bench, m)
            # v12-#8: Apply gap amplifier for escalation to make high escalation rates
            # produce meaningful red scores instead of lingering in amber
            amplifier = GAP_AMPLIFIERS.get(m, 1.0)
            sc = max(0, min(100, 50 + gap * amplifier * 50))
            scores[m] = sc
            details[m] = {'value':round(val,2),'benchmark':round(bench,2),'gap':round(gap,3),
                          'score':round(sc,1),'rating':'green' if sc>=70 else 'amber' if sc>=40 else 'red'}
        overall = sum(scores[m]*HEALTH_WEIGHTS[m] for m in HEALTH_WEIGHTS)
        # v12-#8: Friction flag — queues with ANY metric in critical territory get flagged
        has_friction = any(d['rating'] == 'red' for d in details.values())
        friction_score = _compute_friction_score(q, details, benchmarks, params)
        queue_scores.append({
            'queue':q['queue'],'channel':q['channel'],'volume':q['volume'],
            'intent': q.get('intent', 'Unknown'), 'bu': q.get('bu', 'Unknown'),
            'overallScore':round(overall,1),'rating':'green' if overall>=70 else 'amber' if overall>=40 else 'red',
            'metrics':details,'complexity':q.get('complexity',0.5),
            'hasFriction': has_friction, 'frictionScore': friction_score,
        })

    # ── Summary metrics ──
    green = sum(1 for qs in queue_scores if qs['rating']=='green')
    amber = sum(1 for qs in queue_scores if qs['rating']=='amber')
    red = sum(1 for qs in queue_scores if qs['rating']=='red')

    # ── Problem areas ──
    problem_areas = []
    for qs in queue_scores:
        for m, d in qs['metrics'].items():
            if d['rating'] == 'red':
                problem_areas.append({'queue':qs['queue'],'channel':qs['channel'],'metric':m,
                                      'value':d['value'],'benchmark':d['benchmark'],'gap':d['gap'],'score':d['score']})
    problem_areas.sort(key=lambda x: x['score'])

    # ── CR-009: Root cause analysis (with fallback) ──
    root_causes = _build_root_causes(queue_scores, problem_areas, queues, params)

    # ── CR-011: Cost analysis ──
    cost_analysis = _build_cost_analysis(queues, roles, params, benchmarks, queue_scores)

    # ── Channel summary ──
    channel_summary = _build_channel_summary(queues, queue_scores, benchmarks, params)

    # ── CR-016: Mismatch summary ──
    mismatch = _build_mismatch_summary(queues, queue_scores, params)

    return {
        'queueScores': queue_scores,
        'summary': {'green':green,'amber':amber,'red':red,'total':len(queue_scores),
                    'avgScore':round(sum(qs['overallScore'] for qs in queue_scores)/max(len(queue_scores),1),1)},
        'problemAreas': problem_areas,
        'rootCauses': root_causes,
        'costAnalysis': cost_analysis,
        'channelSummary': channel_summary,
        'mismatch': mismatch,
        'subIntentAnalysis': build_sub_intent_analysis(queues),
    }


def _compute_friction_score(queue, metric_details, benchmarks, params):
    """
    v12-#8: Compute a composite friction score for a queue.
    Friction = weighted combination of escalation severity, AHT excess, FCR shortfall, and repeat contact proxy.
    Score 0-100 where higher = more friction. Hotspot threshold = 60.
    """
    friction = 0
    # Escalation: primary friction driver (40% weight)
    esc = metric_details.get('escalation', {})
    if esc.get('rating') != 'grey':
        esc_val = esc.get('value', 0)
        esc_bench = esc.get('benchmark', 0.12)
        if esc_bench > 0:
            esc_ratio = max(0, (esc_val - esc_bench) / esc_bench)
            friction += min(100, esc_ratio * 120) * 0.40

    # AHT excess: secondary friction (25% weight)
    aht = metric_details.get('aht', {})
    if aht.get('rating') != 'grey':
        aht_val = aht.get('value', 0)
        aht_bench = aht.get('benchmark', 360)
        if aht_bench > 0:
            aht_ratio = max(0, (aht_val - aht_bench) / aht_bench)
            friction += min(100, aht_ratio * 100) * 0.25

    # FCR shortfall: (20% weight)
    fcr = metric_details.get('fcr', {})
    if fcr.get('rating') != 'grey':
        fcr_val = fcr.get('value', 1)
        fcr_bench = fcr.get('benchmark', 0.75)
        if fcr_bench > 0:
            fcr_ratio = max(0, (fcr_bench - fcr_val) / fcr_bench)
            friction += min(100, fcr_ratio * 100) * 0.20

    # CSAT shortfall: (15% weight)
    csat = metric_details.get('csat', {})
    if csat.get('rating') != 'grey':
        csat_val = csat.get('value', 5)
        csat_bench = csat.get('benchmark', 4.0)
        if csat_bench > 0:
            csat_ratio = max(0, (csat_bench - csat_val) / csat_bench)
            friction += min(100, csat_ratio * 100) * 0.15

    return round(min(100, friction), 1)


def _get_bench(benchmarks, channel, metric, params):
    """CR-FIX-BENCH: Unified benchmark access via data_loader.resolve_benchmark.
    Converts AHT from canonical minutes (in benchmark config) to seconds for diagnostic scoring."""
    from engines.data_loader import resolve_benchmark
    val, _tq, _src = resolve_benchmark(benchmarks, metric.upper() if metric in ('aht','fcr','csat','cpc','ces') else metric.capitalize(), channel=channel)
    if val is None or val == 0:
        # Hard fallback if resolve_benchmark returns nothing
        defaults = {'aht':{'Voice':360,'Chat':420,'Email':600,'IVR':120,'App/Self-Service':180,'SMS/WhatsApp':300,'Social Media':300,'Retail/Walk-in':600},
                    'fcr':{'Voice':0.75,'Chat':0.72,'Email':0.65,'IVR':0.80,'App/Self-Service':0.85,'SMS/WhatsApp':0.70,'Social Media':0.65,'Retail/Walk-in':0.80},
                    'csat':{'Voice':4.0,'Chat':3.8,'Email':3.5,'IVR':3.5,'App/Self-Service':4.0,'SMS/WhatsApp':3.8,'Social Media':3.5,'Retail/Walk-in':4.2},
                    'escalation':{'Voice':0.12,'Chat':0.10,'Email':0.08,'IVR':0.05,'App/Self-Service':0.03,'SMS/WhatsApp':0.08,'Social Media':0.10,'Retail/Walk-in':0.15},
                    'cpc':{'Voice':8.50,'Chat':5.00,'Email':4.00,'IVR':1.50,'App/Self-Service':0.50,'SMS/WhatsApp':3.00,'Social Media':4.00,'Retail/Walk-in':15.00}}
        return defaults.get(metric,{}).get(channel)
    # AHT: benchmark config stores in minutes, diagnostic needs seconds
    if metric == 'aht':
        val = val * 60
    return val


def _calc_gap(val, bench, metric):
    if bench == 0: return 0
    if METRIC_DIRECTIONS[metric] == 'lower':
        return (bench - val) / bench
    else:
        return (val - bench) / bench


def _build_root_causes(queue_scores, problem_areas, queues, params):
    """CR-009: Root cause for each red/amber queue; fallback if none red."""
    root_causes = []
    targets = [qs for qs in queue_scores if qs['rating']=='red']
    if not targets:
        targets = sorted([qs for qs in queue_scores if qs['rating']=='amber'], key=lambda x: x['overallScore'])[:5]
    if not targets:
        targets = sorted(queue_scores, key=lambda x: x['overallScore'])[:3]

    for qs in targets:
        worst = min(qs['metrics'].items(), key=lambda x: x[1]['score'])
        m_name, m_data = worst
        cause_map = {
            'aht': f"High handle time ({m_data['value']:.0f}s vs {m_data['benchmark']:.0f}s benchmark) — likely driven by agent skill gaps, complex processes, or inadequate knowledge tools.",
            'fcr': f"Low first-contact resolution ({m_data['value']:.0%} vs {m_data['benchmark']:.0%}) — repeat contacts suggest unresolved issues, incomplete information, or process gaps.",
            'csat': f"Below-target satisfaction ({m_data['value']:.1f} vs {m_data['benchmark']:.1f}) — root causes may include long wait times, agent capability gaps, or channel friction.",
            'escalation': f"Elevated escalation rate ({m_data['value']:.1%} vs {m_data['benchmark']:.1%}) — indicates L1 empowerment gaps or complex issues not matched to skill level.",
            'cpc': f"High cost per contact (${m_data['value']:.2f} vs ${m_data['benchmark']:.2f}) — driven by channel cost structure, overstaffing, or inefficient routing.",
        }
        root_causes.append({
            'queue': qs['queue'], 'channel': qs['channel'], 'rating': qs['rating'],
            'worstMetric': m_name, 'score': m_data['score'],
            'rootCause': cause_map.get(m_name, f"Performance gap in {m_name}"),
            'recommendation': _root_cause_recommendation(m_name, qs['channel']),
        })
    return root_causes


def _root_cause_recommendation(metric, channel):
    recs = {
        'aht': {'Voice':'Deploy AI Agent Assist + knowledge base search to cut handle time.',
                'Chat':'Implement canned responses and smart routing to reduce AHT.',
                'Email':'Automate templated responses and deploy document AI.',
                '_default':'Implement process streamlining and agent assist tooling.'},
        'fcr': {'Voice':'Improve agent training and empower L1 with decision authority.',
                'Chat':'Deploy guided resolution flows and escalation path redesign.',
                '_default':'Implement root-cause fix program and knowledge management overhaul.'},
        'csat': {'Voice':'Reduce wait times, improve agent soft skills, enable callback.',
                 'Chat':'Improve bot handoff experience and response speed.',
                 '_default':'Address primary dissatisfiers through journey mapping.'},
        'escalation': {'_default':'Redesign escalation paths and empower frontline agents.'},
        'cpc': {'Voice':'Migrate simple queries to digital channels and automate where possible.',
                '_default':'Optimize channel mix and automate low-complexity interactions.'},
    }
    mr = recs.get(metric, {})
    return mr.get(channel, mr.get('_default', 'Review processes and implement targeted improvements.'))


def _build_cost_analysis(queues, roles, params, benchmarks, queue_scores):
    """CR-011: Robust cost analysis with missing field handling.
    v12-#13: Wasted spend now includes friction-based waste from red/amber queues,
    not just CPC excess above benchmark."""
    total_cost = sum(r['headcount'] * r['costPerFTE'] for r in roles)
    total_vol = sum(q['volume'] for q in queues) or 1
    blended_cpc = total_cost / (total_vol * 12) if total_vol > 0 else 0

    channel_costs = {}
    for q in queues:
        ch = q['channel']
        if ch not in channel_costs:
            channel_costs[ch] = {'volume':0,'cost':0,'benchmark_cpc':0}
        channel_costs[ch]['volume'] += q['volume']
        cpc = q.get('cpc') or q.get('costPerContact') or blended_cpc
        channel_costs[ch]['cost'] += q['volume'] * cpc * 12
        b = _get_bench(benchmarks, ch, 'cpc', params)
        channel_costs[ch]['benchmark_cpc'] = b if b else blended_cpc

    cost_by_channel = []
    wasted_cpc_excess = 0
    for ch, cd in channel_costs.items():
        v = cd['volume']; c = cd['cost']; cpc = c/(v*12) if v>0 else 0
        bench = cd['benchmark_cpc'] or cpc
        excess = max(0, cpc - bench) * v * 12
        wasted_cpc_excess += excess
        cost_by_channel.append({'channel':ch,'annualCost':round(c),'volume':v,'cpc':round(cpc,2),
                                'benchmarkCpc':round(bench,2),'excess':round(excess),'pctOfTotal':round(c/max(total_cost,1)*100,1)})

    cost_by_channel.sort(key=lambda x: x['annualCost'], reverse=True)

    # v12-#13: Friction-based wasted spend — red queues have avoidable cost from
    # high AHT, high escalation, low FCR that drives repeat contacts and excess handling
    wasted_friction = 0
    friction_details = []
    qs_map = {qs['queue']: qs for qs in queue_scores}
    for q in queues:
        qs = qs_map.get(q['queue'])
        if not qs: continue
        cpc = q.get('cpc') or q.get('costPerContact') or blended_cpc
        annual_vol = q['volume'] * 12

        # AHT waste: excess handle time above benchmark → avoidable agent minutes
        aht_data = qs['metrics'].get('aht', {})
        if aht_data.get('rating') in ('red', 'amber') and aht_data.get('benchmark', 0) > 0:
            aht_excess_ratio = max(0, (aht_data['value'] - aht_data['benchmark']) / aht_data['value']) if aht_data['value'] > 0 else 0
            aht_waste = cpc * annual_vol * aht_excess_ratio * 0.5  # 50% of excess is addressable
            wasted_friction += aht_waste

        # Escalation waste: escalated contacts cost 2-3x normal contacts
        esc_data = qs['metrics'].get('escalation', {})
        if esc_data.get('rating') in ('red', 'amber') and esc_data.get('benchmark', 0) > 0:
            excess_esc_rate = max(0, esc_data['value'] - esc_data['benchmark'])
            escalated_contacts = annual_vol * excess_esc_rate
            esc_waste = escalated_contacts * cpc * 1.5  # Escalated contacts cost ~2.5x, so excess = 1.5x
            wasted_friction += esc_waste
            if escalated_contacts > 0:
                friction_details.append({
                    'queue': q['queue'], 'channel': q['channel'],
                    'type': 'escalation', 'annualContacts': round(escalated_contacts),
                    'wastedCost': round(esc_waste),
                })

        # FCR waste: repeat contacts from low FCR
        fcr_data = qs['metrics'].get('fcr', {})
        if fcr_data.get('rating') in ('red', 'amber') and fcr_data.get('benchmark', 0) > 0:
            fcr_gap = max(0, fcr_data['benchmark'] - fcr_data['value'])
            repeat_contacts = annual_vol * fcr_gap  # Each FCR point gap = that % of contacts repeated
            fcr_waste = repeat_contacts * cpc
            wasted_friction += fcr_waste

    total_wasted = wasted_cpc_excess + wasted_friction

    # Complexity cost tiers
    complexity_tiers = {'simple':[],'moderate':[],'complex':[]}
    for q in queues:
        cx = q.get('complexity', 0.5)
        tier = 'simple' if cx < 0.35 else 'complex' if cx > 0.55 else 'moderate'
        cpc = q.get('cpc') or q.get('costPerContact') or blended_cpc
        complexity_tiers[tier].append({'queue':q['queue'],'volume':q['volume'],'cpc':cpc})

    tier_summary = {}
    for tier, items in complexity_tiers.items():
        tv = sum(i['volume'] for i in items)
        tc = sum(i['volume']*i['cpc']*12 for i in items)
        tier_summary[tier] = {'queues':len(items),'volume':tv,'annualCost':round(tc),'avgCpc':round(tc/(tv*12),2) if tv>0 else 0}

    return {
        'totalAnnualCost': round(total_cost),
        'blendedCpc': round(blended_cpc, 2),
        'byChannel': cost_by_channel,
        'wastedSpend': round(total_wasted),
        'wastedCpcExcess': round(wasted_cpc_excess),
        'wastedFriction': round(wasted_friction),
        'frictionDetails': friction_details,
        'wastedPct': round(total_wasted/max(total_cost,1)*100, 1),
        'byComplexity': tier_summary,
    }


def _build_channel_summary(queues, queue_scores, benchmarks, params):
    channels = {}
    for qs in queue_scores:
        ch = qs['channel']
        if ch not in channels:
            channels[ch] = {'queues':0,'volume':0,'scores':[],'ratings':{'green':0,'amber':0,'red':0}}
        channels[ch]['queues'] += 1
        channels[ch]['volume'] += qs['volume']
        channels[ch]['scores'].append(qs['overallScore'])
        channels[ch]['ratings'][qs['rating']] += 1

    result = []
    for ch, cd in channels.items():
        avg = sum(cd['scores'])/len(cd['scores']) if cd['scores'] else 0
        result.append({'channel':ch,'queueCount':cd['queues'],'totalVolume':cd['volume'],
                       'avgScore':round(avg,1),'rating':'green' if avg>=70 else 'amber' if avg>=40 else 'red',
                       'ratings':cd['ratings']})
    result.sort(key=lambda x: x['totalVolume'], reverse=True)
    return result


def _build_mismatch_summary(queues, queue_scores, params):
    """CR-016: Channel-specific mismatch rationales."""
    mismatches = []
    channel_rationale = {
        'Voice': {'high_simple':'High-volume simple queries on Voice should migrate to digital/self-service channels for cost efficiency.',
                  'low_complex':'Complex low-volume Voice queues may benefit from specialist routing or video support.',
                  'high_cost':'Voice channel cost exceeds benchmark — consider IVR containment and chatbot deflection.'},
        'Chat': {'high_simple':'Simple Chat queries are candidates for full bot automation.',
                 'low_complex':'Complex Chat interactions may need escalation to Voice/Video for better resolution.',
                 'high_cost':'Chat costs above benchmark suggest bot containment rate needs improvement.'},
        'Email': {'high_simple':'Simple Email queries should be auto-responded or deflected to self-service FAQ.',
                  'low_complex':'Complex Email threads indicate need for RPA or document AI processing.',
                  'high_cost':'Email processing costs indicate manual handling — automate with AI triage.'},
        'IVR': {'high_simple':'IVR handling simple queries effectively — ensure containment rate stays high.',
                'low_complex':'Complex queries reaching IVR should be fast-tracked to agents.',
                'high_cost':'IVR cost above benchmark suggests infrastructure modernisation needed.'},
        'App/Self-Service': {'high_simple':'Self-service adoption good for simple queries — expand coverage.',
                             'low_complex':'Complex queries on self-service need guided resolution flows.',
                             'high_cost':'Self-service cost should be lowest — review platform efficiency.'},
    }

    for qs in queue_scores:
        q = next((q for q in queues if q['queue']==qs['queue']), None)
        if not q: continue
        ch = qs['channel']; cx = q.get('complexity',0.5); vol = q['volume']
        is_simple = cx < 0.35; is_complex = cx > 0.55
        is_high_vol = vol > sum(q2['volume'] for q2 in queues)/max(len(queues),1)
        cpc_data = qs['metrics'].get('cpc',{})
        is_high_cost = cpc_data.get('rating') == 'red'

        reasons = []
        cr = channel_rationale.get(ch, {})
        if is_simple and is_high_vol and ch in ('Voice','Email'):
            reasons.append(cr.get('high_simple','Consider migrating simple high-volume queries to lower-cost channels.'))
        if is_complex and not is_high_vol:
            reasons.append(cr.get('low_complex','Low-volume complex queries may need specialist handling.'))
        if is_high_cost:
            reasons.append(cr.get('high_cost','Channel cost exceeds benchmark — review efficiency.'))

        if reasons:
            mismatches.append({'queue':qs['queue'],'channel':ch,'volume':vol,
                               'complexity':'simple' if is_simple else 'complex' if is_complex else 'moderate',
                               'score':qs['overallScore'],'reasons':reasons})

    mismatches.sort(key=lambda x: len(x['reasons']), reverse=True)
    return mismatches


# ── P2-9: Sub-Intent Decomposition ──

SUB_INTENT_MAP = {
    'Billing & Payments': [
        {'name': 'Payment Processing', 'volumeShare': 0.35, 'complexity': 'simple', 'automationPotential': 0.85},
        {'name': 'Invoice Disputes', 'volumeShare': 0.20, 'complexity': 'complex', 'automationPotential': 0.30},
        {'name': 'Billing Inquiries', 'volumeShare': 0.25, 'complexity': 'simple', 'automationPotential': 0.75},
        {'name': 'Payment Plan Setup', 'volumeShare': 0.12, 'complexity': 'moderate', 'automationPotential': 0.60},
        {'name': 'Refund Requests', 'volumeShare': 0.08, 'complexity': 'moderate', 'automationPotential': 0.55},
    ],
    'Account Management': [
        {'name': 'Password/Access Reset', 'volumeShare': 0.30, 'complexity': 'simple', 'automationPotential': 0.95},
        {'name': 'Profile Updates', 'volumeShare': 0.25, 'complexity': 'simple', 'automationPotential': 0.90},
        {'name': 'Account Closure', 'volumeShare': 0.15, 'complexity': 'moderate', 'automationPotential': 0.45},
        {'name': 'Account Upgrades', 'volumeShare': 0.20, 'complexity': 'moderate', 'automationPotential': 0.50},
        {'name': 'Loyalty/Rewards', 'volumeShare': 0.10, 'complexity': 'simple', 'automationPotential': 0.70},
    ],
    'Technical Support': [
        {'name': 'Connectivity Issues', 'volumeShare': 0.30, 'complexity': 'moderate', 'automationPotential': 0.55},
        {'name': 'Device Setup', 'volumeShare': 0.20, 'complexity': 'moderate', 'automationPotential': 0.60},
        {'name': 'Software Troubleshooting', 'volumeShare': 0.25, 'complexity': 'complex', 'automationPotential': 0.35},
        {'name': 'Outage Reports', 'volumeShare': 0.15, 'complexity': 'simple', 'automationPotential': 0.80},
        {'name': 'Advanced Diagnostics', 'volumeShare': 0.10, 'complexity': 'complex', 'automationPotential': 0.20},
    ],
    'Sales & Retention': [
        {'name': 'New Product Inquiry', 'volumeShare': 0.30, 'complexity': 'moderate', 'automationPotential': 0.40},
        {'name': 'Upgrade/Cross-sell', 'volumeShare': 0.25, 'complexity': 'moderate', 'automationPotential': 0.35},
        {'name': 'Cancel/Churn Save', 'volumeShare': 0.20, 'complexity': 'complex', 'automationPotential': 0.15},
        {'name': 'Pricing Inquiry', 'volumeShare': 0.15, 'complexity': 'simple', 'automationPotential': 0.70},
        {'name': 'Contract Renewal', 'volumeShare': 0.10, 'complexity': 'moderate', 'automationPotential': 0.50},
    ],
    'Order Management': [
        {'name': 'Order Status Tracking', 'volumeShare': 0.35, 'complexity': 'simple', 'automationPotential': 0.90},
        {'name': 'Order Modifications', 'volumeShare': 0.25, 'complexity': 'moderate', 'automationPotential': 0.55},
        {'name': 'Returns/Exchanges', 'volumeShare': 0.25, 'complexity': 'moderate', 'automationPotential': 0.50},
        {'name': 'Delivery Complaints', 'volumeShare': 0.15, 'complexity': 'complex', 'automationPotential': 0.30},
    ],
    'Complaints & Escalations': [
        {'name': 'Service Complaints', 'volumeShare': 0.35, 'complexity': 'complex', 'automationPotential': 0.15},
        {'name': 'Product Complaints', 'volumeShare': 0.25, 'complexity': 'complex', 'automationPotential': 0.20},
        {'name': 'Escalation Requests', 'volumeShare': 0.20, 'complexity': 'complex', 'automationPotential': 0.10},
        {'name': 'Regulatory Complaints', 'volumeShare': 0.10, 'complexity': 'complex', 'automationPotential': 0.05},
        {'name': 'Feedback/Suggestions', 'volumeShare': 0.10, 'complexity': 'simple', 'automationPotential': 0.65},
    ],
}

# Default decomposition for intents not in the map
DEFAULT_SUB_INTENTS = [
    {'name': 'Simple Inquiries', 'volumeShare': 0.40, 'complexity': 'simple', 'automationPotential': 0.75},
    {'name': 'Standard Processing', 'volumeShare': 0.30, 'complexity': 'moderate', 'automationPotential': 0.50},
    {'name': 'Complex Cases', 'volumeShare': 0.20, 'complexity': 'complex', 'automationPotential': 0.25},
    {'name': 'Exceptions/Escalations', 'volumeShare': 0.10, 'complexity': 'complex', 'automationPotential': 0.10},
]


def build_sub_intent_analysis(queues):
    """
    Decompose each intent into sub-intents with volume, complexity, and automation potential.
    Returns list of intent objects with sub-intent breakdown.
    """
    # Aggregate volume by intent
    intent_volume = {}
    intent_aht = {}
    intent_channels = {}
    for q in queues:
        intent = q.get('intent', 'Unknown')
        vol = q.get('volume', 0)
        intent_volume[intent] = intent_volume.get(intent, 0) + vol
        if intent not in intent_aht:
            intent_aht[intent] = []
        intent_aht[intent].append((q.get('aht', 5.0) * 60, vol))  # CR-FIX-AHT: convert min→sec
        if intent not in intent_channels:
            intent_channels[intent] = set()
        intent_channels[intent].add(q.get('channel', 'Voice'))

    results = []
    for intent, total_vol in sorted(intent_volume.items(), key=lambda x: -x[1]):
        # Weighted AHT
        aht_pairs = intent_aht.get(intent, [(300, 1)])  # default 300 sec = 5 min
        wavg_aht = sum(a * v for a, v in aht_pairs) / max(sum(v for _, v in aht_pairs), 1)

        # Sub-intents
        sub_template = SUB_INTENT_MAP.get(intent, DEFAULT_SUB_INTENTS)
        sub_intents = []
        for si in sub_template:
            si_vol = int(total_vol * si['volumeShare'])
            aht_mult = {'simple': 0.7, 'moderate': 1.0, 'complex': 1.5}.get(si['complexity'], 1.0)
            sub_intents.append({
                'name': si['name'],
                'volume': si_vol,
                'volumeShare': si['volumeShare'],
                'complexity': si['complexity'],
                'automationPotential': si['automationPotential'],
                'estimatedAHT': round(wavg_aht * aht_mult),
                'deflectableVolume': int(si_vol * si['automationPotential']),
            })

        # Intent-level automation potential (weighted by sub-intent volume)
        overall_auto = sum(si['automationPotential'] * si['volumeShare'] for si in sub_template)

        results.append({
            'intent': intent,
            'totalVolume': total_vol,
            'avgAHT': round(wavg_aht),
            'channels': sorted(intent_channels.get(intent, set())),
            'automationPotential': round(overall_auto, 2),
            'subIntents': sub_intents,
            'totalDeflectable': sum(si['deflectableVolume'] for si in sub_intents),
        })

    return results
