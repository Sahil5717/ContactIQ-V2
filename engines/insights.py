"""
ContactIQ — Insight & Hypothesis Engine
CR-FIX-O/Q: Converts diagnostics into consulting-style observations
and maps pain patterns to diagnostic hypotheses.
Does NOT auto-solution — generates structured thinking for consultants.
"""

# ═══════════════════════════════════════════════════════════════
# HYPOTHESIS LIBRARY — maps pain patterns to diagnostic hypotheses
# ═══════════════════════════════════════════════════════════════
HYPOTHESIS_PATTERNS = [
    {
        'pattern': 'high_aht_high_repeat',
        'conditions': lambda d: d.get('aht_red_pct', 0) > 0.5 and d.get('repeat_elevated', False),
        'headline': 'Structural resolution failure',
        'hypotheses': [
            'Fragmented knowledge base — agents searching multiple systems',
            'Poor intent routing — wrong skill group handling complex queries',
            'Policy ambiguity — agents improvising different resolutions',
            'Training inconsistency across teams/shifts',
        ],
        'recommended_checks': [
            'Compare AHT variance across agents for same intent',
            'Audit knowledge base hit rates by intent',
            'Check FCR by agent tenure — new vs experienced gap',
        ],
    },
    {
        'pattern': 'high_escalation_low_fcr',
        'conditions': lambda d: d.get('avg_escalation', 0) > 0.12 and d.get('avg_fcr', 1) < 0.70,
        'headline': 'L1 empowerment gap',
        'hypotheses': [
            'L1 agents lack authority to resolve common issues',
            'Missing decision trees for standard scenarios',
            'Overly restrictive escalation policies',
            'Skill mismatch — complex work routed to generalists',
        ],
        'recommended_checks': [
            'Map escalation reasons to resolution actions taken at L2',
            'Identify top 5 escalation intents — are they truly complex?',
            'Compare L1 authority matrix against actual resolution patterns',
        ],
    },
    {
        'pattern': 'high_cost_low_digital',
        'conditions': lambda d: d.get('voice_pct', 0) > 0.70 and d.get('avg_cpc', 0) > 7.0,
        'headline': 'Channel cost concentration on voice',
        'hypotheses': [
            'Self-service options insufficient or poorly promoted',
            'Customer demographics skew toward voice preference',
            'Digital channels exist but have poor containment rates',
            'IVR design driving customers to agent queues',
        ],
        'recommended_checks': [
            'Audit IVR completion vs abandon by menu branch',
            'Compare digital CSAT vs voice CSAT — channel avoidance signal?',
            'Map contact reasons to digital suitability scores',
        ],
    },
    {
        'pattern': 'high_shrinkage',
        'conditions': lambda d: d.get('shrinkage', 0) > 0.32,
        'headline': 'Workforce utilization gap',
        'hypotheses': [
            'High unplanned absence (sick leave, attrition)',
            'Excessive training/meeting overhead vs productive time',
            'Schedule adherence issues — agents not logging in on time',
            'Overstaffing during low-demand periods',
        ],
        'recommended_checks': [
            'Review WFM shrinkage decomposition (training vs absence vs breaks)',
            'Compare actual vs scheduled hours by day-of-week',
            'Assess attrition rate by team/location/tenure',
        ],
    },
    {
        'pattern': 'csat_below_benchmark',
        'conditions': lambda d: d.get('csat_gap', 0) > 0.3,
        'headline': 'Customer experience below industry standard',
        'hypotheses': [
            'Long wait times creating frustration before agent interaction',
            'Agent soft skills or empathy gaps',
            'Resolution quality acceptable but experience is poor',
            'Post-interaction survey fatigue — only dissatisfied customers responding',
        ],
        'recommended_checks': [
            'Correlate CSAT with wait time by queue',
            'Compare CSAT by agent — identify capability vs systemic issue',
            'Review survey response rate — selection bias check',
        ],
    },
    {
        'pattern': 'volume_fte_mismatch',
        'conditions': lambda d: d.get('vol_scale_factor', 1) > 3.0,
        'headline': 'Source volume significantly lower than FTE capacity implies',
        'hypotheses': [
            'CCaaS data covers only a subset of total contacts (e.g., one region/channel)',
            'Significant offline/manual work not captured in interaction records',
            'Seasonal data — sample period may not be representative',
            'FTE count includes non-contact-handling staff',
        ],
        'recommended_checks': [
            'Verify CCaaS data coverage period and scope',
            'Cross-reference FTE roster with contact-handling roles only',
            'Check for WFM data covering same period as CCaaS extract',
        ],
    },
]


def run_insights(data, diagnostic, waterfall):
    """Generate consulting-grade insights from diagnostics and waterfall."""
    insights = []
    
    # Build context dict for hypothesis matching
    queue_scores = diagnostic.get('queueScores', [])
    total_queues = max(len(queue_scores), 1)
    aht_reds = sum(1 for q in queue_scores if q.get('metrics', {}).get('aht', {}).get('rating') == 'red')
    
    queues = data.get('queues', [])
    total_vol = sum(q.get('volume', 0) for q in queues)
    voice_vol = sum(q.get('volume', 0) for q in queues if q.get('channel') == 'Voice')
    
    context = {
        'aht_red_pct': aht_reds / total_queues,
        'repeat_elevated': data.get('avgRepeat', data.get('avgFCR', 0.75)) > 0.10 if 'avgRepeat' in data else (1 - data.get('avgFCR', 0.75)) > 0.25,
        'avg_escalation': sum(q.get('escalation', 0) * q.get('volume', 0) for q in queues) / max(total_vol, 1),
        'avg_fcr': data.get('avgFCR', 0.75),
        'voice_pct': voice_vol / max(total_vol, 1),
        'avg_cpc': sum(q.get('cpc', 0) * q.get('volume', 0) for q in queues) / max(total_vol, 1),
        'shrinkage': data.get('params', {}).get('shrinkage', 0.30),
        'csat_gap': max(0, 4.0 - data.get('avgCSAT', 3.5)),
        'vol_scale_factor': data.get('volumeScaling', {}).get('factor', 1.0),
    }
    
    # Match hypotheses
    for hp in HYPOTHESIS_PATTERNS:
        try:
            if hp['conditions'](context):
                insights.append({
                    'pattern': hp['pattern'],
                    'headline': hp['headline'],
                    'why_it_matters': _build_why(hp['pattern'], context),
                    'hypotheses': hp['hypotheses'],
                    'recommended_checks': hp['recommended_checks'],
                    'confidence': _assess_confidence(hp['pattern'], context, data),
                    'evidence': _gather_evidence(hp['pattern'], context, queue_scores),
                })
        except Exception:
            continue
    
    # Sort by confidence descending
    insights.sort(key=lambda x: {'high': 0, 'medium': 1, 'low': 2}.get(x.get('confidence', 'low'), 3))
    
    return {
        'insights': insights,
        'totalPatterns': len(insights),
        'highConfidence': sum(1 for i in insights if i['confidence'] == 'high'),
    }


def _build_why(pattern, ctx):
    """Generate business impact explanation for each pattern."""
    whys = {
        'high_aht_high_repeat': f'{ctx["aht_red_pct"]:.0%} of queues have critical AHT, and repeat contacts compound the volume — each unresolved contact generates 1+ additional contacts.',
        'high_escalation_low_fcr': f'Escalation rate at {ctx["avg_escalation"]:.1%} with FCR at {ctx["avg_fcr"]:.1%} means agents are passing work upstream rather than resolving it — L2/L3 capacity is being consumed by L1-resolvable issues.',
        'high_cost_low_digital': f'Voice handles {ctx["voice_pct"]:.0%} of volume at ${ctx["avg_cpc"]:.2f}/contact — digital channels could reduce CPC by 40-60% for suitable intents.',
        'high_shrinkage': f'Shrinkage at {ctx["shrinkage"]:.0%} means {ctx["shrinkage"]:.0%} of paid agent time is non-productive — the gap above industry benchmark (25%) represents avoidable cost.',
        'csat_below_benchmark': f'CSAT gap of {ctx["csat_gap"]:.2f} points below benchmark correlates with higher churn risk and negative NPS spillover.',
        'volume_fte_mismatch': f'CCaaS data shows {ctx["vol_scale_factor"]:.1f}x less volume than FTE capacity implies — model outputs depend heavily on which volume basis is used.',
    }
    return whys.get(pattern, 'Impact assessment requires consultant review.')


def _assess_confidence(pattern, ctx, data):
    """Assess confidence in the insight based on data quality."""
    ms = data.get('metricSources', {})
    actual_count = sum(1 for v in ms.values() if v.get('confidence') in ('actual', 'actual_transformed'))
    if actual_count >= 7:
        return 'high'
    elif actual_count >= 4:
        return 'medium'
    return 'low'


def _gather_evidence(pattern, ctx, queue_scores):
    """Gather supporting evidence for the pattern."""
    evidence = []
    if 'aht' in pattern:
        reds = [q['queue'] for q in queue_scores if q.get('metrics', {}).get('aht', {}).get('rating') == 'red']
        evidence.append(f'{len(reds)} queues with red AHT rating')
    if 'escalation' in pattern:
        evidence.append(f'Average escalation: {ctx["avg_escalation"]:.1%}')
    if 'cost' in pattern:
        evidence.append(f'Voice channel share: {ctx["voice_pct"]:.0%}')
    if 'shrinkage' in pattern:
        evidence.append(f'Current shrinkage: {ctx["shrinkage"]:.0%}')
    if 'csat' in pattern:
        evidence.append(f'CSAT gap: {ctx["csat_gap"]:.2f} points below benchmark')
    return evidence
