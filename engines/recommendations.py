"""
ContactIQ — Recommendation Engine v12
v12-#12: Context-aware recommendations — each page gets distinct initiative ranking
         based on its diagnostic lens, not generic top-N.
v12-#23: Diagnostic-Roadmap alignment — initiative linkage tables pull from
         roadmap-enabled initiatives, ensuring consistency.
v12-#7:  Heatmap recommendations factor in specific intent + channel context.
v12-#11: Cost recommendations rank by cost impact.
"""

SIGNAL_TO_LEVER = {
    'high_aht': ['aht_reduction', 'automation', 'process_improvement'],
    'low_fcr': ['fcr_improvement', 'knowledge_mgmt', 'agent_assist'],
    'low_csat': ['csat_improvement', 'quality', 'agent_assist'],
    'high_cost': ['cost_reduction', 'automation', 'self_service'],
    'high_repeat': ['fcr_improvement', 'process_improvement', 'knowledge_mgmt'],
    'high_escalation': ['aht_reduction', 'agent_assist', 'training'],
    'low_self_service': ['self_service', 'automation', 'channel_shift'],
    'high_abandon': ['automation', 'workforce_optimization', 'channel_shift'],
    'maturity_gap': ['process_improvement', 'automation', 'knowledge_mgmt'],
    'channel_mismatch': ['channel_shift', 'self_service', 'automation'],
    'friction_hotspot': ['process_improvement', 'aht_reduction', 'automation'],
    'cost_outlier': ['cost_reduction', 'automation', 'self_service'],
    'benchmark_gap': ['aht_reduction', 'fcr_improvement', 'automation'],
    'workforce_imbalance': ['workforce_optimization', 'cost_reduction', 'training'],
}

PAGE_SIGNALS = {
    'executive_summary': ['high_cost', 'low_csat', 'high_aht', 'low_fcr'],
    'benchmarking': ['benchmark_gap', 'high_aht', 'low_fcr', 'low_csat', 'high_cost'],
    'maturity': ['maturity_gap', 'low_self_service', 'channel_mismatch'],
    'heatmap': ['friction_hotspot', 'high_aht', 'low_csat', 'high_escalation'],
    'friction_map': ['friction_hotspot', 'high_repeat', 'high_abandon', 'channel_mismatch'],
    'gap_analysis': ['benchmark_gap', 'low_fcr', 'high_aht', 'low_csat'],
    'cost_analysis': ['cost_outlier', 'high_cost', 'workforce_imbalance'],
    'opportunity_buckets': ['low_self_service', 'high_cost', 'channel_mismatch'],
    'self_service': ['low_self_service', 'automation', 'channel_mismatch'],
    'channel_strategy': ['channel_mismatch', 'low_self_service', 'cost_outlier'],
    'impact_dashboard': ['high_cost', 'low_csat', 'low_fcr', 'high_aht'],
    'opportunity_sizing': ['low_self_service', 'high_cost', 'channel_mismatch', 'high_aht'],
}

# ═══════════════════════════════════════════════════════════════
#  v12-#12: PAGE-SPECIFIC SCORING STRATEGIES
# ═══════════════════════════════════════════════════════════════

def _score_for_benchmarking(init, data, diagnostic):
    score = 0; lever = init.get('lever', '')
    bm = data.get('benchmarks', {}).get('_defaults', {})
    if data.get('avgAHT', 0) > bm.get('AHT', {}).get('_default', 6.0) and lever in ('aht_reduction', 'automation', 'process_improvement'):
        score += 3 + abs(init.get('ahtImpact', 0)) * 10
    if data.get('avgFCR', 0) < bm.get('FCR', {}).get('_default', 0.75) and lever in ('fcr_improvement', 'repeat_reduction', 'knowledge_mgmt'):
        score += 3 + init.get('fcrImpact', 0) * 10
    if data.get('avgCSAT', 0) < bm.get('CSAT', {}).get('_default', 3.8) and init.get('csatImpact', 0) > 0:
        score += 2 + init.get('csatImpact', 0) * 10
    if data.get('avgCPC', 0) > bm.get('CPC', {}).get('_default', 5.0) and lever in ('cost_reduction', 'deflection', 'self_service'):
        score += 2
    return score

def _score_for_cost_analysis(init, data, diagnostic):
    score = 0; lever = init.get('lever', ''); layer = init.get('layer', '')
    if lever in ('cost_reduction', 'deflection', 'self_service'): score += 5
    if lever == 'aht_reduction': score += 3
    if lever == 'shrinkage_reduction': score += 2
    if layer == 'Location Strategy': score += 4
    score += min(3, init.get('_annualSaving', 0) / max(data.get('totalCost', 1), 1) * 100)
    score += min(2, init.get('_fteImpact', 0) / max(data.get('totalFTE', 1), 1) * 50)
    return score

def _score_for_heatmap(init, data, diagnostic):
    score = 0; lever = init.get('lever', ''); init_channels = set(init.get('channels', []))
    queue_scores = diagnostic.get('queueScores', [])
    red_queues = [qs for qs in queue_scores if qs.get('rating') == 'red']
    if not red_queues: red_queues = sorted(queue_scores, key=lambda x: x.get('overallScore', 100))[:5]
    for qs in red_queues:
        ch = qs.get('channel', '')
        if ch in init_channels:
            score += 2
            worst = min(qs.get('metrics', {}).items(), key=lambda x: x[1].get('score', 100), default=('', {'score': 100}))
            m = worst[0]
            if m == 'aht' and lever in ('aht_reduction', 'automation'): score += 3
            elif m == 'escalation' and lever in ('escalation_reduction', 'agent_assist'): score += 3
            elif m == 'fcr' and lever in ('fcr_improvement', 'repeat_reduction'): score += 3
            elif m == 'csat' and init.get('csatImpact', 0) > 0: score += 2
            elif m == 'cpc' and lever in ('cost_reduction', 'deflection'): score += 2
    return score

def _score_for_gap_analysis(init, data, diagnostic):
    score = 0; lever = init.get('lever', '')
    for pa in diagnostic.get('problemAreas', []):
        m = pa.get('metric', '')
        if m == 'aht' and lever in ('aht_reduction', 'automation'): score += 1
        elif m == 'fcr' and lever in ('fcr_improvement', 'repeat_reduction'): score += 1
        elif m == 'escalation' and lever in ('escalation_reduction',): score += 1
        elif m == 'csat' and init.get('csatImpact', 0) > 0: score += 0.5
        elif m == 'cpc' and lever in ('cost_reduction', 'deflection'): score += 0.5
    score += min(2, init.get('_fteImpact', 0) / max(data.get('totalFTE', 1), 1) * 30)
    return score

def _score_for_self_service(init, data, diagnostic):
    score = 0; lever = init.get('lever', '')
    if lever == 'deflection': score += 5
    if lever in ('self_service', 'automation'): score += 4
    if lever == 'channel_shift': score += 3
    if set(init.get('channels', [])) & {'App/Self-Service', 'IVR', 'Chat', 'SMS/WhatsApp'}: score += 2
    if init.get('complexity') == 'simple': score += 2
    score += init.get('impact', 0) * 5
    return score

def _score_for_opportunity_buckets(init, data, diagnostic):
    score = 0; lever = init.get('lever', '')
    if lever in ('deflection', 'aht_reduction', 'cost_reduction'): score += 4
    if lever in ('escalation_reduction', 'repeat_reduction', 'shrinkage_reduction'): score += 3
    score += min(5, init.get('_fteImpact', 0) / max(data.get('totalFTE', 1), 1) * 80)
    score += init.get('impact', 0) * 3
    return score

def _score_for_executive_summary(init, data, diagnostic):
    score = 0
    score += {'AI & Automation': 1, 'Operating Model': 1, 'Location Strategy': 1}.get(init.get('layer', ''), 0)
    score += min(3, init.get('_fteImpact', 0) / max(data.get('totalFTE', 1), 1) * 50)
    score += min(3, init.get('_annualSaving', 0) / max(data.get('totalCost', 1), 1) * 80)
    score += init.get('matchScore', 0) / 30
    return score

def _score_for_maturity(init, data, diagnostic):
    """v14: Maturity page — favour initiatives that close maturity gaps."""
    score = 0; lever = init.get('lever', ''); layer = init.get('layer', '')
    # Process improvement & knowledge management close maturity gaps
    if lever in ('process_improvement', 'knowledge_mgmt', 'automation'): score += 4
    if lever in ('quality', 'training', 'agent_assist'): score += 3
    # AI & Automation initiatives raise tech maturity
    if layer == 'AI & Automation': score += 2
    # Operating Model initiatives raise process maturity
    if layer == 'Operating Model': score += 2
    # Higher-impact initiatives rank better on maturity page
    score += min(2, init.get('_fteImpact', 0) / max(data.get('totalFTE', 1), 1) * 40)
    score += min(2, init.get('_annualSaving', 0) / max(data.get('totalCost', 1), 1) * 80)
    return score

def _score_for_channel_strategy(init, data, diagnostic):
    """v14: Channel strategy page — favour deflection, channel shift, digital enablement."""
    score = 0; lever = init.get('lever', '')
    if lever == 'deflection': score += 5
    if lever in ('channel_shift', 'self_service'): score += 4
    if lever == 'automation': score += 3
    # Bonus for initiatives targeting digital channels
    digital_chs = {'Chat', 'App/Self-Service', 'SMS/WhatsApp', 'IVR', 'Email'}
    init_chs = set(init.get('channels', []))
    if init_chs & digital_chs: score += 2
    # Channel migrations directly improve this page
    score += min(2, init.get('_annualSaving', 0) / max(data.get('totalCost', 1), 1) * 60)
    return score

def _score_for_impact_dashboard(init, data, diagnostic):
    """v14: Impact dashboard — overall business impact, balanced across levers."""
    score = 0
    # Broad layer balance bonus
    score += {'AI & Automation': 1, 'Operating Model': 1, 'Location Strategy': 1}.get(init.get('layer', ''), 0)
    # Weight heavily by FTE and saving (this is the impact page)
    score += min(4, init.get('_fteImpact', 0) / max(data.get('totalFTE', 1), 1) * 60)
    score += min(4, init.get('_annualSaving', 0) / max(data.get('totalCost', 1), 1) * 100)
    # CSAT contributors get a boost
    if init.get('csatImpact', 0) > 0: score += 1
    return score

def _score_for_opportunity_sizing(init, data, diagnostic):
    """v14: Opportunity sizing — same logic as opportunity_buckets."""
    return _score_for_opportunity_buckets(init, data, diagnostic)

PAGE_SCORERS = {
    'executive_summary': _score_for_executive_summary,
    'benchmarking': _score_for_benchmarking,
    'heatmap': _score_for_heatmap,
    'gap_analysis': _score_for_gap_analysis,
    'cost_analysis': _score_for_cost_analysis,
    'self_service': _score_for_self_service,
    'opportunity_buckets': _score_for_opportunity_buckets,
    # v14: Add scorers for pages that previously had None (all fell through to generic)
    'maturity': _score_for_maturity,
    'channel_strategy': _score_for_channel_strategy,
    'impact_dashboard': _score_for_impact_dashboard,
    'opportunity_sizing': _score_for_opportunity_sizing,
}


def _build_initiative_triggers(init, signals, data):
    """v4.5-#5: Generate initiative-specific trigger text from metadata, not generic signals.
    Uses mechanism, savings, lever, channels, and impact data for differentiation."""
    triggers = []
    lever = init.get('lever', '')
    layer = init.get('layer', '')
    saving = init.get('_annualSaving', 0)
    fte = init.get('_fteImpact', 0)
    mechanism = init.get('_mechanism', '')
    pool_consumed = init.get('_poolConsumed', 0)
    channels = init.get('channels', [])
    impact_pct = init.get('impact', 0)

    if lever == 'cost_reduction':
        # Location Strategy: differentiate by mechanism and migration specifics
        if pool_consumed > 0:
            triggers.append(f"{pool_consumed:.0f} FTE migratable")
        arb = data.get('params', {}).get('locationArbitrage', 0.35)
        if arb > 0:
            triggers.append(f"{arb:.0%} cost arbitrage")
        if saving > 0:
            triggers.append(f"${saving/1000:,.0f}K/yr saving")
    elif lever == 'deflection':
        if impact_pct > 0:
            triggers.append(f"{impact_pct:.0%} deflection rate")
        if channels:
            triggers.append(f"via {channels[0]}" if len(channels) == 1 else f"{len(channels)} channels")
        if fte > 0:
            triggers.append(f"−{fte:.0f} FTE")
    elif lever == 'aht_reduction':
        aht_impact = init.get('ahtImpact', impact_pct)
        if aht_impact > 0:
            triggers.append(f"−{aht_impact:.0%} AHT")
        if fte > 0:
            triggers.append(f"−{fte:.0f} FTE")
    elif lever in ('repeat_reduction', 'fcr_improvement'):
        fcr_impact = init.get('fcrImpact', impact_pct)
        if fcr_impact > 0:
            triggers.append(f"+{fcr_impact:.0%} FCR")
        if fte > 0:
            triggers.append(f"−{fte:.0f} FTE")
    elif lever in ('escalation_reduction', 'shrinkage_reduction'):
        if impact_pct > 0:
            triggers.append(f"−{impact_pct:.0%} {lever.replace('_', ' ')}")
        if fte > 0:
            triggers.append(f"−{fte:.0f} FTE")
    else:
        # Generic: use first relevant signal description
        for sig in signals:
            if lever in SIGNAL_TO_LEVER.get(sig['type'], []):
                short = sig.get('description', '').split('(')[0].strip().split('—')[0].strip()
                if short and short not in triggers:
                    triggers.append(short)
                    break
        if fte > 0:
            triggers.append(f"−{fte:.0f} FTE")

    if not triggers:
        triggers = [init.get('description', layer) or layer]

    return triggers[:3]


def get_recommendations(page_context, data, diagnostic, initiatives, waterfall=None, max_recs=5, maturity=None):
    """v12-#12: Context-aware recommendations. v12-#23: Only surfaces enabled (roadmap-aligned) initiatives. v12-#43: Maturity-aware."""
    signals = _detect_signals(page_context, data, diagnostic, waterfall, maturity)
    enabled = [i for i in initiatives if i.get('enabled')]
    scorer = PAGE_SCORERS.get(page_context)
    scored = []
    for init in enabled:
        if scorer:
            relevance = scorer(init, data, diagnostic)
        else:
            lever = init.get('lever', ''); relevant_levers = set()
            for sig in signals: relevant_levers.update(SIGNAL_TO_LEVER.get(sig['type'], []))
            relevance = 0
            if lever in relevant_levers: relevance += 3
            relevance += min(2, init.get('_fteImpact', 0) / max(data.get('totalFTE', 1), 1) * 20)
            relevance += init.get('matchScore', 0) / 100
        if relevance > 0: scored.append((relevance, init))
    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:max_recs]
    recommendations = []
    for rel, init in top:
        saving = init.get('_annualSaving', 0)
        # v4.5-#5: Initiative-specific trigger details (not generic signal matching)
        lever = init.get('lever', '')
        trigger_details = _build_initiative_triggers(init, signals, data)
        # v12-#44: Format savings_display
        if abs(saving) >= 1_000_000:
            savings_display = f"${saving/1_000_000:,.1f}M/yr"
        elif abs(saving) >= 1_000:
            savings_display = f"${saving/1_000:,.0f}K/yr"
        else:
            savings_display = f"${saving:,.0f}/yr"
        recommendations.append({
            'id': init['id'], 'name': init['name'], 'layer': init.get('layer', ''),
            'lever': init.get('lever', ''), 'rationale': _build_rationale(init, signals),
            'fte_impact': round(init.get('_fteImpact', 0), 1),
            'annual_saving': round(saving),
            'trigger_details': trigger_details[:3],   # v12-#44
            'savings_display': savings_display,        # v12-#44
            'priority': 'high' if rel > 4 else 'medium' if rel > 2 else 'low',
            'relevance_score': round(rel, 2),
        })
    headline = _build_headline(page_context, signals, data, waterfall)
    key_findings = [{'signal': s['type'], 'description': s['description'],
                     'metric': s.get('metric', ''), 'value': s.get('value', ''),
                     'severity': s.get('severity', 'medium')} for s in signals[:5]]
    opportunity = _size_opportunity(page_context, signals, data, waterfall, recommendations)
    return {'headline': headline, 'key_findings': key_findings, 'recommendations': recommendations,
            'opportunity_size': opportunity, 'signal_count': len(signals), 'page_context': page_context}


def get_initiative_linkage(page_context, data, diagnostic, initiatives, waterfall=None):
    """v12-#23: Initiative linkage table. v12-#2: All below-benchmark metrics. v12-#15: All above-benchmark costs."""
    enabled = [i for i in initiatives if i.get('enabled')]
    benchmarks = data.get('benchmarks', {}); bm_defaults = benchmarks.get('_defaults', {})
    queues = data.get('queues', []); linkages = []

    if page_context == 'benchmarking':
        metrics = [
            ('AHT', data.get('avgAHT', 0), bm_defaults.get('AHT', {}).get('_default', 6.0), 'lower', 'min'),
            ('FCR', data.get('avgFCR', 0), bm_defaults.get('FCR', {}).get('_default', 0.75), 'higher', '%'),
            ('CSAT', data.get('avgCSAT', 0), bm_defaults.get('CSAT', {}).get('_default', 3.8), 'higher', 'score'),
            ('CPC', data.get('avgCPC', 0), bm_defaults.get('CPC', {}).get('_default', 5.0), 'lower', '$'),
        ]
        avg_esc = sum(q.get('escalation', 0) * q.get('volume', 0) for q in queues) / max(sum(q.get('volume', 0) for q in queues), 1)
        metrics.append(('Escalation', avg_esc, 0.12, 'lower', '%'))
        lever_map = {'AHT': ['aht_reduction', 'automation', 'process_improvement'],
                     'FCR': ['fcr_improvement', 'repeat_reduction', 'knowledge_mgmt'],
                     'CSAT': ['csat_improvement', 'quality', 'agent_assist'],
                     'CPC': ['cost_reduction', 'deflection', 'self_service'],
                     'Escalation': ['escalation_reduction', 'agent_assist']}
        for name, current, benchmark, direction, fmt in metrics:
            is_below = (direction == 'higher' and current < benchmark) or (direction == 'lower' and current > benchmark)
            if is_below:
                matched = sorted([i for i in enabled if i.get('lever', '') in lever_map.get(name, [])],
                                key=lambda x: x.get('_fteImpact', 0), reverse=True)
                linkages.append({'metric': name, 'current': current, 'benchmark': benchmark,
                                 'gap': round(abs(current - benchmark), 3), 'direction': direction, 'format': fmt,
                                 'initiatives': [{'id': m['id'], 'name': m['name'], 'layer': m.get('layer', ''),
                                                  'impact': round(m.get('_fteImpact', 0), 1)} for m in matched[:5]]})

    elif page_context == 'cost_analysis':
        for q in queues:
            cpc = q.get('cpc', 0); ch = q.get('channel', 'Voice')
            bench_cpc = bm_defaults.get('CPC', {}).get(ch, bm_defaults.get('CPC', {}).get('_default', 5.0))
            if cpc > bench_cpc * 1.1:
                matched = sorted([i for i in enabled if i.get('lever', '') in ('cost_reduction', 'deflection', 'aht_reduction', 'self_service')
                                  and (ch in i.get('channels', []) or not i.get('channels'))],
                                key=lambda x: x.get('_annualSaving', 0), reverse=True)
                linkages.append({'intent': q.get('intent', 'Unknown'), 'channel': ch, 'queue': q.get('queue', ''),
                                 'volume': q.get('volume', 0), 'currentCpc': round(cpc, 2), 'benchmarkCpc': round(bench_cpc, 2),
                                 'excessPct': round((cpc / max(bench_cpc, 0.01) - 1) * 100, 1),
                                 'initiatives': [{'id': m['id'], 'name': m['name'], 'layer': m.get('layer', ''),
                                                  'saving': round(m.get('_annualSaving', 0))} for m in matched[:3]]})
        linkages.sort(key=lambda x: x.get('excessPct', 0), reverse=True)

    elif page_context in ('heatmap', 'gap_analysis'):
        queue_scores = diagnostic.get('queueScores', [])
        problem_queues = sorted([qs for qs in queue_scores if qs.get('rating') in ('red', 'amber')],
                               key=lambda x: x.get('overallScore', 100))
        lever_map = {'aht': ['aht_reduction', 'automation'], 'fcr': ['fcr_improvement', 'repeat_reduction'],
                     'escalation': ['escalation_reduction', 'agent_assist'], 'csat': ['quality', 'agent_assist'],
                     'cpc': ['cost_reduction', 'deflection']}
        for qs in problem_queues[:10]:
            ch = qs.get('channel', '')
            worst = min(qs.get('metrics', {}).items(), key=lambda x: x[1].get('score', 100), default=('', {}))
            mn, md = worst
            matched = sorted([i for i in enabled if i.get('lever', '') in lever_map.get(mn, [])
                             and (ch in i.get('channels', []) or not i.get('channels'))],
                            key=lambda x: x.get('_fteImpact', 0), reverse=True)
            linkages.append({'queue': qs.get('queue', ''), 'channel': ch, 'intent': qs.get('intent', ''),
                             'worstMetric': mn, 'score': qs.get('overallScore', 0), 'rating': qs.get('rating', ''),
                             'metricValue': md.get('value', 0) if isinstance(md, dict) else 0,
                             'metricBenchmark': md.get('benchmark', 0) if isinstance(md, dict) else 0,
                             'initiatives': [{'id': m['id'], 'name': m['name'], 'layer': m.get('layer', ''),
                                              'impact': round(m.get('_fteImpact', 0), 1)} for m in matched[:3]]})
    return {'page_context': page_context, 'linkages': linkages, 'total_findings': len(linkages)}


def _detect_signals(page_context, data, diagnostic, waterfall, maturity=None):
    signals = []; queues = data.get('queues', []); benchmarks = data.get('benchmarks', {})
    total_vol = max(data.get('totalVolume', 1), 1); bm_defaults = benchmarks.get('_defaults', {})
    avg_aht = data.get('avgAHT', 0); bm_aht = bm_defaults.get('AHT', {}).get('_default', 6.0)
    if avg_aht > bm_aht * 1.15:
        pct = round((avg_aht / bm_aht - 1) * 100)
        signals.append({'type': 'high_aht', 'severity': 'high' if pct > 30 else 'medium',
                        'description': f'Average handle time is {pct}% above benchmark ({avg_aht:.1f} min vs {bm_aht:.1f} min)',
                        'metric': 'AHT', 'value': f'{avg_aht:.1f} min', 'gap_pct': pct})
    avg_fcr = data.get('avgFCR', 0); bm_fcr = bm_defaults.get('FCR', {}).get('_default', 0.75)
    if avg_fcr < bm_fcr * 0.90:
        gap = round((bm_fcr - avg_fcr) * 100)
        signals.append({'type': 'low_fcr', 'severity': 'high' if gap > 10 else 'medium',
                        'description': f'FCR is {gap}pp below benchmark ({avg_fcr:.0%} vs {bm_fcr:.0%})',
                        'metric': 'FCR', 'value': f'{avg_fcr:.0%}', 'gap_pct': gap})
    avg_csat = data.get('avgCSAT', 0); bm_csat = bm_defaults.get('CSAT', {}).get('_default', 3.8)
    if avg_csat < bm_csat * 0.92:
        gap = round(bm_csat - avg_csat, 2)
        signals.append({'type': 'low_csat', 'severity': 'high' if gap > 0.5 else 'medium',
                        'description': f'CSAT {gap:.2f}pts below benchmark ({avg_csat:.2f} vs {bm_csat:.2f})',
                        'metric': 'CSAT', 'value': f'{avg_csat:.2f}', 'gap': gap})
    avg_cpc = data.get('avgCPC', 0); bm_cpc = bm_defaults.get('CPC', {}).get('_default', 5.0)
    if avg_cpc > bm_cpc * 1.20:
        pct = round((avg_cpc / max(bm_cpc, 0.01) - 1) * 100)
        signals.append({'type': 'high_cost', 'severity': 'high' if pct > 40 else 'medium',
                        'description': f'CPC is {pct}% above benchmark (${avg_cpc:.2f} vs ${bm_cpc:.2f})',
                        'metric': 'CPC', 'value': f'${avg_cpc:.2f}', 'gap_pct': pct})
    esc_queues = [q for q in queues if q.get('escalation', 0) > 0.10]
    if esc_queues:
        avg_esc = sum(q['escalation'] * q['volume'] for q in esc_queues) / max(sum(q['volume'] for q in esc_queues), 1)
        signals.append({'type': 'high_escalation', 'severity': 'high' if avg_esc > 0.18 else 'medium',
                        'description': f'{len(esc_queues)} queues with escalation >10% (avg {avg_esc:.0%})',
                        'metric': 'Escalation', 'value': f'{avg_esc:.0%}'})
    ss_vol = sum(q['volume'] for q in queues if q['channel'] in {'App/Self-Service', 'IVR'})
    ss_pct = ss_vol / total_vol
    if ss_pct < 0.25:
        signals.append({'type': 'low_self_service', 'severity': 'high' if ss_pct < 0.15 else 'medium',
                        'description': f'Self-service at {ss_pct:.0%} — significant deflection potential',
                        'metric': 'Self-Service %', 'value': f'{ss_pct:.0%}'})
    mismatches = diagnostic.get('mismatch', [])
    if mismatches:
        signals.append({'type': 'channel_mismatch', 'severity': 'medium',
                        'description': f'{len(mismatches)} intent-channel mismatches detected',
                        'metric': 'Mismatches', 'value': str(len(mismatches))})
    for pa in diagnostic.get('problemAreas', [])[:3]:
        signals.append({'type': 'friction_hotspot', 'severity': 'high' if pa.get('score', 0) < 40 else 'medium',
                        'description': f'{pa.get("queue", "?")}: {pa.get("metric", "")} rated red',
                        'metric': pa.get('metric', ''), 'value': str(pa.get('value', ''))})
    if waterfall and waterfall.get('totalSaving', 0) > 0:
        signals.append({'type': 'cost_outlier', 'severity': 'high',
                        'description': f'${waterfall["totalSaving"]:,.0f} savings across {waterfall.get("totalReduction", 0)} FTE',
                        'metric': 'Total Savings', 'value': f'${waterfall["totalSaving"]:,.0f}'})
    # v12-#43: Maturity gap signals — surface when dimensions score below target
    if maturity:
        mat_overall = maturity.get('overall', 3.0)
        mat_target = maturity.get('target', 4.0)
        if mat_overall < mat_target * 0.75:
            severity = 'high' if mat_overall < 2.5 else 'medium'
            signals.append({'type': 'maturity_gap', 'severity': severity,
                            'description': f'Overall maturity {mat_overall:.1f}/5 vs target {mat_target:.1f} — limits transformation readiness',
                            'metric': 'Maturity', 'value': f'{mat_overall:.1f}/5'})
        for dim_key, dim_data in maturity.get('dimensions', {}).items():
            dim_score = dim_data.get('score', 3.0)
            if dim_score < 2.5:
                signals.append({'type': 'maturity_gap', 'severity': 'high' if dim_score < 2.0 else 'medium',
                                'description': f'{dim_data.get("label", dim_key)} maturity at {dim_score:.1f}/5 — critical gap',
                                'metric': f'{dim_key} Maturity', 'value': f'{dim_score:.1f}/5'})
    page_types = set(PAGE_SIGNALS.get(page_context, []))
    if page_types:
        relevant = [s for s in signals if s['type'] in page_types]
        for s in signals:
            if s['severity'] == 'high' and s not in relevant: relevant.append(s)
        signals = relevant
    signals.sort(key=lambda s: {'high': 0, 'medium': 1, 'low': 2}.get(s['severity'], 2))
    return signals

def _build_rationale(init, signals):
    lever = init.get('lever', ''); addressed = []
    for sig in signals:
        if lever in SIGNAL_TO_LEVER.get(sig['type'], []): addressed.append(sig['description'].split('(')[0].strip())
    return f"Addresses: {'; '.join(addressed[:2])}" if addressed else f"Contributes to {init.get('layer', 'transformation')} improvement"

def _build_headline(page_context, signals, data, waterfall):
    tc = data.get('totalCost', 0); hs = [s for s in signals if s['severity'] == 'high']
    h = {
        'executive_summary': lambda: f"${waterfall.get('totalSaving',0):,.0f} savings opportunity with {waterfall.get('totalReduction',0)} FTE transformation" if waterfall else f"${tc:,.0f} annual cost with {len(hs)} critical areas",
        'benchmarking': lambda: f"{len(signals)} metrics trailing benchmark — closing gaps unlocks FTE & cost savings",
        'maturity': lambda: "Current maturity limits automation & self-service potential",
        'heatmap': lambda: f"{len([s for s in signals if s['type']=='friction_hotspot'])} friction hotspots in high-volume queues",
        'gap_analysis': lambda: f"Performance gaps across {len(signals)} metrics with direct improvement levers",
        'cost_analysis': lambda: f"${tc:,.0f} annual cost — structural optimization opportunities exist",
        'self_service': lambda: f"Self-service at {next((s['value'] for s in signals if s['type']=='low_self_service'), 'low')} — significant deflection potential",
        'impact_dashboard': lambda: f"${waterfall.get('totalNPV',0):,.0f} NPV with {waterfall.get('payback',0):.1f}yr payback" if waterfall else "Complete financial overview",
    }
    try: return h.get(page_context, lambda: f"{len(signals)} improvement areas identified")()
    except: return f"{len(signals)} improvement areas identified"

def _size_opportunity(page_context, signals, data, waterfall, recommendations):
    return {'annual_saving': round(sum(r.get('annual_saving', 0) for r in recommendations)),
            'fte_impact': round(sum(r.get('fte_impact', 0) for r in recommendations), 1),
            'initiative_count': len(recommendations), 'signal_count': len(signals),
            'high_severity_count': len([s for s in signals if s['severity'] == 'high'])}

# ═══════════════════════════════════════════════════════════════
#  INDUSTRY STARTER PACKS
# ═══════════════════════════════════════════════════════════════
INDUSTRY_CONFIGS = {
    'telecommunications': {'label': 'Telecommunications', 'intents': ['Billing & Payments', 'Technical Support', 'Network Outage', 'Plan Change', 'Device Troubleshooting', 'Service Cancellation', 'Roaming', 'Data Usage', 'Contract Renewal', 'Complaints', 'New Connection', 'SIM Replacement', 'Number Portability', 'Value-Added Services', 'General Enquiry'], 'benchmarks': {'AHT': {'Voice': 5.0, 'Chat': 8.0, 'Email': 12.0, '_default': 6.0}, 'FCR': {'Voice': 0.78, 'Chat': 0.80, 'Email': 0.75, '_default': 0.75}, 'CSAT': {'Voice': 4.0, 'Chat': 4.0, 'Email': 3.8, '_default': 3.8}, 'CPC': {'Voice': 8.50, 'Chat': 5.00, 'Email': 4.00, '_default': 5.0}}},
    'industrial_products': {'label': 'Industrial Products', 'intents': ['Product Inquiry', 'Order Status', 'Technical Support', 'Warranty Claims', 'Parts & Spares', 'Installation Support', 'Maintenance Scheduling', 'Billing & Invoicing', 'Returns & Replacements', 'Safety & Compliance', 'Delivery Tracking', 'Account Management', 'Complaints', 'General Enquiry'], 'benchmarks': {'AHT': {'Voice': 7.0, 'Chat': 10.0, 'Email': 15.0, '_default': 8.0}, 'FCR': {'Voice': 0.72, 'Chat': 0.70, 'Email': 0.68, '_default': 0.70}, 'CSAT': {'Voice': 3.8, 'Chat': 3.7, 'Email': 3.5, '_default': 3.6}, 'CPC': {'Voice': 12.00, 'Chat': 7.50, 'Email': 6.00, '_default': 8.0}}},
    'automotive': {'label': 'Automotive', 'intents': ['Vehicle Purchase Inquiry', 'Service Booking', 'Warranty Claims', 'Parts & Accessories', 'Recall Notifications', 'Financing & Leasing', 'Roadside Assistance', 'Vehicle Delivery', 'Insurance', 'Trade-In Valuation', 'Test Drive Booking', 'Complaints', 'Connected Vehicle Support', 'General Enquiry'], 'benchmarks': {'AHT': {'Voice': 6.5, 'Chat': 9.0, 'Email': 14.0, '_default': 7.5}, 'FCR': {'Voice': 0.74, 'Chat': 0.72, 'Email': 0.70, '_default': 0.72}, 'CSAT': {'Voice': 4.0, 'Chat': 3.9, 'Email': 3.6, '_default': 3.8}, 'CPC': {'Voice': 10.00, 'Chat': 6.50, 'Email': 5.00, '_default': 7.0}}},
    'manufacturing': {'label': 'Manufacturing', 'intents': ['Order Management', 'Production Scheduling', 'Quality Issues', 'Supply Chain Inquiry', 'Technical Specifications', 'Logistics & Shipping', 'Billing & Payments', 'Warranty & Returns', 'Equipment Support', 'Compliance & Certifications', 'New Customer Onboarding', 'Account Management', 'Complaints', 'General Enquiry'], 'benchmarks': {'AHT': {'Voice': 8.0, 'Chat': 11.0, 'Email': 16.0, '_default': 9.0}, 'FCR': {'Voice': 0.70, 'Chat': 0.68, 'Email': 0.65, '_default': 0.68}, 'CSAT': {'Voice': 3.7, 'Chat': 3.6, 'Email': 3.4, '_default': 3.5}, 'CPC': {'Voice': 14.00, 'Chat': 8.00, 'Email': 7.00, '_default': 9.5}}},
}

def get_industry_config(industry_key):
    key = industry_key.lower().replace(' ', '_') if industry_key else 'telecommunications'
    if key in INDUSTRY_CONFIGS: return INDUSTRY_CONFIGS[key]
    for k, v in INDUSTRY_CONFIGS.items():
        if key in k or k in key: return v
    return INDUSTRY_CONFIGS.get('telecommunications')

def get_available_industries():
    return [{'key': k, 'label': v['label']} for k, v in INDUSTRY_CONFIGS.items()]
