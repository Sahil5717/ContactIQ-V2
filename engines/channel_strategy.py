"""
ContactIQ — Channel Strategy Engine

Core Logic: Optimal Channel = f(Intent Complexity, Customer Preference, Cost to Serve, Resolution Risk)
Channel strategy is intent-led, not technology-led.

Flow:
  1. Intent Segmentation — classify queues into tiers
  2. Demand & Behaviour Analysis — detect friction patterns
  3. Cost-to-Serve Modelling — relative channel cost index
  4. Migration Readiness Score per channel
  5. Channel Suitability Rules with CX Safeguard gates
  6. Queue-level migration decisions with volumes
  7. Build Channel Intent Matrix, Target Mix, Sankey
"""

CHANNEL_HIERARCHY = {
    'App/Self-Service': {'costTier': 1, 'costIndex': 1.0,  'digitalScore': 1.0, 'automationPotential': 0.90},
    'IVR':              {'costTier': 2, 'costIndex': 2.0,  'digitalScore': 0.8, 'automationPotential': 0.70},
    'SMS/WhatsApp':     {'costTier': 3, 'costIndex': 3.0,  'digitalScore': 0.9, 'automationPotential': 0.60},
    'Chat':             {'costTier': 4, 'costIndex': 5.0,  'digitalScore': 0.7, 'automationPotential': 0.55},
    'Email':            {'costTier': 5, 'costIndex': 4.0,  'digitalScore': 0.6, 'automationPotential': 0.50},
    'Social Media':     {'costTier': 6, 'costIndex': 4.5,  'digitalScore': 0.5, 'automationPotential': 0.40},
    'Voice':            {'costTier': 7, 'costIndex': 9.0,  'digitalScore': 0.3, 'automationPotential': 0.25},
    'Retail/Walk-in':   {'costTier': 8, 'costIndex': 12.0, 'digitalScore': 0.1, 'automationPotential': 0.10},
}

INTENT_TIERS = {
    'transactional': {
        'label': 'Transactional',
        'description': 'Status check, password reset, simple lookups — digital-first',
        'channel_bias': ['App/Self-Service', 'IVR', 'SMS/WhatsApp'],
        'migration_eligible': True,
        'max_migration_pct': 0.50,
    },
    'informational': {
        'label': 'Informational',
        'description': 'Policy queries, FAQs, how-to — chat/self-service suitable',
        'channel_bias': ['Chat', 'App/Self-Service', 'Email'],
        'migration_eligible': True,
        'max_migration_pct': 0.30,
    },
    'assisted': {
        'label': 'Assisted Resolution',
        'description': 'Billing issues, troubleshooting — needs human guidance',
        'channel_bias': ['Chat', 'Voice'],
        'migration_eligible': False,
        'max_migration_pct': 0.10,
    },
    'high_emotion': {
        'label': 'High Emotion / Complex',
        'description': 'Complaints, escalations, sensitive issues — voice/human required',
        'channel_bias': ['Voice', 'Retail/Walk-in'],
        'migration_eligible': False,
        'max_migration_pct': 0.0,
    },
}

DECISION_RATIONALE = {
    'App/Self-Service': {
        'invest': 'Self-service delivers lowest cost-per-contact with highest scalability. Expand transaction coverage and guided resolution flows.',
        'maintain': 'Self-service performing well. Monitor adoption rates for expansion.',
        'optimise': 'Self-service adoption below potential. Improve UX, add guided flows, promote via IVR and agent prompts.',
    },
    'IVR': {
        'invest': 'IVR containment has room to improve. Invest in conversational IVR, visual IVR, intent-based routing.',
        'maintain': 'IVR at benchmark. Maintain configuration and monitor containment.',
        'optimise': 'IVR containment below target. Redesign menu trees, add NLU, implement callbacks.',
        'migrate_from': 'Migrate simple IVR transactions to app/self-service for better CX at lower cost.',
    },
    'Chat': {
        'invest': 'Chat shows strong CX at lower cost than voice. Invest in AI chatbot, proactive chat, co-browse.',
        'maintain': 'Chat at benchmark. Maintain bot/agent ratio and monitor CSAT.',
        'optimise': 'Chat containment and CSAT need improvement. Tune bot flows, improve handoff, add sentiment detection.',
        'migrate_from': 'Deflect simple chat to self-service; escalate complex to voice.',
    },
    'Voice': {
        'invest': 'Voice critical for complex/emotional interactions. Invest in AI agent assist and speech analytics.',
        'maintain': 'Voice at target. Maintain staffing and skill-based routing.',
        'optimise': 'Voice costs above benchmark. Implement IVR deflection, callbacks, AHT reduction.',
        'migrate_from': 'Migrate simple voice queries to digital. Target 15-25% shift to chat/self-service.',
        'protect': 'Voice CSAT significantly above digital — protect from aggressive deflection. Retain for complex intents.',
    },
    'Email': {
        'invest': 'Email handles complex async queries well. Invest in AI triage, auto-response, document processing.',
        'maintain': 'Email SLAs met. Maintain current workflow.',
        'optimise': 'Email response times below target. Implement smart routing and templates.',
        'migrate_from': 'Deflect simple email queries to FAQ/self-service with proactive notifications.',
        'sunset': 'Email volume declining. Consolidate into omnichannel queue.',
    },
    'SMS/WhatsApp': {
        'invest': 'Messaging growing rapidly. Invest in WhatsApp Business, rich messaging, bot integration.',
        'maintain': 'Messaging stable. Maintain automation rate, expand incrementally.',
        'optimise': 'Messaging adoption below target. Promote channel, add bot capabilities.',
    },
    'Social Media': {
        'invest': 'Social is a customer expectation. Invest in monitoring, auto-response, reputation management.',
        'maintain': 'Social response adequate. Maintain monitoring and escalation.',
        'optimise': 'Social response times need improvement. Add automation and dedicated agents.',
        'sunset': 'Social volume too low for dedicated team. Route to general digital queue.',
    },
    'Retail/Walk-in': {
        'invest': 'Retail critical for complex sales/service. Invest in appointments, video kiosks, digital check-in.',
        'maintain': 'Retail at target. Maintain staffing and appointment mix.',
        'optimise': 'Retail costs high. Implement self-service kiosks and appointment model.',
        'migrate_from': 'Deflect simple retail queries to digital with in-store signage.',
        'sunset': 'Retail footprint reduction warranted. Consolidate, shift digital-first.',
    },
}


def classify_intent_tier(queue):
    complexity = queue.get('complexity', 0.5)
    fcr = queue.get('fcr', 0.7)
    escalation = queue.get('escalation', 0.10)
    csat = queue.get('csat', 3.5)
    if complexity > 0.65 or escalation > 0.15 or csat < 3.0:
        return 'high_emotion'
    elif 0.40 <= complexity <= 0.65:
        return 'assisted'
    elif complexity < 0.25 and fcr > 0.75:
        return 'transactional'
    elif complexity < 0.40 and fcr > 0.60:
        return 'informational'
    return 'informational'


def analyse_channel_friction(channels_data, total_volume):
    friction_signals = []
    for ch_name, ch_data in channels_data.items():
        ch_queues = ch_data['queues']
        ch_volume = ch_data['volume']
        if ch_volume == 0:
            continue
        avg_esc = sum(q['volume'] * q.get('escalation', 0) for q in ch_queues) / max(ch_volume, 1)
        avg_fcr = sum(q['volume'] * q.get('fcr', 0.7) for q in ch_queues) / max(ch_volume, 1)
        repeat_rate = 1.0 - avg_fcr
        if avg_esc > 0.12:
            friction_signals.append({
                'from': ch_name, 'to': 'Voice', 'type': 'escalation_friction',
                'volume': int(ch_volume * avg_esc),
                'severity': 'high' if avg_esc > 0.18 else 'medium',
                'insight': f'{ch_name} has {avg_esc:.0%} escalation — customers forced to switch channels',
            })
        if repeat_rate > 0.30:
            friction_signals.append({
                'from': ch_name, 'to': ch_name, 'type': 'repeat_friction',
                'volume': int(ch_volume * repeat_rate * 0.3),
                'severity': 'high' if repeat_rate > 0.40 else 'medium',
                'insight': f'{ch_name} FCR at {avg_fcr:.0%} — repeat contacts driving volume inflation',
            })
    return friction_signals


def compute_cost_analysis(channels_data):
    all_cpcs = [cd.get('avgCpc', 0) for cd in channels_data.values() if cd.get('avgCpc', 0) > 0]
    base_cpc = min(all_cpcs) if all_cpcs else 5.0
    cost_analysis = {}
    for ch_name, ch_data in channels_data.items():
        actual = ch_data.get('avgCpc', 0)
        hier = CHANNEL_HIERARCHY.get(ch_name, {'costIndex': 5.0})
        expected = base_cpc * hier['costIndex']
        gap = actual / max(expected, 0.01) if expected > 0 else 1.0
        eff = 'over' if gap > 1.15 else 'under' if gap < 0.85 else 'normal'
        cost_analysis[ch_name] = {
            'actualCpc': round(actual, 2), 'expectedCpc': round(expected, 2),
            'costGap': round(gap, 3), 'efficiency': eff, 'costIndex': hier['costIndex'],
        }
    return cost_analysis, base_cpc


def compute_migration_readiness(ch_name, ch_data, best_digital_cpc, total_volume):
    ch_queues = ch_data['queues']
    ch_volume = ch_data['volume']
    if ch_volume == 0:
        return 0.0
    repeatable_vol = sum(q['volume'] for q in ch_queues if classify_intent_tier(q) in ('transactional', 'informational'))
    repeatable_pct = repeatable_vol / max(ch_volume, 1)
    actual_cpc = ch_data.get('avgCpc', 0)
    cost_gap = max(0, (actual_cpc - best_digital_cpc) / max(actual_cpc, 0.01)) if actual_cpc > 0 else 0
    low_cx_vol = sum(q['volume'] for q in ch_queues if q.get('complexity', 0.5) < 0.40)
    low_cx_pct = low_cx_vol / max(ch_volume, 1)
    readiness = (repeatable_pct * 0.4) + (cost_gap * 0.3) + (low_cx_pct * 0.3)
    return round(min(1.0, readiness), 3)


def decide_channel_strategy(ch_name, ch_data, readiness, all_channels_data, total_volume):
    hier = CHANNEL_HIERARCHY.get(ch_name, {'costTier': 5, 'digitalScore': 0.5})
    cost_tier = hier['costTier']
    ch_volume = ch_data['volume']
    vol_share = ch_volume / max(total_volume, 1)
    ch_queues = ch_data['queues']
    if ch_queues:
        avg_csat = sum(q['volume'] * q.get('csat', 3.5) for q in ch_queues) / max(ch_volume, 1)
        avg_esc = sum(q['volume'] * q.get('escalation', 0.1) for q in ch_queues) / max(ch_volume, 1)
    else:
        avg_csat, avg_esc = 3.5, 0.10
    digital_csat_sum = digital_vol_sum = 0
    for oc, od in all_channels_data.items():
        oh = CHANNEL_HIERARCHY.get(oc, {})
        if oh.get('digitalScore', 0) >= 0.6 and od['volume'] > 0:
            ov = od['volume']
            oc_csat = sum(q['volume'] * q.get('csat', 3.5) for q in od['queues']) / max(ov, 1)
            digital_csat_sum += oc_csat * ov
            digital_vol_sum += ov
    avg_digital_csat = digital_csat_sum / max(digital_vol_sum, 1) if digital_vol_sum > 0 else 3.5
    csat_gap = avg_csat - avg_digital_csat
    cx_risk = csat_gap > 0.4
    migrate_pct = 0.0

    if cx_risk and cost_tier >= 6:
        decision = 'protect'
        rationale = DECISION_RATIONALE.get(ch_name, {}).get('protect',
            f'{ch_name} CSAT ({avg_csat:.2f}) above digital avg ({avg_digital_csat:.2f}) — protect from deflection.')
    elif readiness > 0.5 and cost_tier >= 6 and not cx_risk:
        migrate_pct = min(0.30, readiness * 0.4)
        decision = 'migrate_from'
        rationale = DECISION_RATIONALE.get(ch_name, {}).get('migrate_from',
            f'Readiness {readiness:.0%}, cost tier {cost_tier}. Shift {migrate_pct:.0%} to digital.')
    elif cost_tier <= 4 and avg_csat >= 3.5 and vol_share >= 0.05:
        decision = 'invest'
        rationale = DECISION_RATIONALE.get(ch_name, {}).get('invest',
            f'{ch_name} good CX ({avg_csat:.2f}) at low cost. Expand coverage.')
    elif cost_tier <= 4 and (avg_csat < 3.5 or avg_esc > 0.12):
        decision = 'optimise'
        rationale = DECISION_RATIONALE.get(ch_name, {}).get('optimise',
            f'{ch_name} CX issues (CSAT {avg_csat:.2f}). Fix before expanding.')
    elif vol_share < 0.03:
        decision = 'sunset'
        rationale = DECISION_RATIONALE.get(ch_name, {}).get('sunset',
            f'{ch_name} only {vol_share:.1%} volume. Consolidate.')
    elif readiness > 0.3 and cost_tier >= 5:
        migrate_pct = min(0.15, readiness * 0.25)
        decision = 'optimise'
        rationale = DECISION_RATIONALE.get(ch_name, {}).get('optimise',
            f'Moderate readiness ({readiness:.0%}). Improve containment first.')
    elif avg_csat >= 3.3 and avg_esc < 0.15:
        decision = 'maintain'
        rationale = DECISION_RATIONALE.get(ch_name, {}).get('maintain',
            f'{ch_name} acceptable. Monitor quarterly.')
    else:
        decision = 'optimise'
        rationale = DECISION_RATIONALE.get(ch_name, {}).get('optimise',
            f'{ch_name} needs attention. Review CX and containment.')

    return decision, rationale, migrate_pct, {
        'avgCsat': round(avg_csat, 2), 'avgEscalation': round(avg_esc, 3),
        'avgDigitalCsat': round(avg_digital_csat, 2), 'csatGap': round(csat_gap, 2),
        'cxRisk': cx_risk, 'volShare': round(vol_share, 3),
    }


def compute_migrations(channels_data, decisions, total_volume):
    migrations = []
    for ch_name, decision_data in decisions.items():
        decision = decision_data['decision']
        migrate_pct = decision_data.get('migratePct', 0)
        ch_data = channels_data.get(ch_name, {})
        ch_queues = ch_data.get('queues', [])
        for queue in ch_queues:
            tier = classify_intent_tier(queue)
            tier_info = INTENT_TIERS[tier]
            q_volume = queue.get('volume', 0)
            q_csat = queue.get('csat', 3.5)
            target_channel = ch_name
            for bias_ch in tier_info['channel_bias']:
                if bias_ch != ch_name and bias_ch in channels_data:
                    target_channel = bias_ch
                    break
            if target_channel == ch_name and tier_info['migration_eligible']:
                current_tier = CHANNEL_HIERARCHY.get(ch_name, {}).get('costTier', 5)
                for cand in sorted(CHANNEL_HIERARCHY.keys(), key=lambda x: CHANNEL_HIERARCHY[x]['costTier']):
                    if CHANNEL_HIERARCHY[cand]['costTier'] < current_tier and cand in channels_data:
                        target_channel = cand
                        break
            if decision in ('migrate_from', 'optimise') and migrate_pct > 0 and tier_info['migration_eligible']:
                effective_pct = min(migrate_pct, tier_info['max_migration_pct'])
                if q_csat > 4.0:
                    effective_pct *= 0.5
                migrate_vol = int(q_volume * effective_pct)
                retain_vol = q_volume - migrate_vol
                if migrate_vol > 50:
                    migrations.append({
                        'fromChannel': ch_name, 'toChannel': target_channel,
                        'queue': queue.get('queue', queue.get('intent', 'Unknown')),
                        'intent': queue.get('intent', queue.get('queue', 'Unknown')),
                        'intentTier': tier, 'tierLabel': tier_info['label'],
                        'volume': migrate_vol, 'cxRisk': 'low' if tier == 'transactional' else 'medium',
                        'retained': False,
                    })
                    if retain_vol > 50:
                        migrations.append({
                            'fromChannel': ch_name, 'toChannel': ch_name,
                            'queue': queue.get('queue', queue.get('intent', 'Unknown')),
                            'intent': queue.get('intent', queue.get('queue', 'Unknown')),
                            'intentTier': tier, 'tierLabel': tier_info['label'],
                            'volume': retain_vol, 'cxRisk': 'n/a', 'retained': True,
                            'retainReason': 'Partial retention after migration',
                        })
                else:
                    migrations.append({
                        'fromChannel': ch_name, 'toChannel': ch_name,
                        'queue': queue.get('queue', queue.get('intent', 'Unknown')),
                        'intent': queue.get('intent', queue.get('queue', 'Unknown')),
                        'intentTier': tier, 'tierLabel': tier_info['label'],
                        'volume': q_volume, 'cxRisk': 'n/a', 'retained': True,
                        'retainReason': 'Volume too small for meaningful migration',
                    })
            else:
                reason = ('High complexity/emotion — human channel required' if tier in ('high_emotion', 'assisted')
                          else f'Channel protected — CX safeguard' if decision == 'protect'
                          else f'Channel strategy: {decision}')
                migrations.append({
                    'fromChannel': ch_name, 'toChannel': ch_name,
                    'queue': queue.get('queue', queue.get('intent', 'Unknown')),
                    'intent': queue.get('intent', queue.get('queue', 'Unknown')),
                    'intentTier': tier, 'tierLabel': tier_info['label'],
                    'volume': q_volume, 'cxRisk': 'n/a', 'retained': True,
                    'retainReason': reason,
                })
    return migrations


def build_intent_matrix(channels_data):
    intent_data = {}
    for ch_name, ch_data in channels_data.items():
        for q in ch_data['queues']:
            intent = q.get('intent', q.get('queue', 'Unknown'))
            tier = classify_intent_tier(q)
            if intent not in intent_data:
                intent_data[intent] = {'intent': intent, 'tier': tier,
                    'tierLabel': INTENT_TIERS[tier]['label'], 'channels': {}, 'totalVolume': 0}
            intent_data[intent]['channels'][ch_name] = intent_data[intent]['channels'].get(ch_name, 0) + q['volume']
            intent_data[intent]['totalVolume'] += q['volume']
    matrix = []
    for iname, idata in sorted(intent_data.items(), key=lambda x: -x[1]['totalVolume']):
        tier = idata['tier']
        tier_info = INTENT_TIERS[tier]
        current_primary = max(idata['channels'].items(), key=lambda x: x[1])[0] if idata['channels'] else 'Voice'
        rec_primary = current_primary
        for bc in tier_info['channel_bias']:
            if bc in channels_data or bc == current_primary:
                rec_primary = bc
                break
        rec_secondary = current_primary if current_primary != rec_primary else None
        if not rec_secondary and len(tier_info['channel_bias']) > 1:
            for bc in tier_info['channel_bias'][1:]:
                if bc in channels_data:
                    rec_secondary = bc
                    break
        matrix.append({
            'intent': iname, 'tier': tier, 'tierLabel': tier_info['label'],
            'currentPrimary': current_primary, 'recommendedPrimary': rec_primary,
            'recommendedSecondary': rec_secondary or current_primary,
            'volume': idata['totalVolume'], 'changed': current_primary != rec_primary,
        })
    return matrix


def compute_target_mix(channels_data, migrations, total_volume):
    current_mix = {ch: cd['volume'] for ch, cd in channels_data.items()}
    target_mix = {ch: 0 for ch in channels_data}
    for m in migrations:
        tch = m['toChannel']
        if tch not in target_mix:
            target_mix[tch] = 0
        target_mix[tch] += m['volume']
    mix = {}
    for ch in set(list(current_mix.keys()) + list(target_mix.keys())):
        curr = current_mix.get(ch, 0)
        tgt = target_mix.get(ch, 0)
        mix[ch] = {
            'current': curr, 'target': tgt,
            'currentPct': round(curr / max(total_volume, 1) * 100, 1),
            'targetPct': round(tgt / max(total_volume, 1) * 100, 1),
            'change': tgt - curr, 'changePct': round((tgt - curr) / max(total_volume, 1) * 100, 1),
        }
    return mix


def build_sankey(channels_data, migrations, total_volume):
    nodes = []; links = []; node_index = {}; idx = 0
    for ch in sorted(channels_data.keys(), key=lambda x: -channels_data[x]['volume']):
        nodes.append({'id': idx, 'name': ch, 'side': 'left', 'volume': channels_data[ch]['volume']})
        node_index[f'src_{ch}'] = idx; idx += 1
    target_vols = {}
    for m in migrations:
        target_vols[m['toChannel']] = target_vols.get(m['toChannel'], 0) + m['volume']
    for ch in sorted(target_vols.keys(), key=lambda x: -target_vols.get(x, 0)):
        nodes.append({'id': idx, 'name': ch, 'side': 'right', 'volume': target_vols.get(ch, 0)})
        node_index[f'tgt_{ch}'] = idx; idx += 1
    link_map = {}
    for m in migrations:
        key = (m['fromChannel'], m['toChannel'])
        if key not in link_map:
            link_map[key] = {'volume': 0, 'queues': [], 'type': 'retain'}
        link_map[key]['volume'] += m['volume']
        if not m.get('retained', True):
            link_map[key]['type'] = 'migrate'
        link_map[key]['queues'].append({
            'queue': m['queue'], 'volume': m['volume'],
            'tier': m.get('tierLabel', ''), 'retained': m.get('retained', True),
        })
    for (fc, tc), ld in link_map.items():
        si = node_index.get(f'src_{fc}')
        ti = node_index.get(f'tgt_{tc}')
        if si is not None and ti is not None and ld['volume'] > 0:
            links.append({
                'source': si, 'target': ti, 'value': ld['volume'], 'type': ld['type'],
                'queues': ld['queues'][:5], 'label': f"{fc} → {tc}: {ld['volume']:,}",
            })
    return {'nodes': nodes, 'links': links}


def compute_migration_savings(migrations, channels_data, cost_analysis):
    total_saving = 0
    for m in migrations:
        if m.get('retained', True):
            continue
        from_cpc = cost_analysis.get(m['fromChannel'], {}).get('actualCpc', 0)
        to_cpc = cost_analysis.get(m['toChannel'], {}).get('actualCpc', 0)
        if from_cpc > to_cpc:
            total_saving += (from_cpc - to_cpc) * m['volume'] * 12
    fte_saved = total_saving / max(55000, 1)
    return {
        'annualMigrationSaving': round(total_saving),
        'monthlyMigrationSaving': round(total_saving / 12),
        'fteSaved': round(fte_saved, 1),
    }


def run_channel_strategy(data, diagnostic, initiatives=None):
    queues = data['queues']; params = data['params']
    channels_data = {}
    for q in queues:
        ch = q['channel']
        if ch not in channels_data:
            channels_data[ch] = {'volume': 0, 'queues': [], 'scores': [], 'cpc_vals': []}
        channels_data[ch]['volume'] += q['volume']
        channels_data[ch]['queues'].append(q)
        ds = next((qs for qs in diagnostic.get('queueScores', []) if qs['queue'] == q['queue']), None)
        if ds:
            channels_data[ch]['scores'].append(ds['overallScore'])
        if q.get('cpc'):
            channels_data[ch]['cpc_vals'].append(q['cpc'])
    for ch_name, ch_data in channels_data.items():
        ch_data['avgCpc'] = (sum(ch_data['cpc_vals']) / len(ch_data['cpc_vals'])) if ch_data['cpc_vals'] else 0
    total_vol = sum(cd['volume'] for cd in channels_data.values()) or 1

    friction_signals = analyse_channel_friction(channels_data, total_vol)
    cost_analysis, base_cpc = compute_cost_analysis(channels_data)

    digital_cpcs = [cd.get('avgCpc', 0) for ch, cd in channels_data.items()
                    if CHANNEL_HIERARCHY.get(ch, {}).get('digitalScore', 0) >= 0.6 and cd.get('avgCpc', 0) > 0]
    best_digital_cpc = min(digital_cpcs) if digital_cpcs else base_cpc

    # v12-#49: Compute initiative-driven deflection rates per channel
    init_deflection_by_channel = {}
    if initiatives:
        enabled = [i for i in initiatives if i.get('enabled')]
        deflection_levers = {'deflection', 'self_service', 'channel_shift', 'automation'}
        for init in enabled:
            if init.get('lever', '') in deflection_levers:
                impact = init.get('impact', 0) * init.get('adoption', 0.8)
                target_channels = init.get('channels', [])
                for ch in target_channels:
                    if ch in channels_data:
                        init_deflection_by_channel[ch] = init_deflection_by_channel.get(ch, 0) + impact

    recommendations = []; decisions = {}; cx_safeguards = {}; readiness_scores = {}
    for ch_name, ch_data in channels_data.items():
        hier = CHANNEL_HIERARCHY.get(ch_name, {'costTier': 5, 'digitalScore': 0.5, 'automationPotential': 0.40})
        avg_score = sum(ch_data['scores']) / len(ch_data['scores']) if ch_data['scores'] else 50
        readiness = compute_migration_readiness(ch_name, ch_data, best_digital_cpc, total_vol)
        readiness_scores[ch_name] = readiness
        decision, rationale, migrate_pct, cx_metrics = decide_channel_strategy(
            ch_name, ch_data, readiness, channels_data, total_vol)

        # v12-#49: Override with initiative-driven deflection if available
        init_defl = init_deflection_by_channel.get(ch_name, 0)
        if init_defl > 0 and hier.get('costTier', 5) >= 5:
            # Initiative says to deflect from this channel — override health-only decision
            new_migrate_pct = min(0.45, init_defl)
            if new_migrate_pct > migrate_pct:
                migrate_pct = new_migrate_pct
                if decision == 'maintain':
                    decision = 'migrate_from'
                    rationale = f'Initiative-driven: {init_defl:.0%} deflection potential from enabled initiatives. ' + rationale
                elif decision in ('optimise', 'protect'):
                    rationale = f'Initiative overlay: {init_defl:.0%} additional deflection. ' + rationale

        decisions[ch_name] = {'decision': decision, 'rationale': rationale, 'migratePct': migrate_pct}
        if cx_metrics['cxRisk']:
            cx_safeguards[ch_name] = {
                'channel': ch_name, 'csat': cx_metrics['avgCsat'],
                'digitalAvg': cx_metrics['avgDigitalCsat'], 'gap': cx_metrics['csatGap'],
                'reason': f'{ch_name} CSAT ({cx_metrics["avgCsat"]:.2f}) above digital avg ({cx_metrics["avgDigitalCsat"]:.2f}) — protected',
            }
        tier_breakdown = {}
        for q in ch_data['queues']:
            t = classify_intent_tier(q)
            tier_breakdown[t] = tier_breakdown.get(t, 0) + q['volume']
        recommendations.append({
            'channel': ch_name, 'decision': decision, 'rationale': rationale,
            'migrationReadiness': readiness, 'migratePct': round(migrate_pct * 100, 1),
            'volumeShare': round(ch_data['volume'] / total_vol * 100, 1),
            'avgScore': round(avg_score, 1), 'avgCpc': round(ch_data.get('avgCpc', 0), 2),
            'digitalScore': hier['digitalScore'], 'automationPotential': round(hier['automationPotential'] * 100),
            'costTier': hier['costTier'], 'costAnalysis': cost_analysis.get(ch_name, {}),
            'cxMetrics': cx_metrics,
            'tierBreakdown': {k: round(v / max(ch_data['volume'], 1) * 100, 1) for k, v in tier_breakdown.items()},
            'migrationTarget': None, 'migrationVolume': 0,
            'queueCount': len(ch_data['queues']), 'totalVolume': ch_data['volume'],
        })
    recommendations.sort(key=lambda x: x['totalVolume'], reverse=True)

    # Backfill migrationTarget for backward compat
    for rec in recommendations:
        ch = rec['channel']; dec = decisions[ch]
        if dec['migratePct'] > 0:
            for q in channels_data[ch]['queues']:
                tier = classify_intent_tier(q)
                if INTENT_TIERS[tier]['migration_eligible']:
                    for bc in INTENT_TIERS[tier]['channel_bias']:
                        if bc != ch and bc in channels_data:
                            rec['migrationTarget'] = bc
                            rec['migrationVolume'] = int(channels_data[ch]['volume'] * dec['migratePct'])
                            break
                    if rec['migrationTarget']:
                        break

    migrations = compute_migrations(channels_data, decisions, total_vol)
    intent_matrix = build_intent_matrix(channels_data)
    target_mix = compute_target_mix(channels_data, migrations, total_vol)
    sankey = build_sankey(channels_data, migrations, total_vol)
    migration_savings = compute_migration_savings(migrations, channels_data, cost_analysis)

    current_digital = sum(cd['volume'] for ch, cd in channels_data.items()
        if CHANNEL_HIERARCHY.get(ch, {}).get('digitalScore', 0) >= 0.6) / total_vol
    target_digital_vol = sum(target_mix[ch]['target'] for ch in target_mix
        if CHANNEL_HIERARCHY.get(ch, {}).get('digitalScore', 0) >= 0.6)
    target_digital = target_digital_vol / max(total_vol, 1)

    return {
        'recommendations': recommendations,
        'intentMatrix': intent_matrix,
        'targetMix': target_mix,
        'migrations': migrations,
        'migrationReadiness': readiness_scores,
        'frictionSignals': friction_signals,
        'cxSafeguards': cx_safeguards,
        'costAnalysis': cost_analysis,
        'migrationSavings': migration_savings,
        'sankey': sankey,
        'currentDigitalPct': round(current_digital * 100, 1),
        'targetDigitalPct': round(target_digital * 100, 1),
        'totalChannels': len(channels_data),
    }
