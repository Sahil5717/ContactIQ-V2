"""
ContactIQ — Gross Impact Engine
Computes lever-specific gross FTE impact for each initiative.

Instead of a single generic formula (affected_FTE × impact% × adoption),
each lever type has its own physics:

  Deflection:    contacts_removed → hours_saved → FTE
  AHT Reduction: seconds_saved_per_contact × eligible_contacts → hours → FTE
  Transfer:      transfers_avoided × extra_time_per_transfer → hours → FTE
  Escalation:    escalations_avoided × extra_time_per_escalation → hours → FTE
  Repeat:        repeat_contacts_eliminated × AHT → hours → FTE
  Location:      FTE_migrated × cost_arbitrage (NO workload reduction)
  Shrinkage:     shrinkage_pct_reduced × total_FTE
"""
import math


def compute_gross_impact(initiative, enriched_queues, roles, pools_data, params, cost_matrix=None):
    """
    Compute the gross FTE impact for a single initiative using lever-specific physics.
    
    P2-1: Now filters queues by initiative's targetBUs (if specified) and uses
    location-aware costs from cost_matrix when available.
    
    Args:
        initiative: dict with id, lever, impact, adoption, channels, roles, targetBUs, etc.
        enriched_queues: output of intent_profile.enrich_intents()
        roles: list of role dicts
        pools_data: output of pools.compute_pools() — for reference ceilings
        params: system parameters
        cost_matrix: dict[location][sourcing] = {costPerFTE, ...} or None
    
    Returns:
        dict with gross_fte, gross_contacts, gross_saving, mechanism, per_bu breakdown
    """
    lever = initiative.get('lever', 'aht_reduction')
    impact = initiative.get('impact', 0)
    adoption = initiative.get('adoption', 0.80)
    channels = set(initiative.get('channels', []))
    target_roles = set(initiative.get('roles', []))
    target_bus = set(initiative.get('targetBUs', []))  # P2-1: empty = all BUs
    
    # Get queues matching this initiative's channels AND BUs
    matching_queues = [q for q in enriched_queues 
                       if q.get('channel', '') in channels
                       and (not target_bus or q.get('bu', '') in target_bus)]
    if not matching_queues:
        matching_queues = enriched_queues  # fallback: all queues
    
    # Get roles matching this initiative
    affected_roles = [r for r in roles if r['role'] in target_roles]
    affected_fte = sum(r['headcount'] for r in affected_roles)
    if affected_fte > 0:
        weighted_cost = sum(r['headcount'] * r['costPerFTE'] for r in affected_roles) / affected_fte
    else:
        weighted_cost = 55000
    
    # P2-1: Location-aware cost is tracked at the pool/BU level (pools.py).
    # Per-initiative savings use role-weighted costs from HRIS data (more accurate
    # than the config cost matrix, since role costs reflect actual payroll).
    # cost_matrix is used by pools for BU-level dashboards and arbitrage calculations.
    
    # Net productive hours
    shrinkage = params.get('shrinkage', 0.30)
    net_prod_hours = params.get('grossHoursPerYear', 2080) * (1 - shrinkage)
    
    total_matching_volume = sum(q['volume'] for q in matching_queues)
    
    # ── Dispatch to lever-specific formula ──
    if lever == 'deflection':
        return _gross_deflection(initiative, matching_queues, affected_fte, weighted_cost,
                                  net_prod_hours, impact, adoption, pools_data)
    
    elif lever == 'aht_reduction':
        return _gross_aht_reduction(initiative, matching_queues, affected_fte, weighted_cost,
                                     net_prod_hours, impact, adoption, total_matching_volume, pools_data)
    
    elif lever == 'escalation_reduction':
        return _gross_escalation(initiative, matching_queues, affected_fte, weighted_cost,
                                  net_prod_hours, impact, adoption, pools_data)
    
    elif lever == 'repeat_reduction':
        return _gross_repeat(initiative, matching_queues, affected_fte, weighted_cost,
                              net_prod_hours, impact, adoption, pools_data)
    
    elif lever == 'transfer_reduction':
        return _gross_transfer(initiative, matching_queues, affected_fte, weighted_cost,
                                net_prod_hours, impact, adoption, pools_data)
    
    elif lever == 'cost_reduction':
        return _gross_location(initiative, matching_queues, affected_fte, weighted_cost,
                                impact, adoption, pools_data, params)
    
    elif lever == 'shrinkage_reduction':
        return _gross_shrinkage(initiative, affected_fte, weighted_cost,
                                 impact, adoption, pools_data, params)
    
    else:
        # Unknown lever — use generic formula with conservative cap
        return _gross_generic(initiative, affected_fte, weighted_cost, impact, adoption)


def _gross_deflection(init, queues, affected_fte, cost, net_hours, impact, adoption, pools):
    """
    Deflection: contacts_removed × avg_handle_time → hours_saved → FTE
    
    Gross = Σ(Volume_i × Eligible_i × Impact × Adoption) across eligible intents
    
    NOTE (V6 fix): deflection_eligible_pct already incorporates containment_feasibility
    (eligible = repeatability × containment × auth_penalty). So we must NOT multiply
    by containment again here. The initiative's `impact` represents its own effectiveness
    rate, applied directly to the eligible pool.
    """
    total_deflectable = 0
    mechanism_parts = []
    
    for q in queues:
        eligible_pct = q.get('deflection_eligible_pct', 0)
        containment = q.get('containment_feasibility', 0)
        
        # V6: eligible_pct is now containment-free (repeatability × auth_penalty only).
        # Containment is applied here as a cap: the initiative can't contain more
        # than what's physically feasible for this intent.
        effective_rate = eligible_pct * min(impact, containment) * adoption
        
        contacts_deflected = q['volume'] * effective_rate
        total_deflectable += contacts_deflected
        
        if contacts_deflected > 10:
            mechanism_parts.append(f"{q.get('intent','?')}/{q.get('channel','?')}: "
                                   f"{q['volume']:,} × {effective_rate:.1%} = {contacts_deflected:,.0f}")
    
    # Convert contacts to hours to FTE
    avg_aht_sec = sum(q['aht'] * 60 * q['volume'] for q in queues) / max(sum(q['volume'] for q in queues), 1)
    hours_saved = (total_deflectable * avg_aht_sec) / 3600
    gross_fte = hours_saved / max(net_hours, 1)
    
    return {
        'gross_fte': round(gross_fte, 1),
        'gross_contacts': round(total_deflectable),
        'gross_seconds': 0,
        'gross_saving': round(gross_fte * cost),
        'mechanism': f"Deflection: {total_deflectable:,.0f} contacts × {avg_aht_sec:.0f}s AHT → "
                     f"{hours_saved:,.0f} hrs → {gross_fte:.1f} FTE",
        'mechanism_detail': mechanism_parts[:5],
        'eligible_volume': sum(q['volume'] for q in queues),
    }


def _gross_aht_reduction(init, queues, affected_fte, cost, net_hours, impact, adoption, total_vol, pools):
    """
    AHT Reduction: seconds_saved_per_contact × eligible_contacts → hours → FTE
    Only search + wrap time is reducible.
    """
    total_seconds_saved = 0
    mechanism_parts = []
    
    for q in queues:
        decomp = q.get('aht_decomp', {})
        reducible_sec = decomp.get('reducible_sec', q['aht'] * 60 * 0.35)  # fallback: 35% of AHT (search+wrap+talk efficiency)
        
        # Initiative's impact represents fraction of reducible time it can save
        seconds_per_contact = reducible_sec * impact * adoption
        total_saved = q['volume'] * seconds_per_contact
        total_seconds_saved += total_saved
        
        if total_saved > 100:
            mechanism_parts.append(f"{q.get('intent','?')}: {q['volume']:,} × {seconds_per_contact:.1f}s = "
                                   f"{total_saved/3600:.0f}hrs")
    
    hours_saved = total_seconds_saved / 3600
    gross_fte = hours_saved / max(net_hours, 1)
    
    return {
        'gross_fte': round(gross_fte, 1),
        'gross_contacts': 0,
        'gross_seconds': round(total_seconds_saved),
        'gross_saving': round(gross_fte * cost),
        'mechanism': f"AHT: {total_seconds_saved/3600:,.0f} hrs saved across {total_vol:,} contacts → {gross_fte:.1f} FTE",
        'mechanism_detail': mechanism_parts[:5],
        'eligible_volume': total_vol,
    }


def _gross_escalation(init, queues, affected_fte, cost, net_hours, impact, adoption, pools):
    """
    Escalation reduction: preventable escalations avoided × extra time per escalation → FTE
    """
    total_prevented = 0
    for q in queues:
        esc_rate = q.get('escalation', 0)
        complexity = q.get('complexity', 0.5)
        prev_share = max(0.10, 0.60 - complexity * 0.50)
        preventable = q['volume'] * esc_rate * prev_share * impact * adoption
        total_prevented += preventable
    
    extra_sec_per_esc = 900  # V6: 15 min — prevented escalation avoids full L2/L3 handle time
    hours_saved = (total_prevented * extra_sec_per_esc) / 3600
    gross_fte = hours_saved / max(net_hours, 1)
    
    return {
        'gross_fte': round(gross_fte, 1),
        'gross_contacts': round(total_prevented),
        'gross_seconds': 0,
        'gross_saving': round(gross_fte * cost),
        'mechanism': f"Escalation: {total_prevented:,.0f} prevented × 15min = {hours_saved:,.0f}hrs → {gross_fte:.1f} FTE",
        'mechanism_detail': [],
        'eligible_volume': sum(q['volume'] for q in queues),
    }


def _gross_transfer(init, queues, affected_fte, cost, net_hours, impact, adoption, pools):
    """
    Transfer reduction: preventable transfers avoided × extra time per transfer → FTE
    
    Transfers differ from escalations: transfers are lateral (agent-to-agent within same tier),
    while escalations go up-tier. Transfer extra time is typically shorter (3 min vs 5 min).
    """
    total_prevented = 0
    for q in queues:
        transfer_rate = q.get('transfer', q.get('transfer_rate', 0))
        complexity = q.get('complexity', 0.5)
        # Preventable share: higher for simple intents (routing errors), lower for complex
        prev_share = q.get('preventable_transfer_pct', max(0.15, 0.55 - complexity * 0.40))
        preventable = q['volume'] * transfer_rate * prev_share * impact * adoption
        total_prevented += preventable
    
    extra_sec_per_transfer = 180  # 3 min per prevented transfer (shorter than escalation)
    hours_saved = (total_prevented * extra_sec_per_transfer) / 3600
    gross_fte = hours_saved / max(net_hours, 1)
    
    return {
        'gross_fte': round(gross_fte, 1),
        'gross_contacts': round(total_prevented),
        'gross_seconds': 0,
        'gross_saving': round(gross_fte * cost),
        'mechanism': f"Transfer: {total_prevented:,.0f} prevented × 3min = {hours_saved:,.0f}hrs → {gross_fte:.1f} FTE",
        'mechanism_detail': [],
        'eligible_volume': sum(q['volume'] for q in queues),
    }


def _gross_repeat(init, queues, affected_fte, cost, net_hours, impact, adoption, pools):
    """
    Repeat reduction: repeat contacts eliminated × AHT → FTE
    
    V6 fix: Raw CCaaS exports often cover 1-3 months, making repeat detection
    unreliable (customer rarely contacts twice in one month). If weighted avg
    repeat rate < 2%, fall back to industry default (1 - avgFCR) as proxy.
    """
    # V6: Check if repeat data is implausibly low (data artifact from short sample)
    total_vol = sum(q['volume'] for q in queues)
    if total_vol > 0:
        weighted_repeat = sum(q.get('repeat', 0) * q['volume'] for q in queues) / total_vol
    else:
        weighted_repeat = 0
    
    REPEAT_FLOOR = 0.02  # Below this, data is unreliable
    use_fallback = weighted_repeat < REPEAT_FLOOR
    
    if use_fallback:
        # Derive from FCR: repeat ≈ (1 - FCR) × 0.6 — not all non-FCR contacts are repeats
        weighted_fcr = sum(q.get('fcr', 0.75) * q['volume'] for q in queues) / max(total_vol, 1)
        fallback_repeat = max(0.05, (1 - weighted_fcr) * 0.60)
    
    total_eliminated = 0
    for q in queues:
        repeat_rate = fallback_repeat if use_fallback else q.get('repeat', 0)
        eliminable = q['volume'] * repeat_rate * impact * adoption
        # Cap at 70% of actual repeats
        eliminable = min(eliminable, q['volume'] * repeat_rate * 0.70)
        total_eliminated += eliminable
    
    avg_aht_sec = sum(q['aht'] * 60 * q['volume'] for q in queues) / max(sum(q['volume'] for q in queues), 1)
    hours_saved = (total_eliminated * avg_aht_sec) / 3600
    gross_fte = hours_saved / max(net_hours, 1)
    
    return {
        'gross_fte': round(gross_fte, 1),
        'gross_contacts': round(total_eliminated),
        'gross_seconds': 0,
        'gross_saving': round(gross_fte * cost),
        'mechanism': f"Repeat: {total_eliminated:,.0f} contacts eliminated → {hours_saved:,.0f}hrs → {gross_fte:.1f} FTE"
                     + (f" (using FCR-derived {fallback_repeat:.0%} rate — raw data too sparse)" if use_fallback else ""),
        'mechanism_detail': [],
        'eligible_volume': sum(q['volume'] for q in queues),
    }


def _gross_location(init, queues, affected_fte, cost, impact, adoption, pools, params):
    """
    Location: FTE migrated × cost arbitrage. NO workload reduction.
    The initiative moves people, not removes work.
    
    v14 fix: Uses location_readiness (offshoring suitability) instead of
    migration_readiness (channel migration suitability). Text channels like
    Chat/Email are EASIER to offshore than Voice (no accent/language barriers).
    """
    # What % of affected FTE can actually be migrated offshore
    migratable_share = 0
    total_vol = sum(q['volume'] for q in queues)
    if total_vol > 0:
        migratable_vol = 0
        for q in queues:
            # v14: Use location-specific readiness, not channel migration readiness
            loc_readiness = q.get('location_readiness', None)
            if loc_readiness is None:
                # Derive from channel + complexity: text channels are most offshorable
                ch = q.get('channel', '')
                complexity = q.get('complexity', 0.5)
                emotional_risk = q.get('emotional_risk', q.get('emotionalRisk', 0.3))
                loc_readiness = _location_readiness(ch, complexity, emotional_risk)
            migratable_vol += q['volume'] * loc_readiness
        migratable_share = migratable_vol / total_vol
    
    fte_migrated = affected_fte * migratable_share * impact * adoption
    cost_arbitrage = params.get('locationArbitrage', 0.35)
    saving = fte_migrated * cost * cost_arbitrage
    
    return {
        'gross_fte': 0,  # Location doesn't reduce FTE — it reduces cost
        'gross_contacts': 0,
        'gross_seconds': 0,
        'gross_saving': round(saving),
        'gross_fte_migrated': round(fte_migrated, 1),
        'mechanism': f"Location: {fte_migrated:.1f} FTE migrated × {cost_arbitrage:.0%} arbitrage = ${saving:,.0f}/yr",
        'mechanism_detail': [],
        'eligible_volume': total_vol,
        '_is_location': True,
    }


def _location_readiness(channel, complexity, emotional_risk):
    """
    v14: Assess how suitable a queue is for geographic relocation (offshore/nearshore).
    
    Unlike channel migration readiness (Voice→Digital), location readiness measures
    how easily agents handling this work can be moved to a lower-cost geography.
    Text-based channels (Chat, Email) are MOST suitable — no accent/language barriers.
    Voice is moderately suitable. Complex or emotionally sensitive work is less suitable.
    """
    # Base readiness by channel — text channels are easiest to offshore
    channel_base = {
        'Email': 0.85,
        'Chat': 0.80,
        'SMS/WhatsApp': 0.75,
        'Social Media': 0.70,
        'DIGITAL': 0.70,
        'MESSAGE': 0.70,
        'App/Self-Service': 0.60,  # Self-service is already automated, less offshoring gain
        'IVR': 0.50,               # IVR is automated, limited offshoring gain
        'Voice': 0.55,             # Voice has accent/language considerations
    }
    base = channel_base.get(channel, 0.50)
    base -= complexity * 0.25         # Complex work harder to offshore (training, QA)
    base -= emotional_risk * 0.20     # Emotional work needs cultural nuance
    return round(max(0.05, min(1.0, base)), 3)


def _gross_shrinkage(init, affected_fte, cost, impact, adoption, pools, params):
    """
    Shrinkage: reduce shrinkage % → release capacity → FTE equivalent
    
    V4.6-#6 fix: Shrinkage reduction frees capacity across the ENTIRE FTE base,
    not just the implementing roles (e.g. WFM Analysts). When shrinkage drops from
    30% to 28%, ALL agents get 2% more productive time, not just the analysts who
    configured the WFM tool.
    
    We use total FTE from pools data as the base, with the initiative's impact/adoption
    determining how much of the shrinkage gap it can close.
    """
    current_shrinkage = params.get('shrinkage', 0.30)
    target = params.get('targetShrinkage', 0.22)
    max_reduction = max(0, current_shrinkage - target)
    
    shrinkage_reduction = current_shrinkage * impact * adoption
    shrinkage_reduction = min(shrinkage_reduction, max_reduction)
    
    # V4.6-#6: Use total FTE base, not just target roles
    # Shrinkage improvement benefits ALL agents, not just WFM/Supervisors
    total_fte = params.get('totalFTE', affected_fte)
    if pools and isinstance(pools, dict):
        pool_summary = pools.get('summary', {})
        total_fte = pool_summary.get('total_fte', total_fte)
    
    fte_freed = total_fte * shrinkage_reduction
    
    # Cost: use weighted average cost across all roles (not just target roles)
    avg_cost = cost  # fallback
    if pools and isinstance(pools, dict):
        pool_summary = pools.get('summary', {})
        avg_cost = pool_summary.get('weighted_cost_per_fte', cost)
    
    saving = fte_freed * avg_cost
    
    return {
        'gross_fte': round(fte_freed, 1),
        'gross_contacts': 0,
        'gross_seconds': 0,
        'gross_saving': round(saving),
        'mechanism': f"Shrinkage: {shrinkage_reduction:.1%} reduction on {total_fte} total FTE → {fte_freed:.1f} FTE",
        'mechanism_detail': [],
        'eligible_volume': 0,
    }


def _gross_generic(init, affected_fte, cost, impact, adoption):
    """
    Fallback generic formula for unknown levers.
    Conservative: 25% haircut on raw impact (pools provide primary ceiling).
    """
    raw_fte = affected_fte * impact * adoption * 0.75
    saving = raw_fte * cost
    
    return {
        'gross_fte': round(raw_fte, 1),
        'gross_contacts': 0,
        'gross_seconds': 0,
        'gross_saving': round(saving),
        'mechanism': f"Generic: {affected_fte} FTE × {impact:.0%} × {adoption:.0%} × 75% safety = {raw_fte:.1f} FTE",
        'mechanism_detail': [],
        'eligible_volume': 0,
    }


# ── Multi-Lever Secondary Impact Engine ──
# CR-025: 34 of 58 initiatives have secondary lever impacts that were previously uncredited.
# Secondary impacts are weighted at 50% to reflect indirect mechanism (e.g., FCR improvement
# reduces repeats, but not all FCR gain translates 1:1 to repeat reduction).
SECONDARY_WEIGHT = 0.50
SECONDARY_THRESHOLD = 0.03  # Minimum impact to qualify as meaningful secondary lever

# Map initiative fields to lever types for secondary computation
SECONDARY_LEVER_MAP = {
    'fcrImpact': 'repeat_reduction',       # FCR improvement → fewer repeat contacts
    'ahtImpact': 'aht_reduction',          # AHT change → handle time savings
    'csatImpact': None,                     # CSAT doesn't map to FTE reduction directly
}


def compute_secondary_impacts(initiative, enriched_queues, roles, pools_data, params):
    """
    Compute secondary lever FTE impacts for a multi-lever initiative.
    Only computes for levers that are DIFFERENT from the primary lever.
    
    Returns list of dicts: [{lever, gross_fte, gross_saving, mechanism, field}, ...]
    """
    primary_lever = initiative.get('lever', '')
    adoption = initiative.get('adoption', 0.80)
    channels = set(initiative.get('channels', []))
    target_roles = set(initiative.get('roles', []))
    
    matching_queues = [q for q in enriched_queues if q.get('channel', '') in channels]
    if not matching_queues:
        matching_queues = enriched_queues
    
    affected_roles = [r for r in roles if r['role'] in target_roles]
    affected_fte = sum(r['headcount'] for r in affected_roles)
    weighted_cost = (sum(r['headcount'] * r['costPerFTE'] for r in affected_roles) / affected_fte) if affected_fte > 0 else 55000
    
    shrinkage = params.get('shrinkage', 0.30)
    net_prod_hours = params.get('grossHoursPerYear', 2080) * (1 - shrinkage)
    
    secondaries = []
    
    for field, target_lever in SECONDARY_LEVER_MAP.items():
        if target_lever is None:
            continue
        if target_lever == primary_lever:
            continue  # Don't double-count primary lever
        
        impact_val = initiative.get(field, 0)
        # For ahtImpact, it's stored as negative (e.g., -0.20 means 20% reduction)
        if field == 'ahtImpact':
            impact_val = abs(impact_val)
        
        if impact_val < SECONDARY_THRESHOLD:
            continue
        
        # Compute secondary FTE using simplified physics
        secondary_fte = 0
        mechanism = ''
        
        if target_lever == 'repeat_reduction' and field == 'fcrImpact':
            # FCR improvement → fewer repeat contacts
            total_vol = sum(q['volume'] for q in matching_queues)
            weighted_repeat = 0
            if total_vol > 0:
                weighted_repeat = sum(q.get('repeat', 0.15) * q['volume'] for q in matching_queues) / total_vol
                if weighted_repeat < 0.02:
                    weighted_fcr = sum(q.get('fcr', 0.75) * q['volume'] for q in matching_queues) / total_vol
                    weighted_repeat = max(0.05, (1 - weighted_fcr) * 0.60)
            
            # FCR improvement of X% eliminates X% of repeat volume
            repeats_eliminated = total_vol * weighted_repeat * impact_val * adoption * SECONDARY_WEIGHT
            avg_aht_sec = sum(q['aht'] * 60 * q['volume'] for q in matching_queues) / max(total_vol, 1)
            hours_saved = (repeats_eliminated * avg_aht_sec) / 3600
            secondary_fte = hours_saved / max(net_prod_hours, 1)
            mechanism = f"Secondary repeat reduction via FCR +{impact_val:.0%}: {repeats_eliminated:,.0f} contacts → {secondary_fte:.1f} FTE"
        
        elif target_lever == 'aht_reduction' and field == 'ahtImpact':
            # Secondary AHT reduction (initiative's primary lever is something else)
            total_vol = sum(q['volume'] for q in matching_queues)
            total_seconds = 0
            for q in matching_queues:
                decomp = q.get('aht_decomp', {})
                reducible_sec = decomp.get('reducible_sec', q['aht'] * 60 * 0.35)
                seconds_per_contact = reducible_sec * impact_val * adoption * SECONDARY_WEIGHT
                total_seconds += q['volume'] * seconds_per_contact
            
            hours_saved = total_seconds / 3600
            secondary_fte = hours_saved / max(net_prod_hours, 1)
            mechanism = f"Secondary AHT reduction {impact_val:.0%}: {hours_saved:,.0f} hrs → {secondary_fte:.1f} FTE"
        
        if secondary_fte > 0.1:  # Only include if meaningful
            secondaries.append({
                'lever': target_lever,
                'gross_fte': round(secondary_fte, 1),
                'gross_saving': round(secondary_fte * weighted_cost),
                'mechanism': mechanism,
                'field': field,
                'weight': SECONDARY_WEIGHT,
            })
    
    return secondaries
