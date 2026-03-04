"""
ContactIQ — Opportunity Pool Engine
Computes hard ceilings for each benefit lever from actual client data.

Pool Types:
  1. Deflection Pool — max contacts that could be self-served / automated
  2. AHT Pool — max seconds saveable from search+wrap reduction
  3. Transfer Pool — max preventable transfers that can be avoided
  4. Escalation Pool — max preventable escalations
  5. Location Pool — max FTE migratable to lower-cost locations
  6. Shrinkage Pool — max shrinkage reduction from WFM improvements

Each pool is computed from the enriched intent data (intent_profile.py output)
and provides a ceiling that the waterfall netting engine cannot exceed.
"""


def compute_pools(enriched_queues, roles, params, cost_matrix=None):
    """
    Compute all opportunity pools from enriched queue data.
    
    P2-1: Now computes BOTH global pools AND per-BU sub-pools.
    Global pools are used for backward-compatible waterfall netting.
    Per-BU pools provide dimensional breakdown for dashboards.
    
    Args:
        enriched_queues: output of intent_profile.enrich_intents()
        roles: list of role dicts with headcount, costPerFTE, location, sourcing
        params: system params (shrinkage, productive hours, etc.)
        cost_matrix: dict[location][sourcing] = {costPerFTE, ...} or None
    
    Returns:
        dict with pool name → {ceiling, unit, breakdown, fte_equivalent, by_bu}
    """
    total_volume_raw = sum(q['volume'] for q in enriched_queues)
    total_fte = sum(r['headcount'] for r in roles)
    
    # ── Net productive hours per FTE per year ──
    shrinkage = params.get('shrinkage', 0.30)
    gross_hours_year = params.get('grossHoursPerYear', 2080)
    net_prod_hours = gross_hours_year * (1 - shrinkage)
    
    annualization = params.get('volumeAnnualizationFactor', 12)
    total_volume = total_volume_raw * annualization
    
    # Apply annualization to queue volumes
    ann_queues = []
    for q in enriched_queues:
        aq = dict(q)
        aq['annual_volume'] = q['volume'] * annualization
        ann_queues.append(aq)
    
    # ── P2-1: Location-weighted cost per FTE ──
    # Global weighted cost (backward compat)
    if total_fte > 0:
        weighted_cost = sum(r['headcount'] * r['costPerFTE'] for r in roles) / total_fte
    else:
        weighted_cost = 55000
    
    # Per-BU weighted cost using location/sourcing from queues and cost matrix
    bus = sorted(set(q.get('bu', 'Default') for q in ann_queues))
    bu_weighted_costs = {}
    bu_fte = {}
    for bu in bus:
        bu_queues = [q for q in ann_queues if q.get('bu') == bu]
        bu_vol = sum(q['annual_volume'] for q in bu_queues)
        if bu_vol > 0 and cost_matrix:
            # Weight cost by volume share at each location/sourcing
            total_cost_weighted = 0
            total_vol_weighted = 0
            for q in bu_queues:
                loc = q.get('location', 'Onshore')
                src = q.get('sourcing', 'In-house')
                q_cost = cost_matrix.get(loc, {}).get(src, {}).get('costPerFTE', weighted_cost)
                total_cost_weighted += q['annual_volume'] * q_cost
                total_vol_weighted += q['annual_volume']
            bu_weighted_costs[bu] = total_cost_weighted / max(total_vol_weighted, 1)
        else:
            bu_weighted_costs[bu] = weighted_cost
        # Estimate FTE per BU from volume share
        bu_fte[bu] = round(total_fte * (bu_vol / max(total_volume, 1)), 1)
    
    pools = {}
    
    # ════════════════════════════════════════════
    # 1. DEFLECTION POOL
    # ════════════════════════════════════════════
    # Ceiling = Σ(Volume_i × DeflectionEligible_i × ContainmentFeasibility_i) across all intents
    # This represents the *achievable* deflection ceiling, not just eligible contacts.
    # Unit: contacts/year
    deflectable_contacts = 0
    deflection_breakdown = []
    for q in ann_queues:
        eligible_pct = q.get('deflection_eligible_pct', 0)
        containment = q.get('containment_feasibility', 0)
        # Achievable = eligible × how many can actually be contained
        achievable = q['annual_volume'] * eligible_pct * containment
        if achievable > 0:
            deflection_breakdown.append({
                'intent': q.get('intent', ''),
                'channel': q.get('channel', ''),
                'volume': q['annual_volume'],
                'eligible_pct': eligible_pct,
                'containment': containment,
                'achievable_contacts': round(achievable),
            })
        deflectable_contacts += achievable
    
    # Convert to FTE: deflected contacts × avg_handle_time → hours saved → FTE
    avg_aht_sec = sum(q['aht'] * 60 * q['annual_volume'] for q in ann_queues) / max(total_volume, 1)
    deflection_hours = (deflectable_contacts * avg_aht_sec) / 3600
    deflection_fte = deflection_hours / max(net_prod_hours, 1)
    
    pools['deflection'] = {
        'ceiling_contacts': round(deflectable_contacts),
        'ceiling_pct': round(deflectable_contacts / max(total_volume, 1), 4),
        'ceiling_fte': round(deflection_fte, 1),
        'ceiling_saving': round(deflection_fte * weighted_cost),
        'unit': 'contacts',
        'remaining_contacts': round(deflectable_contacts),  # consumed by waterfall
        'remaining_fte': round(deflection_fte, 1),
        'breakdown': sorted(deflection_breakdown, key=lambda x: x['achievable_contacts'], reverse=True)[:20],
    }
    
    # ════════════════════════════════════════════
    # 2. AHT REDUCTION POOL
    # ════════════════════════════════════════════
    # Only Search + Wrap seconds are reducible (talk time is value-add, hold is separate)
    # Ceiling = Σ(Volume_i × Reducible_seconds_i)
    total_reducible_seconds = 0
    aht_breakdown = []
    for q in ann_queues:
        decomp = q.get('aht_decomp', {})
        reducible = decomp.get('reducible_sec', 0)
        vol_reducible = q['annual_volume'] * reducible
        if vol_reducible > 0:
            aht_breakdown.append({
                'intent': q.get('intent', ''),
                'channel': q.get('channel', ''),
                'volume': q['annual_volume'],
                'search_sec': decomp.get('search_sec', 0),
                'wrap_sec': decomp.get('wrap_sec', 0),
                'reducible_sec': reducible,
                'total_reducible_hours': round(vol_reducible / 3600, 1),
            })
        total_reducible_seconds += vol_reducible
    
    aht_hours = total_reducible_seconds / 3600
    aht_fte = aht_hours / max(net_prod_hours, 1)
    
    pools['aht_reduction'] = {
        'ceiling_seconds': round(total_reducible_seconds),
        'ceiling_hours': round(aht_hours, 1),
        'ceiling_fte': round(aht_fte, 1),
        'ceiling_saving': round(aht_fte * weighted_cost),
        'unit': 'seconds',
        'remaining_seconds': round(total_reducible_seconds),
        'remaining_fte': round(aht_fte, 1),
        'breakdown': sorted(aht_breakdown, key=lambda x: x['total_reducible_hours'], reverse=True)[:20],
    }
    
    # ════════════════════════════════════════════
    # 3. TRANSFER / ESCALATION POOL
    # ════════════════════════════════════════════
    # Only preventable transfers are in the pool
    # Each preventable transfer costs ~2x AHT (original + receiving agent)
    total_preventable_transfers = 0
    transfer_breakdown = []
    total_preventable_escalations = 0
    escalation_breakdown = []
    
    for q in ann_queues:
        tc = q.get('transfer_class', {})
        prev_transfers = q['annual_volume'] * tc.get('preventable_rate', 0)
        if prev_transfers > 0:
            transfer_breakdown.append({
                'intent': q.get('intent', ''),
                'channel': q.get('channel', ''),
                'volume': q['annual_volume'],
                'preventable_rate': tc.get('preventable_rate', 0),
                'preventable_count': round(prev_transfers),
            })
        total_preventable_transfers += prev_transfers
        
        # Escalations (separate from transfers)
        esc_rate = q.get('escalation', 0)
        complexity = q.get('complexity', 0.5)
        # Preventable escalation share: inverse of complexity
        prev_esc_share = max(0.10, 0.60 - complexity * 0.50)
        prev_esc = q['annual_volume'] * esc_rate * prev_esc_share
        if prev_esc > 0:
            escalation_breakdown.append({
                'intent': q.get('intent', ''),
                'channel': q.get('channel', ''),
                'volume': q['annual_volume'],
                'escalation_rate': esc_rate,
                'preventable_count': round(prev_esc),
            })
        total_preventable_escalations += prev_esc
    
    # Transfer cost: each preventable transfer adds ~3 min extra handle time
    transfer_extra_sec = 180  # 3 min average per unnecessary transfer
    transfer_hours = (total_preventable_transfers * transfer_extra_sec) / 3600
    transfer_fte = transfer_hours / max(net_prod_hours, 1)
    
    pools['transfer_reduction'] = {
        'ceiling_contacts': round(total_preventable_transfers),
        'ceiling_fte': round(transfer_fte, 1),
        'ceiling_saving': round(transfer_fte * weighted_cost),
        'unit': 'transfers',
        'remaining_contacts': round(total_preventable_transfers),
        'remaining_fte': round(transfer_fte, 1),
        'breakdown': sorted(transfer_breakdown, key=lambda x: x['preventable_count'], reverse=True)[:20],
    }
    
    escalation_extra_sec = 300  # 5 min per unnecessary escalation
    esc_hours = (total_preventable_escalations * escalation_extra_sec) / 3600
    esc_fte = esc_hours / max(net_prod_hours, 1)
    
    pools['escalation_reduction'] = {
        'ceiling_contacts': round(total_preventable_escalations),
        'ceiling_fte': round(esc_fte, 1),
        'ceiling_saving': round(esc_fte * weighted_cost),
        'unit': 'escalations',
        'remaining_contacts': round(total_preventable_escalations),
        'remaining_fte': round(esc_fte, 1),
        'breakdown': sorted(escalation_breakdown, key=lambda x: x['preventable_count'], reverse=True)[:20],
    }
    
    # ════════════════════════════════════════════
    # 4. REPEAT / FCR POOL
    # ════════════════════════════════════════════
    # Repeat contacts that could be eliminated with better FCR
    # V6 fix: If raw repeat rates are implausibly low (short CCaaS sample), 
    # derive from FCR gap — same logic as gross.py
    total_repeat_contacts = 0
    repeat_breakdown = []
    
    weighted_repeat = sum(q.get('repeat', 0) * q['annual_volume'] for q in ann_queues) / max(total_volume, 1)
    REPEAT_FLOOR = 0.02
    use_repeat_fallback = weighted_repeat < REPEAT_FLOOR
    if use_repeat_fallback:
        weighted_fcr = sum(q.get('fcr', 0.75) * q['annual_volume'] for q in ann_queues) / max(total_volume, 1)
        fallback_repeat = max(0.05, (1 - weighted_fcr) * 0.60)
    
    for q in ann_queues:
        repeat_rate = fallback_repeat if use_repeat_fallback else q.get('repeat', 0)
        fcr = q.get('fcr', 0.70)
        # Reducible repeat = repeat_rate × (1 - FCR floor)
        # FCR floor ~ 0.85 for simple, 0.70 for complex
        fcr_target = 0.90 - q.get('complexity', 0.4) * 0.15
        fcr_gap = max(0, fcr_target - fcr)
        reducible_repeat = q['annual_volume'] * repeat_rate * min(1.0, fcr_gap / max(repeat_rate, 0.01))
        reducible_repeat = min(reducible_repeat, q['annual_volume'] * repeat_rate * 0.70)  # Can't fix more than 70% of repeats
        
        if reducible_repeat > 0:
            repeat_breakdown.append({
                'intent': q.get('intent', ''),
                'channel': q.get('channel', ''),
                'volume': q['annual_volume'],
                'repeat_rate': repeat_rate,
                'reducible_contacts': round(reducible_repeat),
            })
        total_repeat_contacts += reducible_repeat
    
    repeat_hours = (total_repeat_contacts * avg_aht_sec) / 3600
    repeat_fte = repeat_hours / max(net_prod_hours, 1)
    
    pools['repeat_reduction'] = {
        'ceiling_contacts': round(total_repeat_contacts),
        'ceiling_fte': round(repeat_fte, 1),
        'ceiling_saving': round(repeat_fte * weighted_cost),
        'unit': 'contacts',
        'remaining_contacts': round(total_repeat_contacts),
        'remaining_fte': round(repeat_fte, 1),
        'breakdown': sorted(repeat_breakdown, key=lambda x: x['reducible_contacts'], reverse=True)[:20],
    }
    
    # ════════════════════════════════════════════
    # 5. LOCATION POOL
    # ════════════════════════════════════════════
    # FTE that could be migrated to lower-cost locations (no workload reduction)
    # Based on migration readiness of the volume they handle
    migratable_volume = sum(q['annual_volume'] * q.get('migration_readiness', 0) for q in ann_queues)
    migratable_share = migratable_volume / max(total_volume, 1)
    
    # Only certain roles can be migrated
    migratable_roles = ['Agent L1', 'Agent L2 / Specialist', 'Back-Office / Processing']
    migratable_fte = sum(r['headcount'] for r in roles if r['role'] in migratable_roles)
    migratable_fte_adjusted = migratable_fte * migratable_share
    
    # Cost arbitrage: typically 30-50% cost saving per migrated FTE
    cost_arbitrage = params.get('locationArbitrage', 0.35)
    location_saving = migratable_fte_adjusted * weighted_cost * cost_arbitrage
    
    pools['location'] = {
        'ceiling_fte': round(migratable_fte_adjusted, 1),
        'ceiling_saving': round(location_saving),
        'migratable_share': round(migratable_share, 3),
        'cost_arbitrage': cost_arbitrage,
        'unit': 'fte',
        'remaining_fte': round(migratable_fte_adjusted, 1),
        'remaining_saving': round(location_saving),
    }
    
    # ════════════════════════════════════════════
    # 6. SHRINKAGE POOL
    # ════════════════════════════════════════════
    # Shrinkage improvement potential
    current_shrinkage = shrinkage
    best_practice_shrinkage = params.get('targetShrinkage', 0.22)
    shrinkage_gap = max(0, current_shrinkage - best_practice_shrinkage)
    
    # FTE equivalent of shrinkage reduction
    shrinkage_fte = total_fte * shrinkage_gap
    shrinkage_saving = shrinkage_fte * weighted_cost
    
    pools['shrinkage_reduction'] = {
        'current_shrinkage': round(current_shrinkage, 3),
        'target_shrinkage': round(best_practice_shrinkage, 3),
        'gap': round(shrinkage_gap, 3),
        'ceiling_fte': round(shrinkage_fte, 1),
        'ceiling_saving': round(shrinkage_saving),
        'unit': 'fte',
        'remaining_fte': round(shrinkage_fte, 1),
        'remaining_saving': round(shrinkage_saving),
    }
    
    # ════════════════════════════════════════════
    # 7. CSAT / CUSTOMER EXPERIENCE POOL
    # ════════════════════════════════════════════
    # Unlike FTE pools, the CSAT pool measures opportunity in CSAT points
    # and converts to revenue impact via retention/CLV research.
    #
    # Sources:
    #   Industry benchmark: 10% CSAT increase → 2-3% revenue growth
    #   Industry research: 5% retention improvement → 25-95% profit increase
    #   Teneo.ai / industry: 1% FCR increase ≈ 1% CSAT increase
    #   HBR: Best-experience customers spend 140% more (transaction) /
    #         74% vs 43% retention (subscription)
    
    benchmarks = params.get('_benchmarks_defaults', {})
    csat_benchmarks = benchmarks.get('CSAT', {})
    default_csat_benchmark = csat_benchmarks.get('_default', 3.8)
    
    # Per intent×channel CSAT gap
    csat_breakdown = []
    total_weighted_gap = 0.0
    total_weighted_current = 0.0
    total_weighted_benchmark = 0.0
    
    for q in ann_queues:
        current_csat = q.get('csat', 0)
        if current_csat <= 0:
            continue
        channel = q.get('channel', '')
        # Channel-specific benchmark (Voice=4.0, Chat=4.0, Email=3.8, IVR=3.5)
        ch_benchmark = csat_benchmarks.get(channel, default_csat_benchmark)
        gap = max(0, ch_benchmark - current_csat)
        vol_share = q['annual_volume'] / max(total_volume, 1)
        weighted_gap = gap * vol_share
        
        total_weighted_gap += weighted_gap
        total_weighted_current += current_csat * vol_share
        total_weighted_benchmark += ch_benchmark * vol_share
        
        if gap > 0.05:
            csat_breakdown.append({
                'intent': q.get('intent', ''),
                'channel': channel,
                'volume': q['annual_volume'],
                'current_csat': round(current_csat, 2),
                'benchmark_csat': round(ch_benchmark, 2),
                'gap': round(gap, 2),
                'weighted_gap': round(weighted_gap, 4),
                'vol_share_pct': round(vol_share * 100, 1),
            })
    
    # Override with explicit csatTarget if available (consultant-set)
    csat_target = params.get('csatTarget', 0)
    if csat_target > 0 and csat_target > total_weighted_current:
        portfolio_csat_gap = csat_target - total_weighted_current
    else:
        portfolio_csat_gap = total_weighted_gap
    
    # Revenue/retention monetisation
    customer_base = params.get('customerBase', 0)
    revenue_per_customer = params.get('revenuePerCustomer', 0)
    annual_churn = params.get('annualChurnRate', 0.12)
    total_revenue = customer_base * revenue_per_customer if customer_base > 0 else 0
    
    # Industry multiplier: 10% CSAT improvement → 2.5% revenue (midpoint of 2-3%)
    # On a 5-point scale, "10% CSAT" ≈ 0.5 points. So 1 point ≈ 5% revenue.
    revenue_per_csat_point = total_revenue * 0.05 if total_revenue > 0 else (
        # Fallback: use contact centre cost base as proxy (8% per point)
        sum(r['headcount'] * r['costPerFTE'] for r in roles) * 0.08
    )
    
    # Retention link: 1 CSAT point → ~2.5% retention improvement
    # (derived from: 5% retention → 25-95% profit; conservative end)
    retention_per_csat_point = 0.025
    churn_base = customer_base * annual_churn if customer_base > 0 else 0
    clv = revenue_per_customer * 3 if revenue_per_customer > 0 else 0  # 3-year CLV proxy
    
    # Total CX ceiling = revenue impact + retention value
    ceiling_revenue = portfolio_csat_gap * revenue_per_csat_point
    ceiling_retention_value = portfolio_csat_gap * retention_per_csat_point * churn_base * clv if churn_base > 0 else 0
    ceiling_total_value = ceiling_revenue + ceiling_retention_value
    
    pools['csat_experience'] = {
        'ceiling_csat_points': round(portfolio_csat_gap, 3),
        'ceiling_revenue': round(ceiling_revenue),
        'ceiling_retention_value': round(ceiling_retention_value),
        'ceiling_total_value': round(ceiling_total_value),
        'ceiling_fte': 0,  # Not FTE-denominated — keeps pool summary clean
        'ceiling_saving': round(ceiling_total_value),  # For summary aggregation
        'unit': 'csat_points',
        'current_csat': round(total_weighted_current, 2),
        'benchmark_csat': round(total_weighted_benchmark, 2),
        'portfolio_gap': round(portfolio_csat_gap, 3),
        'revenue_per_point': round(revenue_per_csat_point),
        'retention_per_point': round(retention_per_csat_point, 4),
        'customer_base': customer_base,
        'annual_churn': round(annual_churn, 3),
        # Consumption tracking (updated by waterfall)
        'consumed_csat_points': 0.0,
        'consumed_revenue': 0,
        'remaining_csat_points': round(portfolio_csat_gap, 3),
        'remaining_revenue': round(ceiling_total_value),
        'breakdown': sorted(csat_breakdown, key=lambda x: x['weighted_gap'], reverse=True)[:20],
    }
    
    # ════════════════════════════════════════════
    # P2-1: PER-BU POOL BREAKDOWN
    # ════════════════════════════════════════════
    # Compute each BU's contribution to each pool ceiling.
    # This doesn't change global pools — it provides dimensional visibility.
    for pool_name in ['deflection', 'aht_reduction', 'transfer_reduction',
                      'escalation_reduction', 'repeat_reduction', 'location', 'shrinkage_reduction']:
        pool = pools.get(pool_name)
        if not pool:
            continue
        pool['by_bu'] = {}
        for bu in bus:
            bu_queues = [q for q in ann_queues if q.get('bu') == bu]
            if not bu_queues:
                continue
            bu_vol = sum(q['annual_volume'] for q in bu_queues)
            bu_cost = bu_weighted_costs.get(bu, weighted_cost)

            if pool_name == 'deflection':
                bu_contacts = sum(q['annual_volume'] * q.get('deflection_eligible_pct', 0) * q.get('containment_feasibility', 0) for q in bu_queues)
                bu_avg_aht = sum(q['aht'] * 60 * q['annual_volume'] for q in bu_queues) / max(bu_vol, 1)
                bu_hrs = (bu_contacts * bu_avg_aht) / 3600
                bu_pool_fte = bu_hrs / max(net_prod_hours, 1)
                pool['by_bu'][bu] = {
                    'ceiling_contacts': round(bu_contacts),
                    'ceiling_fte': round(bu_pool_fte, 1),
                    'ceiling_saving': round(bu_pool_fte * bu_cost),
                    'remaining_fte': round(bu_pool_fte, 1),
                    'volume': bu_vol,
                }
            elif pool_name == 'aht_reduction':
                bu_secs = sum(q['annual_volume'] * q.get('aht_decomp', {}).get('reducible_sec', 0) for q in bu_queues)
                bu_hrs = bu_secs / 3600
                bu_pool_fte = bu_hrs / max(net_prod_hours, 1)
                pool['by_bu'][bu] = {
                    'ceiling_seconds': round(bu_secs),
                    'ceiling_fte': round(bu_pool_fte, 1),
                    'ceiling_saving': round(bu_pool_fte * bu_cost),
                    'remaining_fte': round(bu_pool_fte, 1),
                    'volume': bu_vol,
                }
            elif pool_name in ('transfer_reduction', 'escalation_reduction'):
                rate_key = 'transfer' if pool_name == 'transfer_reduction' else 'escalation'
                extra_sec = 180 if pool_name == 'transfer_reduction' else 300
                bu_preventable = 0
                for q in bu_queues:
                    rate = q.get(rate_key, 0)
                    cmplx = q.get('complexity', 0.5)
                    if pool_name == 'transfer_reduction':
                        tc = q.get('transfer_class', {})
                        prev = q['annual_volume'] * tc.get('preventable_rate', 0)
                    else:
                        prev_share = max(0.10, 0.60 - cmplx * 0.50)
                        prev = q['annual_volume'] * rate * prev_share
                    bu_preventable += prev
                bu_hrs = (bu_preventable * extra_sec) / 3600
                bu_pool_fte = bu_hrs / max(net_prod_hours, 1)
                pool['by_bu'][bu] = {
                    'ceiling_contacts': round(bu_preventable),
                    'ceiling_fte': round(bu_pool_fte, 1),
                    'ceiling_saving': round(bu_pool_fte * bu_cost),
                    'remaining_fte': round(bu_pool_fte, 1),
                    'volume': bu_vol,
                }
            elif pool_name == 'repeat_reduction':
                bu_repeat = 0
                for q in bu_queues:
                    rr = fallback_repeat if use_repeat_fallback else q.get('repeat', 0)
                    fcr = q.get('fcr', 0.70)
                    fcr_target = 0.90 - q.get('complexity', 0.4) * 0.15
                    fcr_gap = max(0, fcr_target - fcr)
                    red = q['annual_volume'] * rr * min(1.0, fcr_gap / max(rr, 0.01))
                    red = min(red, q['annual_volume'] * rr * 0.70)
                    bu_repeat += red
                bu_avg_aht = sum(q['aht'] * 60 * q['annual_volume'] for q in bu_queues) / max(bu_vol, 1)
                bu_hrs = (bu_repeat * bu_avg_aht) / 3600
                bu_pool_fte = bu_hrs / max(net_prod_hours, 1)
                pool['by_bu'][bu] = {
                    'ceiling_contacts': round(bu_repeat),
                    'ceiling_fte': round(bu_pool_fte, 1),
                    'ceiling_saving': round(bu_pool_fte * bu_cost),
                    'remaining_fte': round(bu_pool_fte, 1),
                    'volume': bu_vol,
                }
            elif pool_name == 'location':
                bu_mig_vol = sum(q['annual_volume'] * q.get('migration_readiness', 0) for q in bu_queues)
                bu_mig_share = bu_mig_vol / max(bu_vol, 1)
                migratable_roles_names = ['Agent L1', 'Agent L2 / Specialist', 'Back-Office / Processing']
                bu_mig_fte = bu_fte.get(bu, 0) * bu_mig_share * 0.6  # rough: 60% of BU FTE are migratable roles
                bu_arbitrage = params.get('locationArbitrage', 0.35)
                pool['by_bu'][bu] = {
                    'ceiling_fte': round(bu_mig_fte, 1),
                    'ceiling_saving': round(bu_mig_fte * bu_cost * bu_arbitrage),
                    'remaining_fte': round(bu_mig_fte, 1),
                    'volume': bu_vol,
                }
            elif pool_name == 'shrinkage_reduction':
                bu_share = bu_fte.get(bu, 0) / max(total_fte, 1)
                pool['by_bu'][bu] = {
                    'ceiling_fte': round(pool['ceiling_fte'] * bu_share, 1),
                    'ceiling_saving': round(pool['ceiling_saving'] * bu_share),
                    'remaining_fte': round(pool['ceiling_fte'] * bu_share, 1),
                    'volume': bu_vol,
                }

    # ════════════════════════════════════════════
    # SUMMARY
    # ════════════════════════════════════════════
    total_pool_fte = sum(p.get('ceiling_fte', 0) for p in pools.values())
    total_pool_saving = sum(p.get('ceiling_saving', 0) for p in pools.values())
    
    return {
        'pools': pools,
        'summary': {
            'total_pool_fte': round(total_pool_fte, 1),
            'total_pool_saving': round(total_pool_saving),
            'total_fte': total_fte,
            'total_volume': total_volume,
            'net_prod_hours': round(net_prod_hours, 1),
            'weighted_cost_per_fte': round(weighted_cost),
            'shrinkage': round(shrinkage, 3),
            # P2-1: Per-BU metadata
            'bus': bus,
            'bu_weighted_costs': {k: round(v) for k, v in bu_weighted_costs.items()},
            'bu_fte': bu_fte,
        },
        'annualization_factor': annualization,
    }


def consume_pool(pools, lever, amount_fte, amount_contacts=0, amount_seconds=0, bu=None):
    """
    Consume from a pool during waterfall netting.
    Returns the actual consumed amount (capped by remaining pool).
    
    P2-1: If bu is specified, also tracks consumption on the BU sub-pool.
    The global pool is ALWAYS consumed (it's the netting authority).
    BU sub-pool consumption is tracked for reporting only.
    
    Args:
        pools: the pools dict (modified in place)
        lever: which pool to consume from
        amount_fte: FTE requested
        amount_contacts: contacts requested (for deflection/repeat/transfer)
        amount_seconds: seconds requested (for AHT)
        bu: optional BU name for per-BU tracking
    
    Returns:
        dict with actual consumed amounts
    """
    # Map lever names to pool keys
    lever_pool_map = {
        'deflection': 'deflection',
        'aht_reduction': 'aht_reduction',
        'escalation_reduction': 'escalation_reduction',
        'transfer_reduction': 'transfer_reduction',
        'repeat_reduction': 'repeat_reduction',
        'cost_reduction': 'location',
        'shrinkage_reduction': 'shrinkage_reduction',
    }
    
    pool_key = lever_pool_map.get(lever, lever)
    pool = pools.get(pool_key)
    
    if pool is None:
        # Unknown lever — fail closed: no savings for unmodeled levers (v5 fix)
        import logging
        logging.warning(f"consume_pool: unknown lever '{lever}' — returning 0 (fail closed)")
        return {'consumed_fte': 0, 'capped': True, 'pool_exhausted': False, 'unknown_lever': True}
    
    remaining_fte = pool.get('remaining_fte', 0)
    
    if remaining_fte <= 0:
        return {'consumed_fte': 0, 'capped': True, 'pool_exhausted': True}
    
    # Cap by remaining pool
    actual_fte = min(amount_fte, remaining_fte)
    cap_ratio = actual_fte / max(amount_fte, 0.001)
    
    # Deduct from pool
    pool['remaining_fte'] = round(remaining_fte - actual_fte, 1)
    
    # Also deduct contacts/seconds proportionally
    if 'remaining_contacts' in pool and amount_contacts > 0:
        pool['remaining_contacts'] = max(0, round(pool['remaining_contacts'] - amount_contacts * cap_ratio))
    if 'remaining_seconds' in pool and amount_seconds > 0:
        pool['remaining_seconds'] = max(0, round(pool['remaining_seconds'] - amount_seconds * cap_ratio))
    if 'remaining_saving' in pool:
        pool['remaining_saving'] = max(0, round(pool['remaining_saving'] - actual_fte * pool.get('ceiling_saving', 0) / max(pool.get('ceiling_fte', 1), 0.1)))
    
    # P2-1: Track BU sub-pool consumption (reporting only — global is authority)
    if bu and 'by_bu' in pool and bu in pool['by_bu']:
        bu_pool = pool['by_bu'][bu]
        bu_remaining = bu_pool.get('remaining_fte', 0)
        bu_consumed = min(actual_fte, bu_remaining)
        bu_pool['remaining_fte'] = round(max(0, bu_remaining - bu_consumed), 1)
    
    return {
        'consumed_fte': round(actual_fte, 1),
        'consumed_contacts': round(amount_contacts * cap_ratio) if amount_contacts else 0,
        'consumed_seconds': round(amount_seconds * cap_ratio) if amount_seconds else 0,
        'capped': cap_ratio < 0.95,
        'pool_exhausted': pool.get('remaining_fte', 0) <= 0.5,
        'pool_remaining_fte': pool.get('remaining_fte', 0),
    }
