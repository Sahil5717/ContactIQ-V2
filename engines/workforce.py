"""
ContactIQ — Workforce Transition Engine (P2-2)
Reskilling matrix, redeployment planning, location-aware transition planning,
and per-BU workforce impact.
"""

RESKILL_PATHS = {
    'Agent L1': [
        {'target':'Chat/Digital Agent','effort':'low','duration':4,'skills':['Digital literacy','Chat etiquette','Multi-tasking']},
        {'target':'AI Bot Trainer','effort':'medium','duration':8,'skills':['NLP basics','Intent design','Testing methodology']},
        {'target':'QA Analyst','effort':'medium','duration':12,'skills':['Quality frameworks','Calibration','Coaching']},
    ],
    'Agent L2 / Specialist': [
        {'target':'AI Operations Specialist','effort':'medium','duration':8,'skills':['AI monitoring','Exception handling','Escalation design']},
        {'target':'Knowledge Manager','effort':'medium','duration':6,'skills':['Content curation','Taxonomy design','Analytics']},
        {'target':'Process Analyst','effort':'high','duration':12,'skills':['Process mapping','Lean Six Sigma','Data analysis']},
    ],
    'Agent L3 / Expert': [
        {'target':'Solution Architect','effort':'high','duration':12,'skills':['System design','Integration patterns','Vendor management']},
        {'target':'CX Strategy Lead','effort':'high','duration':12,'skills':['Journey mapping','VoC analytics','Design thinking']},
    ],
    'Supervisor / Team Lead': [
        {'target':'Digital Operations Manager','effort':'medium','duration':8,'skills':['Digital KPIs','AI oversight','Change management']},
        {'target':'WFM Manager','effort':'medium','duration':6,'skills':['Forecasting','Scheduling','Analytics']},
    ],
    'Back-Office / Processing': [
        {'target':'RPA Developer','effort':'high','duration':12,'skills':['RPA tools','Process analysis','Testing']},
        {'target':'Data Entry Automation Analyst','effort':'medium','duration':6,'skills':['Automation tools','Quality checking','Reporting']},
    ],
    'QA Analyst': [
        {'target':'AI Quality Lead','effort':'medium','duration':6,'skills':['AI output evaluation','Prompt QA','Bias detection']},
        {'target':'CX Insights Analyst','effort':'medium','duration':8,'skills':['VoC analytics','Sentiment analysis','Reporting']},
    ],
    'WFM Analyst': [
        {'target':'AI Capacity Planner','effort':'medium','duration':6,'skills':['AI demand forecasting','Bot capacity','Hybrid scheduling']},
    ],
    'Trainer': [
        {'target':'AI Learning Designer','effort':'medium','duration':8,'skills':['LMS design','AI tool training','Change adoption']},
    ],
    'Knowledge Manager': [
        {'target':'AI Knowledge Architect','effort':'medium','duration':8,'skills':['Knowledge graph design','RAG optimization','Content strategy']},
    ],
    'Reporting / Analytics': [
        {'target':'Data Engineer','effort':'high','duration':12,'skills':['ETL pipelines','SQL/Python','BI tooling']},
        {'target':'AI Analytics Lead','effort':'medium','duration':8,'skills':['AI dashboards','Predictive analytics','Stakeholder reporting']},
    ],
}

# Location-specific transition parameters
LOCATION_TRANSITION = {
    'Onshore': {'severance_multiplier': 1.0, 'notice_weeks': 8, 'redeployment_feasibility': 0.40, 'reskill_cost_multiplier': 1.0},
    'Nearshore': {'severance_multiplier': 0.60, 'notice_weeks': 4, 'redeployment_feasibility': 0.25, 'reskill_cost_multiplier': 0.70},
    'Offshore': {'severance_multiplier': 0.35, 'notice_weeks': 4, 'redeployment_feasibility': 0.15, 'reskill_cost_multiplier': 0.50},
}

# Sourcing-specific: outsourced FTE are vendor-managed (no severance, no reskill — contract adjustment)
SOURCING_TRANSITION = {
    'In-house': {'severance_applicable': True, 'reskill_applicable': True, 'contract_adjustment': False},
    'Outsourced': {'severance_applicable': False, 'reskill_applicable': False, 'contract_adjustment': True},
    'Managed Service': {'severance_applicable': False, 'reskill_applicable': False, 'contract_adjustment': True},
}


def run_workforce(data, waterfall, initiatives):
    """
    Compute workforce transition plan.
    
    P2-2 enhancements:
    - Location-aware severance and reskill costs
    - Per-BU workforce impact breakdown
    - Sourcing-aware transitions (in-house vs outsourced)
    - Expanded reskill paths for all 10 roles
    """
    roles = data['roles']; params = data['params']
    horizon = params.get('horizon', 3)
    role_impact = waterfall.get('roleImpact', {})
    cost_matrix = data.get('locationCostMatrix', {})
    
    # Base parameters
    redeployment_pct = params.get('redeploymentPct', 0.30)
    attrition_monthly = params.get('attritionMonthly', 0.015)
    annual_attrition = 1 - (1 - attrition_monthly) ** 12
    severance_pct = params.get('severancePct', 0.25)
    reskill_cost_base = params.get('reskillCostPerFTE', 5000)

    transitions = []
    by_location = {}  # location → {year → {reduction, attrited, redeployed, separated, cost}}
    by_bu = {}        # bu → {year → {reduction, separated, cost}}
    
    for r in roles:
        rn = r['role']; hc = r['headcount']; cost = r['costPerFTE']
        loc = r.get('location', 'Onshore')
        src = r.get('sourcing', 'In-house')
        ri = role_impact.get(rn, {'baseline': hc, 'yearly': [0] * horizon})
        yearly_red = ri.get('yearly', [0] * horizon)
        
        # Location-specific parameters
        loc_params = LOCATION_TRANSITION.get(loc, LOCATION_TRANSITION['Onshore'])
        src_params = SOURCING_TRANSITION.get(src, SOURCING_TRANSITION['In-house'])
        
        # Location-aware cost from cost matrix
        if cost_matrix:
            loc_cost = cost_matrix.get(loc, {}).get(src, {}).get('costPerFTE', cost)
        else:
            loc_cost = cost
        
        # Location-aware attrition (offshore/nearshore typically higher)
        loc_attrition_adj = cost_matrix.get(loc, {}).get(src, {}).get('attritionRate', attrition_monthly)
        loc_annual_attrition = 1 - (1 - loc_attrition_adj) ** 12

        for yr in range(horizon):
            red = yearly_red[yr] if yr < len(yearly_red) else 0
            if red <= 0.5:
                continue
            
            red = round(red)
            # CR-019: Cap attrition absorption at 60% of natural attrition
            # In practice, not all attrition can be perfectly captured as managed non-backfill;
            # some roles/skills won't align, requiring active transition pathways
            natural_attrition = round(hc * loc_annual_attrition)
            attrition_capture_rate = 0.60  # Realistic capture of natural attrition for managed reduction
            attrited = min(red, round(natural_attrition * attrition_capture_rate))
            remaining = max(0, red - attrited)
            
            if src_params['contract_adjustment']:
                # Outsourced/managed: no severance, no reskill — contract wind-down
                redeployed = 0
                separated = 0
                contract_adj = remaining
                sep_cost = 0
                reskill_cost = 0
                contract_cost = round(remaining * loc_cost * 0.10)  # 10% early termination
            else:
                # In-house: severance + reskill
                redep_rate = min(redeployment_pct, loc_params['redeployment_feasibility'])
                redeployed = round(remaining * redep_rate)
                separated = max(0, round(remaining - redeployed))
                contract_adj = 0
                
                sev_mult = loc_params['severance_multiplier']
                sep_cost = round(separated * loc_cost * severance_pct * sev_mult)
                
                rsk_mult = loc_params['reskill_cost_multiplier']
                reskill_cost = round(redeployed * reskill_cost_base * rsk_mult)
                contract_cost = 0
            
            paths = RESKILL_PATHS.get(rn, [{'target': 'General Reskill', 'effort': 'medium', 'duration': 6, 'skills': ['Transferable skills']}])
            
            t = {
                'role': rn, 'year': yr + 1, 'baseline': hc, 'reduction': red,
                'location': loc, 'sourcing': src,
                'attrited': attrited, 'redeployed': redeployed,
                'separated': separated, 'contractAdjustment': contract_adj,
                'separationCost': sep_cost, 'reskillCost': reskill_cost,
                'contractAdjustmentCost': contract_cost,
                'totalTransitionCost': sep_cost + reskill_cost + contract_cost,
                'noticeWeeks': loc_params['notice_weeks'],
                'reskillPaths': paths if redeployed > 0 else [],
            }
            transitions.append(t)
            
            # Aggregate by location
            if loc not in by_location:
                by_location[loc] = {yr + 1: {'reduction': 0, 'attrited': 0, 'redeployed': 0,
                                              'separated': 0, 'contractAdj': 0, 'cost': 0} for yr in range(horizon)}
            bl = by_location[loc].get(yr + 1, {'reduction': 0, 'attrited': 0, 'redeployed': 0, 'separated': 0, 'contractAdj': 0, 'cost': 0})
            bl['reduction'] += red; bl['attrited'] += attrited; bl['redeployed'] += redeployed
            bl['separated'] += separated; bl['contractAdj'] += contract_adj
            bl['cost'] += sep_cost + reskill_cost + contract_cost
            by_location[loc][yr + 1] = bl

    # ── Per-BU breakdown from waterfall buSummary ──
    bu_summary = waterfall.get('buSummary', {})
    for bu, bdata in bu_summary.items():
        by_bu[bu] = {}
        for ydata in bdata.get('yearly', []):
            yr = ydata['year']
            by_bu[bu][yr] = {
                'fteReduction': round(ydata.get('fteReduction', 0)),
                'annualSaving': round(ydata.get('annualSaving', 0)),
            }

    # ── Summary ──
    total_red = sum(t['reduction'] for t in transitions)
    total_attr = sum(t['attrited'] for t in transitions)
    total_redep = sum(t['redeployed'] for t in transitions)
    total_sep = sum(t['separated'] for t in transitions)
    total_contract = sum(t['contractAdjustment'] for t in transitions)
    total_sep_cost = sum(t['separationCost'] for t in transitions)
    total_reskill_cost = sum(t['reskillCost'] for t in transitions)
    total_contract_cost = sum(t['contractAdjustmentCost'] for t in transitions)

    # ── Reskill demand: how many people need each target role ──
    reskill_demand = {}
    for t in transitions:
        if t['redeployed'] > 0 and t['reskillPaths']:
            primary = t['reskillPaths'][0]
            target = primary['target']
            if target not in reskill_demand:
                reskill_demand[target] = {'count': 0, 'effort': primary['effort'], 'duration': primary['duration'], 'skills': primary['skills']}
            reskill_demand[target]['count'] += t['redeployed']

    return {
        'transitions': transitions,
        'summary': {
            'totalReduction': total_red, 'totalAttrited': total_attr,
            'totalRedeployed': total_redep, 'totalSeparated': total_sep,
            'totalContractAdjustment': total_contract,
            'totalSeparationCost': round(total_sep_cost),
            'totalReskillCost': round(total_reskill_cost),
            'totalContractCost': round(total_contract_cost),
            'totalTransitionCost': round(total_sep_cost + total_reskill_cost + total_contract_cost),
            'attritionRate': round(annual_attrition * 100, 1),
            'redeploymentRate': round(redeployment_pct * 100, 1),
        },
        'byLocation': by_location,
        'byBU': by_bu,
        'reskillMatrix': RESKILL_PATHS,
        'reskillDemand': reskill_demand,
    }
