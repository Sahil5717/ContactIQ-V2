"""
ContactIQ — Risk Assessment Engine (P2-7)
3-axis risk scoring (Implementation, CX, Operational) with per-initiative
mitigations, dependency mapping, and dimensional awareness.
"""

# ── Implementation Risk Factors ──
# Technical complexity, integration scope, timeline pressure
def _impl_risk(init):
    score = 1.0
    # Effort / complexity
    eff = init.get('effort', 'medium').lower()
    score += {'high': 2.0, 'medium': 1.0, 'low': 0.3}.get(eff, 1.0)
    # Number of channels (integration breadth)
    nch = len(init.get('channels', []))
    score += min(1.5, (nch - 1) * 0.35)
    # Timeline pressure
    tl = init.get('timeline', 'medium_term')
    score += {'strategic': 1.2, 'medium_term': 0.5, 'quick_win': 0.0}.get(tl, 0.5)
    # Technology maturity (AI initiatives inherently riskier)
    lever = init.get('lever', '')
    if lever in ('deflection', 'aht_reduction') and 'AI' in init.get('name', ''):
        score += 0.5
    return min(5.0, round(score, 1))


# ── CX Risk Factors ──
# Customer experience disruption, adoption friction, quality degradation
def _cx_risk(init):
    score = 1.0
    # Adoption shortfall risk (lower adoption = higher CX risk from forced migration)
    adopt = init.get('adoption', 0.80)
    score += max(0, (1 - adopt) * 3.0)
    # Channel migration risk (moving customers to unfamiliar channels)
    lever = init.get('lever', '')
    if lever == 'deflection':
        score += 1.2  # Deflection = taking away human contact
    elif lever == 'channel_migration':
        score += 0.8
    # Volume of customers affected
    fte = init.get('_fteImpact', 0)
    if fte > 30:
        score += 0.5
    elif fte > 15:
        score += 0.3
    # Quality safeguard: if initiative touches high-complexity intents
    if init.get('_complexity_score', 0) > 0.6:
        score += 0.5
    return min(5.0, round(score, 1))


# ── Operational Risk Factors ──
# Workforce disruption, process change, dependency chains
def _ops_risk(init):
    score = 1.0
    # Number of roles affected
    nr = len(init.get('roles', []))
    score += min(1.5, (nr - 1) * 0.3)
    # FTE impact magnitude
    fte = init.get('_fteImpact', 0)
    if fte > 40:
        score += 1.5
    elif fte > 20:
        score += 1.0
    elif fte > 10:
        score += 0.5
    # Change management: multi-BU initiatives are harder
    target_bus = init.get('targetBUs', [])
    if len(target_bus) == 0:  # applies to all BUs
        score += 0.5
    elif len(target_bus) > 1:
        score += 0.3
    # Location risk: offshore transitions carry execution risk
    lever = init.get('lever', '')
    if lever == 'cost_reduction':
        score += 0.8  # Location moves = operational disruption
    # Dependency chains
    deps = init.get('dependencies', [])
    score += len(deps) * 0.25
    return min(5.0, round(score, 1))


# ── Mitigation Library ──
MITIGATION_LIBRARY = {
    'impl_high': [
        'Phase implementation: pilot in single BU/location before enterprise rollout',
        'Establish integration testing environment with production-like data early',
        'Assign dedicated technical lead with vendor management authority',
        'Build 20% timeline buffer and define abort criteria at each gate',
    ],
    'impl_medium': [
        'Weekly progress reviews with technical risk radar',
        'Set interim milestones with measurable acceptance criteria',
    ],
    'cx_high': [
        'Implement customer opt-out mechanism for first 90 days',
        'Deploy real-time CSAT monitoring with automatic escalation triggers',
        'Run A/B test with 10% traffic before full launch',
        'Establish CX quality floor — auto-revert if CSAT drops >5 points',
    ],
    'cx_medium': [
        'Monitor CSAT and FCR weekly during rollout',
        'Create fallback to human agent for all automated interactions',
    ],
    'ops_high': [
        'Appoint change champions in each affected team/location',
        'Secure executive sponsor with authority to resolve cross-BU conflicts',
        'Build contingency staffing plan for transition period',
        'Run tabletop exercise before go-live to test failure modes',
    ],
    'ops_medium': [
        'Communicate timeline and impact to affected teams 8 weeks before',
        'Conduct readiness assessment for each impacted role',
    ],
}


def _get_mitigations(impl, cx, ops):
    """Select mitigations based on risk axis scores."""
    mits = []
    if impl >= 3.5:
        mits.extend(MITIGATION_LIBRARY['impl_high'][:2])
    elif impl >= 2.5:
        mits.extend(MITIGATION_LIBRARY['impl_medium'][:1])
    if cx >= 3.5:
        mits.extend(MITIGATION_LIBRARY['cx_high'][:2])
    elif cx >= 2.5:
        mits.extend(MITIGATION_LIBRARY['cx_medium'][:1])
    if ops >= 3.5:
        mits.extend(MITIGATION_LIBRARY['ops_high'][:2])
    elif ops >= 2.5:
        mits.extend(MITIGATION_LIBRARY['ops_medium'][:1])
    if not mits:
        mits.append('Standard project governance — no elevated risk controls needed')
    return mits


# ── Dependency Map ──
# Which initiatives depend on others being implemented first
DEPENDENCY_MAP = {
    # AI dependencies
    'AI01': ['CC01'],                 # Virtual Agent needs CCaaS platform for NLU
    'AI02': ['AI08'],                 # Agent Assist needs Knowledge Base for context
    'AI03': ['CC01'],                 # IVR Upgrade needs CCaaS platform
    'AI05': ['AI22'],                 # Predictive Routing needs Customer 360 data
    'AI06': ['AI21'],                 # Sentiment Analysis needs Speech Analytics
    'AI09': ['AI01'],                 # WhatsApp Bot reuses Virtual Agent NLU engine
    'AI10': ['AI04'],                 # Social Auto-Response shares Email Auto-Response logic
    'AI14': ['AI03'],                 # Visual IVR needs IVR Upgrade first
    'AI15': ['AI06'],                 # Complaint Triage needs Sentiment Analysis
    'AI16': ['AI22'],                 # Churn Detection needs Customer 360 data
    'AI17': ['CC01'],                 # Auto Summarisation needs CCaaS integration
    'AI18': ['AI08'],                 # Self-Service App needs Knowledge Base
    'AI20': ['AI21'],                 # AI Training needs Speech Analytics data
    'AI22': ['CC01'],                 # Customer 360 needs CCaaS integration
    'AI25': ['AI08'],                 # Smart FAQ needs Knowledge Base AI Search
    'AI27': ['AI22', 'AI02'],         # Next-Best-Action needs Customer 360 + Agent Assist
    # CCaaS dependencies
    'CC02': ['CC01'],                 # Omnichannel Routing needs CCaaS Platform
    'CC03': ['CC01'],                 # CCaaS Analytics needs CCaaS Platform
    # Operating Model dependencies
    'OP02': ['OP01'],                 # Cross-skilling needs Tiered Service Model
    'OP03': ['CC02'],                 # Queue Consolidation needs Omnichannel Routing
    'OP07': ['AI08'],                 # FCR Improvement needs Knowledge Base
    'OP08': ['OP01'],                 # Escalation Redesign needs Tiered Service Model
    'OP09': ['AI08'],                 # Knowledge Management Overhaul needs AI Knowledge Base
    # Location dependencies
    'LS02': ['LS01'],                 # Offshore Expansion benefits from Nearshore Hub experience
}


def run_risk(initiatives, data):
    """
    Compute 3-axis risk scores for each enabled initiative.
    
    Returns:
        dict with:
          initiatives: list of per-initiative risk assessments
          summary: high/medium/low counts and avg risk
          dependencies: dependency map for enabled initiatives
          riskByLayer: average risk per layer
          riskByBU: risk exposure by BU (based on targetBUs)
    """
    results = []
    for init in initiatives:
        if not init.get('enabled'):
            continue
        
        impl = _impl_risk(init)
        cx = _cx_risk(init)
        ops = _ops_risk(init)
        overall = round((impl * 0.35 + cx * 0.30 + ops * 0.35), 1)
        rating = 'critical' if overall > 4.0 else 'high' if overall > 3.0 else 'medium' if overall > 2.0 else 'low'
        
        mitigations = _get_mitigations(impl, cx, ops)
        
        # Dependencies
        deps = DEPENDENCY_MAP.get(init['id'], [])
        enabled_ids = {i['id'] for i in initiatives if i.get('enabled')}
        unmet_deps = [d for d in deps if d not in enabled_ids]
        if unmet_deps:
            mitigations.insert(0, f"Dependency warning: {', '.join(unmet_deps)} not in scope — may limit effectiveness")
        
        results.append({
            'id': init['id'],
            'name': init['name'],
            'layer': init.get('layer', ''),
            'lever': init.get('lever', ''),
            'implRisk': impl,
            'cxRisk': cx,
            'opsRisk': ops,
            'overallRisk': overall,
            'rating': rating,
            'scores': {
                'complexity': impl,
                'adoption': round(max(0, (1 - init.get('adoption', 0.80)) * 5), 1),
                'integration': round(min(5, len(init.get('channels', [])) * 0.8), 1),
                'timeline': round({'strategic': 4.0, 'medium_term': 2.5, 'quick_win': 1.0}.get(init.get('timeline', 'medium_term'), 2.5), 1),
                'change': round(min(5, 1 + len(init.get('roles', [])) * 0.5), 1),
            },
            'mitigations': mitigations,
            'fteImpact': init.get('_fteImpact', 0),
            'annualSaving': init.get('_annualSaving', 0),
            'dependencies': deps,
            'unmetDependencies': unmet_deps,
            'targetBUs': init.get('targetBUs', []),
        })
    
    results.sort(key=lambda x: x['overallRisk'], reverse=True)
    
    # ── Summary ──
    n = max(len(results), 1)
    summary = {
        'critical': sum(1 for r in results if r['rating'] == 'critical'),
        'high': sum(1 for r in results if r['rating'] == 'high'),
        'medium': sum(1 for r in results if r['rating'] == 'medium'),
        'low': sum(1 for r in results if r['rating'] == 'low'),
        'avgRisk': round(sum(r['overallRisk'] for r in results) / n, 1),
        'totalAtRisk': round(sum(r['annualSaving'] for r in results if r['rating'] in ('high', 'critical'))),
    }
    
    # ── Risk by Layer ──
    risk_by_layer = {}
    for r in results:
        l = r['layer']
        if l not in risk_by_layer:
            risk_by_layer[l] = {'count': 0, 'sum': 0, 'high_count': 0}
        risk_by_layer[l]['count'] += 1
        risk_by_layer[l]['sum'] += r['overallRisk']
        if r['rating'] in ('high', 'critical'):
            risk_by_layer[l]['high_count'] += 1
    for l in risk_by_layer:
        risk_by_layer[l]['avg'] = round(risk_by_layer[l]['sum'] / max(risk_by_layer[l]['count'], 1), 1)
    
    # ── Risk exposure by BU ──
    risk_by_bu = {}
    bus = data.get('bus', [])
    for bu in bus:
        bu_inits = [r for r in results if not r['targetBUs'] or bu in r['targetBUs']]
        if bu_inits:
            risk_by_bu[bu] = {
                'count': len(bu_inits),
                'avgRisk': round(sum(r['overallRisk'] for r in bu_inits) / len(bu_inits), 1),
                'savingAtRisk': round(sum(r['annualSaving'] for r in bu_inits if r['rating'] in ('high', 'critical'))),
                'highCount': sum(1 for r in bu_inits if r['rating'] in ('high', 'critical')),
            }
    
    # ── Dependency chains ──
    dep_chains = {}
    for r in results:
        if r['dependencies']:
            dep_chains[r['id']] = {
                'name': r['name'],
                'depends_on': r['dependencies'],
                'unmet': r['unmetDependencies'],
                'blocked': len(r['unmetDependencies']) > 0,
            }
    
    return {
        'initiatives': results,
        'summary': summary,
        'riskByLayer': risk_by_layer,
        'riskByBU': risk_by_bu,
        'dependencies': dep_chains,
    }
