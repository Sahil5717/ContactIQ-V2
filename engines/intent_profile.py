"""
ContactIQ — Intent Profile Enrichment Engine
Enriches each queue/intent with:
  - Deflection eligibility (repeatability, emotional risk, auth requirement, containment feasibility)
  - AHT decomposition (talk, hold, search, wrap) from single AHT+ACW values
  - Transfer classification (preventable vs structural)
  - Migration readiness (digital channel suitability)

These enrichments feed the opportunity pool calculations in pools.py.
When detailed data isn't available, heuristics are derived from complexity, channel, and volume patterns.
"""

# ── Complexity-based heuristic tables ──
# Maps intent complexity (0.0 = trivial, 1.0 = extremely complex) to profile attributes

def _repeatability_from_complexity(complexity):
    """Higher complexity = lower repeatability (fewer are simple repeat queries)."""
    if complexity <= 0.20: return 0.85
    if complexity <= 0.35: return 0.65
    if complexity <= 0.50: return 0.45
    if complexity <= 0.70: return 0.25
    return 0.10

def _emotional_risk_from_complexity(complexity, intent_name=''):
    """Estimate emotional risk. Complaints/disputes are high regardless of complexity."""
    name_lower = intent_name.lower() if intent_name else ''
    # Known high-emotion intents
    if any(kw in name_lower for kw in ['complaint', 'dispute', 'cancel', 'fraud', 'bereavement',
                                         'hardship', 'escalat', 'threat', 'legal']):
        return 0.85
    if any(kw in name_lower for kw in ['refund', 'overcharge', 'disconnect',
                                         'terminate', 'close account']):
        return 0.60
    # Complexity-based fallback
    if complexity <= 0.25: return 0.10
    if complexity <= 0.45: return 0.25
    if complexity <= 0.65: return 0.45
    return 0.65

def _auth_required_from_complexity(complexity, intent_name=''):
    """Estimate whether authentication is needed (blocks self-service deflection)."""
    name_lower = intent_name.lower() if intent_name else ''
    # Known no-auth intents
    if any(kw in name_lower for kw in ['faq', 'general', 'product info', 'hours', 'location',
                                         'status check', 'tracking', 'pricing']):
        return 0.10
    # High-auth: actions that change account state or move money
    if any(kw in name_lower for kw in ['account change', 'password', 'transfer funds',
                                         'address change', 'personal detail', 'close account']):
        return 0.85
    # Moderate-auth: billing/payment — auth needed but digital auth (OTP, app login) is routine
    if any(kw in name_lower for kw in ['payment', 'billing', 'transaction', 'refund']):
        return 0.45
    # Complexity-based fallback
    if complexity <= 0.25: return 0.20
    if complexity <= 0.50: return 0.45
    return 0.65

def _containment_feasibility(repeatability, emotional_risk, auth_required, complexity):
    """
    Containment feasibility = how likely a virtual agent can fully resolve this intent.
    High repeatability + low emotion + low auth = high containment.
    Auth is weighted in the base formula — no separate halving penalty.
    """
    # Weighted formula: repeatability matters most
    base = (repeatability * 0.40) + ((1 - emotional_risk) * 0.25) + ((1 - auth_required) * 0.25) + ((1 - complexity) * 0.10)
    return round(min(1.0, max(0.0, base)), 3)


def _decompose_aht(aht_minutes, acw_minutes, complexity):
    """
    Decompose total AHT into talk/hold/search/wrap components.
    
    Industry heuristics (validated against COPC benchmarks):
      - Talk time: 50-65% of handle time (higher for complex)
      - Hold time: 5-15% (higher for complex due to research)
      - Search/navigation time: 15-25% (the reducible component for AHT initiatives)  
      - Wrap/ACW: reported separately
    
    Returns dict with seconds for each component.
    """
    aht_sec = aht_minutes * 60
    acw_sec = acw_minutes * 60
    
    if complexity <= 0.25:
        talk_pct, hold_pct, search_pct = 0.55, 0.05, 0.25
    elif complexity <= 0.50:
        talk_pct, hold_pct, search_pct = 0.55, 0.10, 0.20
    elif complexity <= 0.70:
        talk_pct, hold_pct, search_pct = 0.55, 0.15, 0.18
    else:
        talk_pct, hold_pct, search_pct = 0.50, 0.20, 0.15
    
    # Remaining goes to "other" (system time, dead air, etc.)
    other_pct = max(0, 1.0 - talk_pct - hold_pct - search_pct)
    
    return {
        'talk_sec': round(aht_sec * talk_pct, 1),
        'hold_sec': round(aht_sec * hold_pct, 1),
        'search_sec': round(aht_sec * search_pct, 1),
        'other_sec': round(aht_sec * other_pct, 1),
        'wrap_sec': round(acw_sec, 1),
        'total_handle_sec': round(aht_sec + acw_sec, 1),
        # CR-030: Reducible now includes search + 80% of wrap + 15% of talk time.
        # AI copilots (agent assist, next-best-action, knowledge search) reduce talk
        # time by helping agents answer faster with fewer clarifying loops.
        # Hold time partially reducible too (faster system lookups).
        'reducible_sec': round(
            aht_sec * search_pct           # full search time
            + acw_sec * 0.80               # 80% of wrap (auto-summarization, auto-disposition)
            + aht_sec * talk_pct * 0.15    # 15% of talk (AI-assisted faster resolution)
            + aht_sec * hold_pct * 0.30    # 30% of hold (faster system lookups)
        , 1),
    }


def _transfer_classification(transfer_rate, escalation_rate, complexity):
    """
    Classify transfers as preventable vs structural.
    
    Preventable: agent didn't have info/skills/authority → fixable via training, tools, empowerment
    Structural: genuinely needs specialist → not reducible by frontline initiatives
    
    Heuristic: low-complexity intents with high transfer = mostly preventable.
    High-complexity with moderate transfer = mostly structural.
    """
    if transfer_rate <= 0:
        return {'total_rate': 0, 'preventable_rate': 0, 'structural_rate': 0, 'preventable_share': 0}
    
    if complexity <= 0.30:
        preventable_share = 0.75  # Most transfers on simple intents are preventable
    elif complexity <= 0.55:
        preventable_share = 0.50
    elif complexity <= 0.75:
        preventable_share = 0.30
    else:
        preventable_share = 0.15  # Complex intents genuinely need escalation
    
    # High escalation rate suggests structural complexity
    if escalation_rate > 0.15:
        preventable_share *= 0.70
    
    return {
        'total_rate': round(transfer_rate, 4),
        'preventable_rate': round(transfer_rate * preventable_share, 4),
        'structural_rate': round(transfer_rate * (1 - preventable_share), 4),
        'preventable_share': round(preventable_share, 3),
    }


def _migration_readiness(channel, complexity, emotional_risk, auth_required):
    """
    Assess how ready this intent is for digital channel migration.
    Voice intents with low complexity/emotion/auth are prime candidates.
    Already-digital channels score 0 (no migration needed).
    """
    # Already digital — no migration opportunity
    if channel in ('Chat', 'Email', 'App/Self-Service', 'SMS/WhatsApp', 'Social Media'):
        return 0.0
    
    # IVR is partially digital
    if channel == 'IVR':
        return 0.20 * (1 - complexity)
    
    # Voice and Retail are the migration candidates
    base = 0.80
    base -= complexity * 0.30        # Complex intents harder to migrate
    base -= emotional_risk * 0.25     # Emotional intents need human touch
    base -= auth_required * 0.15      # Auth adds friction to digital
    
    return round(max(0.0, min(1.0, base)), 3)


def enrich_intents(queues, params=None):
    """
    Main entry point: enrich each queue record with intent profile data.
    
    Args:
        queues: list of queue dicts from data_loader (must have intent, channel, 
                volume, aht, acw, complexity, transfer, escalation, repeat, fcr)
        params: optional params dict (for shrinkage, productive hours)
    
    Returns:
        list of enriched queue dicts (original fields preserved, new fields added)
    """
    if params is None:
        params = {}
    
    enriched = []
    for q in queues:
        eq = dict(q)  # preserve original
        
        complexity = q.get('complexity', 0.40)
        intent_name = q.get('intent', '')
        channel = q.get('channel', 'Voice')
        aht = q.get('aht', 5.0)
        acw = q.get('acw', 1.0)
        transfer_rate = q.get('transfer', 0.0)
        escalation_rate = q.get('escalation', 0.0)
        repeat_rate = q.get('repeat', 0.0)
        
        # ── Deflection eligibility ──
        repeatability = _repeatability_from_complexity(complexity)
        # Boost repeatability if we have actual repeat rate data
        if repeat_rate > 0.15:
            repeatability = min(1.0, repeatability + 0.15)
        
        emotional_risk = _emotional_risk_from_complexity(complexity, intent_name)
        auth_required = _auth_required_from_complexity(complexity, intent_name)
        containment = _containment_feasibility(repeatability, emotional_risk, auth_required, complexity)
        
        eq['repeatability'] = round(repeatability, 3)
        eq['emotional_risk'] = round(emotional_risk, 3)
        eq['auth_required'] = round(auth_required, 3)
        eq['containment_feasibility'] = containment
        
        # Deflection eligibility: % of this intent's volume addressable for deflection
        # V9 fix: Auth penalty removed from eligibility — handled solely in containment_feasibility.
        # Eligible = repeatable contacts (auth is a containment constraint, not an eligibility one).
        eq['deflection_eligible_pct'] = round(repeatability, 3)
        
        # ── AHT decomposition ──
        aht_decomp = _decompose_aht(aht, acw, complexity)
        eq['aht_decomp'] = aht_decomp
        
        # ── Transfer classification ──
        transfer_class = _transfer_classification(transfer_rate, escalation_rate, complexity)
        eq['transfer_class'] = transfer_class
        
        # ── Migration readiness (channel migration: voice→digital) ──
        eq['migration_readiness'] = _migration_readiness(channel, complexity, emotional_risk, auth_required)
        
        # ── CR-FIX-LOC: Location readiness (offshoring suitability) ──
        # Separate concept from migration_readiness: how easily can THIS work be geographically relocated?
        _loc_base = {'Email':0.85,'Chat':0.80,'SMS/WhatsApp':0.75,'Social Media':0.70,
                     'App/Self-Service':0.60,'IVR':0.50,'Voice':0.55}.get(channel, 0.50)
        _loc_base -= complexity * 0.25
        _loc_base -= emotional_risk * 0.20
        eq['location_readiness'] = round(max(0.05, min(1.0, _loc_base)), 3)
        
        enriched.append(eq)
    
    return enriched


def compute_intent_summary(enriched_queues):
    """
    Aggregate intent profiles into summary statistics for display.
    """
    total_vol = sum(q['volume'] for q in enriched_queues)
    if total_vol == 0:
        return {'totalVolume': 0, 'deflectableVolume': 0, 'deflectablePct': 0,
                'avgContainment': 0, 'avgEmotionalRisk': 0, 'migratableVolume': 0}
    
    deflectable_vol = sum(q['volume'] * q['deflection_eligible_pct'] for q in enriched_queues)
    migratable_vol = sum(q['volume'] * q['migration_readiness'] for q in enriched_queues)
    avg_containment = sum(q['containment_feasibility'] * q['volume'] for q in enriched_queues) / total_vol
    avg_emotion = sum(q['emotional_risk'] * q['volume'] for q in enriched_queues) / total_vol
    
    return {
        'totalVolume': round(total_vol),
        'deflectableVolume': round(deflectable_vol),
        'deflectablePct': round(deflectable_vol / total_vol, 3),
        'avgContainment': round(avg_containment, 3),
        'avgEmotionalRisk': round(avg_emotion, 3),
        'migratableVolume': round(migratable_vol),
        'migratablePct': round(migratable_vol / total_vol, 3),
    }
