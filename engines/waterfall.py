"""
ContactIQ — Initiative Library + Pool-Based Waterfall Engine
58 initiatives across 3 layers with relevance scoring,
pool-based netting (no double-counting), and validated financial projections.

v4 Architecture:
  - Intent enrichment (intent_profile.py) → Opportunity pools (pools.py)
  - Lever-specific gross impact (gross.py) → Pool consumption netting (this file)
  - Each initiative consumes from finite pools; net = min(gross, remaining_pool)
  
CR-003: Phased ramp-up model (S-curve: 30% Y1, 70% Y2, 95% Y3)
CR-013: Per-initiative ramp-up %, queue-level granularity
CR-014: Industry-benchmarked FTE reduction caps per initiative type
CR-015v2: Pool-based netting replaces exponential diminishing returns
CR-018: Working scenario comparison and sensitivity tornado
"""
import math
from engines.intent_profile import enrich_intents
from engines.pools import compute_pools, consume_pool
from engines.gross import compute_gross_impact, compute_secondary_impacts

# ── 58 Initiative Library ──
INITIATIVE_LIBRARY = [
    # Layer 1: AI & Automation (28)
    {'id':'AI01','name':'Conversational Virtual Agent','layer':'AI & Automation','lever':'deflection','impact':0.30,'channels':['Voice','Chat'],'complexity':'simple','effort':'high','ahtImpact':0,'fcrImpact':0.05,'csatImpact':0.10,'roles':['Agent L1'],'ramp':9,'adoption':0.80},
    {'id':'AI02','name':'AI Agent Assist (Real-time)','layer':'AI & Automation','lever':'aht_reduction','impact':0.20,'channels':['Voice','Chat'],'complexity':'any','effort':'medium','ahtImpact':-0.20,'fcrImpact':0.08,'csatImpact':0.05,'roles':['Agent L1','Agent L2 / Specialist'],'ramp':6,'adoption':0.80},
    {'id':'AI03','name':'Intelligent IVR Upgrade','layer':'AI & Automation','lever':'deflection','impact':0.25,'channels':['IVR'],'complexity':'simple','effort':'medium','ahtImpact':-0.15,'fcrImpact':0.03,'csatImpact':0.05,'roles':['Agent L1'],'ramp':6,'adoption':0.75},
    {'id':'AI04','name':'Email Auto-Response','layer':'AI & Automation','lever':'deflection','impact':0.35,'channels':['Email'],'complexity':'simple','effort':'medium','ahtImpact':-0.30,'fcrImpact':0.05,'csatImpact':0.03,'roles':['Agent L1','Back-Office / Processing'],'ramp':6,'adoption':0.80},
    {'id':'AI05','name':'Predictive Intent Routing','layer':'AI & Automation','lever':'aht_reduction','impact':0.12,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'medium','ahtImpact':-0.12,'fcrImpact':0.10,'csatImpact':0.08,'roles':['Agent L1','Agent L2 / Specialist'],'ramp':6,'adoption':0.85},
    {'id':'AI06','name':'Sentiment Analysis & Escalation','layer':'AI & Automation','lever':'escalation_reduction','impact':0.25,'channels':['Voice','Chat'],'complexity':'any','effort':'low','ahtImpact':0,'fcrImpact':0.05,'csatImpact':0.12,'roles':['Supervisor / Team Lead'],'ramp':3,'adoption':0.90},
    {'id':'AI07','name':'Automated QA Scoring','layer':'AI & Automation','lever':'aht_reduction','impact':0.05,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'low','ahtImpact':0,'fcrImpact':0.03,'csatImpact':0.02,'roles':['QA Analyst'],'ramp':3,'adoption':0.90},
    {'id':'AI08','name':'Knowledge Base AI Search','layer':'AI & Automation','lever':'aht_reduction','impact':0.15,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'low','ahtImpact':-0.15,'fcrImpact':0.07,'csatImpact':0.03,'roles':['Agent L1','Agent L2 / Specialist','Knowledge Manager'],'ramp':3,'adoption':0.85},
    {'id':'AI09','name':'WhatsApp Bot','layer':'AI & Automation','lever':'deflection','impact':0.20,'channels':['SMS/WhatsApp'],'complexity':'simple','effort':'medium','ahtImpact':0,'fcrImpact':0.03,'csatImpact':0.05,'roles':['Agent L1'],'ramp':6,'adoption':0.70},
    {'id':'AI10','name':'Social Media Auto-Response','layer':'AI & Automation','lever':'deflection','impact':0.15,'channels':['Social Media'],'complexity':'simple','effort':'low','ahtImpact':0,'fcrImpact':0.02,'csatImpact':0.03,'roles':['Agent L1'],'ramp':3,'adoption':0.70},
    {'id':'AI11','name':'Proactive Outbound Notifications','layer':'AI & Automation','lever':'deflection','impact':0.10,'channels':['SMS/WhatsApp','Email'],'complexity':'simple','effort':'low','ahtImpact':0,'fcrImpact':0.05,'csatImpact':0.08,'roles':['Agent L1'],'ramp':3,'adoption':0.85},
    {'id':'AI12','name':'AI-Powered WFM Scheduling','layer':'AI & Automation','lever':'shrinkage_reduction','impact':0.10,'channels':['Voice','Chat'],'complexity':'any','effort':'medium','ahtImpact':0,'fcrImpact':0,'csatImpact':0,'roles':['WFM Analyst'],'ramp':6,'adoption':0.80},
    {'id':'AI13','name':'Robotic Process Automation','layer':'AI & Automation','lever':'aht_reduction','impact':0.25,'channels':['Email'],'complexity':'simple','effort':'medium','ahtImpact':-0.25,'fcrImpact':0.05,'csatImpact':0.02,'roles':['Back-Office / Processing'],'ramp':6,'adoption':0.80},
    {'id':'AI14','name':'Visual IVR / Digital Switchboard','layer':'AI & Automation','lever':'deflection','impact':0.15,'channels':['IVR','App/Self-Service'],'complexity':'simple','effort':'medium','ahtImpact':-0.10,'fcrImpact':0.05,'csatImpact':0.10,'roles':['Agent L1'],'ramp':6,'adoption':0.70},
    {'id':'AI15','name':'AI Complaint Triage','layer':'AI & Automation','lever':'escalation_reduction','impact':0.20,'channels':['Voice','Chat','Email'],'complexity':'complex','effort':'medium','ahtImpact':-0.10,'fcrImpact':0.08,'csatImpact':0.10,'roles':['Agent L2 / Specialist','Agent L3 / Expert'],'ramp':6,'adoption':0.75},
    {'id':'AI16','name':'Predictive Churn Detection','layer':'AI & Automation','lever':'deflection','impact':0.05,'channels':['Voice','Chat'],'complexity':'any','effort':'medium','ahtImpact':0,'fcrImpact':0.03,'csatImpact':0.15,'roles':['Agent L2 / Specialist'],'ramp':6,'adoption':0.75},
    {'id':'AI17','name':'Auto Summarisation (Post-Call)','layer':'AI & Automation','lever':'aht_reduction','impact':0.08,'channels':['Voice','Chat'],'complexity':'any','effort':'low','ahtImpact':-0.08,'fcrImpact':0,'csatImpact':0,'roles':['Agent L1','Agent L2 / Specialist'],'ramp':3,'adoption':0.90},
    {'id':'AI18','name':'Self-Service App Enhancement','layer':'AI & Automation','lever':'deflection','impact':0.20,'channels':['App/Self-Service'],'complexity':'simple','effort':'high','ahtImpact':0,'fcrImpact':0.05,'csatImpact':0.10,'roles':['Agent L1'],'ramp':9,'adoption':0.75},
    {'id':'AI19','name':'Document AI / OCR Processing','layer':'AI & Automation','lever':'aht_reduction','impact':0.30,'channels':['Email'],'complexity':'moderate','effort':'medium','ahtImpact':-0.30,'fcrImpact':0.05,'csatImpact':0.02,'roles':['Back-Office / Processing'],'ramp':6,'adoption':0.80},
    {'id':'AI20','name':'AI Training & Coaching','layer':'AI & Automation','lever':'aht_reduction','impact':0.10,'channels':['Voice','Chat'],'complexity':'any','effort':'low','ahtImpact':-0.10,'fcrImpact':0.05,'csatImpact':0.05,'roles':['Trainer','Agent L1'],'ramp':3,'adoption':0.85},
    {'id':'AI21','name':'Speech Analytics','layer':'AI & Automation','lever':'aht_reduction','impact':0.08,'channels':['Voice'],'complexity':'any','effort':'medium','ahtImpact':-0.05,'fcrImpact':0.05,'csatImpact':0.05,'roles':['QA Analyst','Supervisor / Team Lead'],'ramp':6,'adoption':0.80},
    {'id':'AI22','name':'Customer 360 Screen Pop','layer':'AI & Automation','lever':'aht_reduction','impact':0.12,'channels':['Voice','Chat'],'complexity':'any','effort':'low','ahtImpact':-0.12,'fcrImpact':0.06,'csatImpact':0.05,'roles':['Agent L1','Agent L2 / Specialist'],'ramp':3,'adoption':0.90},
    {'id':'AI23','name':'AI Callback Scheduling','layer':'AI & Automation','lever':'deflection','impact':0.08,'channels':['Voice'],'complexity':'any','effort':'low','ahtImpact':0,'fcrImpact':0.02,'csatImpact':0.08,'roles':['Agent L1'],'ramp':3,'adoption':0.85},
    {'id':'AI24','name':'Video Support Channel','layer':'AI & Automation','lever':'escalation_reduction','impact':0.10,'channels':['Retail/Walk-in'],'complexity':'complex','effort':'high','ahtImpact':-0.10,'fcrImpact':0.10,'csatImpact':0.15,'roles':['Agent L2 / Specialist','Agent L3 / Expert'],'ramp':9,'adoption':0.60},
    {'id':'AI25','name':'Smart FAQ / Guided Resolution','layer':'AI & Automation','lever':'deflection','impact':0.15,'channels':['App/Self-Service','Chat'],'complexity':'simple','effort':'low','ahtImpact':0,'fcrImpact':0.05,'csatImpact':0.05,'roles':['Agent L1','Knowledge Manager'],'ramp':3,'adoption':0.80},
    {'id':'AI26','name':'AI Fraud Detection','layer':'AI & Automation','lever':'escalation_reduction','impact':0.08,'channels':['Voice','Chat','App/Self-Service'],'complexity':'complex','effort':'high','ahtImpact':-0.05,'fcrImpact':0.03,'csatImpact':0.02,'roles':['Agent L3 / Expert'],'ramp':9,'adoption':0.75},
    {'id':'AI27','name':'Next-Best-Action Engine','layer':'AI & Automation','lever':'aht_reduction','impact':0.10,'channels':['Voice','Chat'],'complexity':'any','effort':'medium','ahtImpact':-0.10,'fcrImpact':0.08,'csatImpact':0.10,'roles':['Agent L1','Agent L2 / Specialist'],'ramp':6,'adoption':0.80},
    {'id':'AI28','name':'Automated Compliance Checks','layer':'AI & Automation','lever':'aht_reduction','impact':0.05,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'low','ahtImpact':-0.05,'fcrImpact':0.02,'csatImpact':0,'roles':['QA Analyst','Agent L1'],'ramp':3,'adoption':0.85},
    # Layer 2: Operating Model (18)
    {'id':'OP01','name':'Tiered Service Model','layer':'Operating Model','lever':'deflection','impact':0.15,'channels':['Voice','Chat'],'complexity':'any','effort':'high','ahtImpact':-0.10,'fcrImpact':0.08,'csatImpact':0.05,'roles':['Agent L1','Agent L2 / Specialist','Supervisor / Team Lead'],'ramp':9,'adoption':0.85},
    {'id':'OP02','name':'Universal Agent / Cross-skilling','layer':'Operating Model','lever':'aht_reduction','impact':0.10,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'high','ahtImpact':-0.10,'fcrImpact':0.10,'csatImpact':0.05,'roles':['Agent L1','Trainer'],'ramp':9,'adoption':0.75},
    {'id':'OP03','name':'Queue Consolidation','layer':'Operating Model','lever':'shrinkage_reduction','impact':0.15,'channels':['Voice','Chat'],'complexity':'any','effort':'medium','ahtImpact':-0.05,'fcrImpact':0.05,'csatImpact':0.03,'roles':['WFM Analyst','Supervisor / Team Lead'],'ramp':6,'adoption':0.85},
    {'id':'OP04','name':'Shift Optimisation','layer':'Operating Model','lever':'shrinkage_reduction','impact':0.10,'channels':['Voice','Chat'],'complexity':'any','effort':'low','ahtImpact':0,'fcrImpact':0,'csatImpact':0,'roles':['WFM Analyst'],'ramp':3,'adoption':0.90},
    {'id':'OP05','name':'Supervisor Span Increase','layer':'Operating Model','lever':'aht_reduction','impact':0.05,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'low','ahtImpact':0,'fcrImpact':0,'csatImpact':-0.02,'roles':['Supervisor / Team Lead'],'ramp':3,'adoption':0.90},
    {'id':'OP06','name':'Back-Office Automation','layer':'Operating Model','lever':'aht_reduction','impact':0.20,'channels':['Email'],'complexity':'simple','effort':'medium','ahtImpact':-0.20,'fcrImpact':0.05,'csatImpact':0.03,'roles':['Back-Office / Processing'],'ramp':6,'adoption':0.80},
    {'id':'OP07','name':'FCR Improvement Program','layer':'Operating Model','lever':'repeat_reduction','impact':0.20,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'medium','ahtImpact':0,'fcrImpact':0.20,'csatImpact':0.10,'roles':['Agent L1','Supervisor / Team Lead','Trainer'],'ramp':6,'adoption':0.80},
    {'id':'OP08','name':'Escalation Path Redesign','layer':'Operating Model','lever':'escalation_reduction','impact':0.25,'channels':['Voice','Chat'],'complexity':'any','effort':'medium','ahtImpact':-0.05,'fcrImpact':0.10,'csatImpact':0.08,'roles':['Agent L2 / Specialist','Agent L3 / Expert','Supervisor / Team Lead'],'ramp':6,'adoption':0.80},
    {'id':'OP09','name':'Knowledge Management Overhaul','layer':'Operating Model','lever':'aht_reduction','impact':0.12,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'medium','ahtImpact':-0.12,'fcrImpact':0.08,'csatImpact':0.05,'roles':['Knowledge Manager','Trainer'],'ramp':6,'adoption':0.85},
    {'id':'OP10','name':'Performance Incentive Redesign','layer':'Operating Model','lever':'aht_reduction','impact':0.08,'channels':['Voice','Chat'],'complexity':'any','effort':'low','ahtImpact':-0.05,'fcrImpact':0.05,'csatImpact':0.05,'roles':['Supervisor / Team Lead'],'ramp':3,'adoption':0.85},
    {'id':'OP11','name':'Demand Forecasting Model','layer':'Operating Model','lever':'shrinkage_reduction','impact':0.08,'channels':['Voice','Chat'],'complexity':'any','effort':'medium','ahtImpact':0,'fcrImpact':0,'csatImpact':0,'roles':['WFM Analyst','Reporting / Analytics'],'ramp':6,'adoption':0.85},
    {'id':'OP12','name':'Channel Migration Campaign','layer':'Operating Model','lever':'deflection','impact':0.10,'channels':['Voice','Retail/Walk-in'],'complexity':'simple','effort':'low','ahtImpact':0,'fcrImpact':0,'csatImpact':-0.02,'roles':['Agent L1'],'ramp':3,'adoption':0.70},
    {'id':'OP13','name':'Lean Process Mapping','layer':'Operating Model','lever':'aht_reduction','impact':0.08,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'low','ahtImpact':-0.08,'fcrImpact':0.03,'csatImpact':0.02,'roles':['Agent L1','Supervisor / Team Lead'],'ramp':3,'adoption':0.85},
    {'id':'OP14','name':'Specialised Retention Team','layer':'Operating Model','lever':'escalation_reduction','impact':0.15,'channels':['Voice'],'complexity':'complex','effort':'medium','ahtImpact':-0.05,'fcrImpact':0.12,'csatImpact':0.10,'roles':['Agent L2 / Specialist'],'ramp':6,'adoption':0.80},
    {'id':'OP15','name':'Proactive Outbound (Manual)','layer':'Operating Model','lever':'deflection','impact':0.05,'channels':['Voice','SMS/WhatsApp'],'complexity':'simple','effort':'low','ahtImpact':0,'fcrImpact':0.03,'csatImpact':0.05,'roles':['Agent L1'],'ramp':3,'adoption':0.80},
    {'id':'OP16','name':'Reporting Consolidation','layer':'Operating Model','lever':'shrinkage_reduction','impact':0.05,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'low','ahtImpact':0,'fcrImpact':0,'csatImpact':0,'roles':['Reporting / Analytics','WFM Analyst'],'ramp':3,'adoption':0.90},
    {'id':'OP17','name':'Call Avoidance (Root Cause Fix)','layer':'Operating Model','lever':'deflection','impact':0.12,'channels':['Voice','Chat'],'complexity':'any','effort':'medium','ahtImpact':0,'fcrImpact':0.10,'csatImpact':0.08,'roles':['Agent L1','Supervisor / Team Lead'],'ramp':6,'adoption':0.75},
    {'id':'OP18','name':'Agent Desktop Unification','layer':'Operating Model','lever':'aht_reduction','impact':0.10,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'medium','ahtImpact':-0.10,'fcrImpact':0.05,'csatImpact':0.03,'roles':['Agent L1','Agent L2 / Specialist'],'ramp':6,'adoption':0.85},
    # Layer 3: Location Strategy (12)
    {'id':'LS01','name':'Nearshore Hub Setup','layer':'Location Strategy','lever':'cost_reduction','impact':0.25,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'high','ahtImpact':0,'fcrImpact':-0.02,'csatImpact':-0.03,'roles':['Agent L1'],'ramp':12,'adoption':0.80},
    {'id':'LS02','name':'Offshore Expansion','layer':'Location Strategy','lever':'cost_reduction','impact':0.40,'channels':['Chat','Email'],'complexity':'simple','effort':'high','ahtImpact':0,'fcrImpact':-0.03,'csatImpact':-0.05,'roles':['Agent L1','Back-Office / Processing'],'ramp':12,'adoption':0.75},
    {'id':'LS03','name':'Work-from-Home Program','layer':'Location Strategy','lever':'cost_reduction','impact':0.10,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'low','ahtImpact':0,'fcrImpact':0,'csatImpact':0,'roles':['Agent L1','Agent L2 / Specialist'],'ramp':3,'adoption':0.90},
    {'id':'LS04','name':'BPO Partnership','layer':'Location Strategy','lever':'cost_reduction','impact':0.30,'channels':['Voice','Chat'],'complexity':'simple','effort':'high','ahtImpact':0.05,'fcrImpact':-0.05,'csatImpact':-0.05,'roles':['Agent L1'],'ramp':9,'adoption':0.75},
    {'id':'LS05','name':'Gig Workforce Model','layer':'Location Strategy','lever':'cost_reduction','impact':0.15,'channels':['Chat','Email'],'complexity':'simple','effort':'medium','ahtImpact':0,'fcrImpact':-0.02,'csatImpact':-0.02,'roles':['Agent L1'],'ramp':6,'adoption':0.65},
    {'id':'LS06','name':'Shared Services Centre','layer':'Location Strategy','lever':'cost_reduction','impact':0.20,'channels':['Email'],'complexity':'any','effort':'high','ahtImpact':0,'fcrImpact':0,'csatImpact':0,'roles':['Back-Office / Processing','Reporting / Analytics'],'ramp':12,'adoption':0.85},
    {'id':'LS07','name':'Site Consolidation','layer':'Location Strategy','lever':'cost_reduction','impact':0.12,'channels':['Voice','Chat'],'complexity':'any','effort':'medium','ahtImpact':0,'fcrImpact':0,'csatImpact':0,'roles':['Agent L1','Supervisor / Team Lead'],'ramp':6,'adoption':0.85},
    {'id':'LS08','name':'Follow-the-Sun Model','layer':'Location Strategy','lever':'shrinkage_reduction','impact':0.08,'channels':['Voice','Chat'],'complexity':'any','effort':'high','ahtImpact':0,'fcrImpact':0,'csatImpact':0.02,'roles':['Agent L1'],'ramp':9,'adoption':0.70},
    {'id':'LS09','name':'Regional Language Hubs','layer':'Location Strategy','lever':'aht_reduction','impact':0.05,'channels':['Voice'],'complexity':'any','effort':'medium','ahtImpact':-0.05,'fcrImpact':0.03,'csatImpact':0.05,'roles':['Agent L1'],'ramp':6,'adoption':0.80},
    {'id':'LS10','name':'Automation Centre of Excellence','layer':'Location Strategy','lever':'aht_reduction','impact':0.08,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'medium','ahtImpact':-0.05,'fcrImpact':0.02,'csatImpact':0.02,'roles':['Reporting / Analytics','WFM Analyst'],'ramp':6,'adoption':0.85},
    {'id':'LS11','name':'Cloud Migration','layer':'Location Strategy','lever':'cost_reduction','impact':0.15,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'high','ahtImpact':0,'fcrImpact':0,'csatImpact':0,'roles':['Agent L1'],'ramp':12,'adoption':0.90},
    {'id':'LS12','name':'Vendor Rationalisation','layer':'Location Strategy','lever':'cost_reduction','impact':0.10,'channels':['Voice','Chat','Email'],'complexity':'any','effort':'medium','ahtImpact':0,'fcrImpact':0,'csatImpact':0,'roles':['Agent L1'],'ramp':6,'adoption':0.85},
]

# CR-014v2: Industry-benchmarked caps (40-50% agent reduction achievable;
# 80% common issues resolved by AI by 2029; 30-40% cost reduction achievable)
# Architecture: 3 cap layers only — Pool Ceiling (data-driven) → Single Init Cap → Per-Role Max
# Per-lever initiative caps REMOVED — redundant with pool netting, was double-counting conservatism
ABSOLUTE_SINGLE_INIT_CAP = 0.30  # 40-50% achievable; 30% per single initiative is conservative
PER_ROLE_MAX_REDUCTION = 0.50    # Industry benchmark: 40-50% fewer agents; 50% is the defensible ceiling

# CR-015v2: Lever saturation caps retained as safety backstop (pool netting is primary)
# These are now only used as absolute ceilings if pool data is unavailable
LEVER_CAPS = {
    'deflection':           {'simple': 0.35, 'moderate': 0.25, 'complex': 0.15},
    'aht_reduction':        {'simple': 0.30, 'moderate': 0.22, 'complex': 0.15},
    'escalation_reduction': {'simple': 0.25, 'moderate': 0.20, 'complex': 0.15},
    'repeat_reduction':     {'simple': 0.25, 'moderate': 0.18, 'complex': 0.12},
    'shrinkage_reduction':  {'simple': 0.20, 'moderate': 0.15, 'complex': 0.10},
    'cost_reduction':       {'simple': 0.35, 'moderate': 0.28, 'complex': 0.20},
}
# Waterfall processing order: deflection first (removes volume), then AHT, then operating model, then location
LEVER_ORDER = ['deflection', 'aht_reduction', 'repeat_reduction', 'transfer_reduction',
               'escalation_reduction', 'shrinkage_reduction', 'cost_reduction']


# ── Monthly Benefit Phasing (CR-020) ──
# S-curve ramp over N months from go-live, not from calendar Year 1.
# Default: 12-month S-curve (30% avg M1-3, 70% avg M4-8, 95% avg M9-12)

def _s_curve_ramp(months_since_golive, ramp_months=12):
    """
    Return benefit accrual % for a given month offset from go-live.
    Uses a logistic S-curve that reaches ~95% at ramp_months.
    
    months_since_golive: 1-based (month 1 = first month of benefits)
    ramp_months: months to reach ~95% (default 12)
    """
    if months_since_golive <= 0:
        return 0.0
    if months_since_golive >= ramp_months:
        return 1.0
    # Logistic: 1/(1 + exp(-k*(t - midpoint)))
    # Tuned so: ~30% at t=ramp/4, ~70% at t=ramp*2/3, ~95% at t=ramp
    midpoint = ramp_months * 0.45
    k = 6.0 / ramp_months
    raw = 1.0 / (1.0 + math.exp(-k * (months_since_golive - midpoint)))
    # Rescale so we hit 0 at t=0 and ~1.0 at t=ramp
    floor = 1.0 / (1.0 + math.exp(-k * (0 - midpoint)))
    ceiling = 1.0 / (1.0 + math.exp(-k * (ramp_months - midpoint)))
    scaled = (raw - floor) / max(ceiling - floor, 0.001)
    return max(0.0, min(1.0, scaled))


def _compute_yearly_factors(start_month, end_month, horizon_years, ramp_months=12):
    """
    Compute the effective benefit fraction for each year, respecting:
      - start_month: month when benefits begin accruing (1-based)
      - end_month: month when benefits stop (null/0 = runs to horizon end)
      - ramp: S-curve from go-live, not from calendar year
    
    Returns: list of floats [year1_factor, year2_factor, ...] where 1.0 = full year at steady state.
    
    Example: start_month=7, ramp=12 months, horizon=3
      Year 1 (M1-12): months 7-12 active → 6 months × early ramp → ~0.12
      Year 2 (M13-24): months 13-24 active → 12 months × mid-to-full ramp → ~0.75
      Year 3 (M25-36): months 25-36 active → 12 months × full ramp → ~0.98
    """
    total_months = horizon_years * 12
    if not end_month or end_month <= 0:
        end_month = total_months
    end_month = min(end_month, total_months)
    
    yearly_factors = []
    for yr in range(horizon_years):
        yr_start = yr * 12 + 1   # month 1, 13, 25, ...
        yr_end = (yr + 1) * 12   # month 12, 24, 36, ...
        
        month_sum = 0.0
        active_months = 0
        for m in range(yr_start, yr_end + 1):
            if m < start_month or m > end_month:
                # Outside active window → 0 benefit
                continue
            # Months since benefits started
            t = m - start_month + 1
            ramp_pct = _s_curve_ramp(t, ramp_months)
            month_sum += ramp_pct
            active_months += 1
        
        # Factor = average ramp across active months in this year × (active_months/12)
        # This gives the fraction of full-year steady-state benefit
        if active_months > 0:
            avg_ramp = month_sum / active_months
            yearly_factors.append(round(avg_ramp * active_months / 12, 4))
        else:
            yearly_factors.append(0.0)
    
    return yearly_factors


def score_initiatives(data, diagnostic, readiness_ctx=None):
    """
    EY Methodology: Score each initiative using consulting-grade logic.

    Flow:
      1. Compute readiness context (or use provided)
      2. Hard exclusion gates (channel, role, volume significance)
      3. Trigger gate per lever (IF repeatable > 30% → deflection qualified, etc.)
      4. Score = (Value × Alignment × Readiness) / (Complexity × Risk)
      5. Normalize to 0-100, classify stage (AI Base / Enhanced / Autonomous)
      6. Select: ≥65 Strong, ≥50 Moderate, <50 Disabled
    """
    from engines.readiness import (compute_readiness, check_trigger, get_alignment,
                                    classify_stage, compute_risk, COMPLEXITY_MAP)

    # ── Step 0: Build readiness context ──
    if readiness_ctx is None:
        readiness_ctx = compute_readiness(data, diagnostic)

    queues = data['queues']
    roles = data['roles']
    channel_volumes = readiness_ctx['channelVolumes']
    channels_used = readiness_ctx['channelsUsed']
    role_names = readiness_ctx['roleNames']
    total_volume = readiness_ctx['totalVolume']
    total_cost = readiness_ctx['totalCost']
    strategic_driver = readiness_ctx['strategicDriver']
    readiness_map = readiness_ctx['readinessMap']

    # Max possible saving cap (no single initiative > 25% of total cost base)
    max_possible_saving = max(total_cost * 0.25, 1)

    initiatives = []
    raw_scores = []

    for lib_init in INITIATIVE_LIBRARY:
        init = dict(lib_init)
        reasons = []
        exclusion = None

        # ── Default fields ──
        init.setdefault('rampYear1', data['params'].get('rampYear1', 0.30))
        init.setdefault('rampYear2', data['params'].get('rampYear2', 0.70))
        init.setdefault('rampYear3', data['params'].get('rampYear3', 0.95))

        # ══ HARD EXCLUSION GATE 1: Channel overlap ══
        matching_channels = set(init['channels']) & channels_used
        if not matching_channels:
            exclusion = 'No matching channels in client operation'

        # ══ HARD EXCLUSION GATE 2: Role overlap ══
        if not exclusion:
            matching_roles = set(init['roles']) & role_names
            if not matching_roles:
                exclusion = 'No matching roles in client workforce'

        # ══ HARD EXCLUSION GATE 3: Volume significance (< 2%) ══
        if not exclusion:
            target_volume = sum(channel_volumes.get(ch, 0) for ch in matching_channels)
            volume_share = target_volume / max(total_volume, 1)
            if volume_share < 0.02:
                exclusion = f'Target channels carry {volume_share:.1%} of volume (< 2% threshold)'

        # ── Handle exclusions ──
        if exclusion:
            init.update({
                'enabled': False, 'matchScore': 0, 'score': 0,
                'reasons': [exclusion], 'triggerPassed': False,
                'matchTier': 'excluded', 'stage': 'N/A',
                '_fteImpact': 0, '_annualSaving': 0, '_effectiveImpact': 0,
                '_linkedQueues': 0, '_rampPcts': [0, 0, 0], '_yearlyFactors': [0, 0, 0], '_contributionPct': 0,
                'startMonth': 0, 'endMonth': 0, '_startMonth': 0, '_endMonth': 0, '_rampCompleteMonth': 0,
            })
            initiatives.append(init)
            raw_scores.append(0)
            continue

        # ══ TRIGGER GATE: Lever-specific qualification ══
        lever = init.get('lever', 'aht_reduction')
        trigger_passed, trigger_reason = check_trigger(lever, readiness_ctx)
        reasons.append(trigger_reason)

        if not trigger_passed:
            # Diagnostic alignment bonus: if diagnostic explicitly flags this lever,
            # override trigger failure (consultant can still see the opportunity)
            if lever in readiness_ctx.get('problemLevers', set()):
                trigger_passed = True
                reasons.append(f'Trigger overridden: diagnostic flagged {lever} as problem area')
            else:
                init.update({
                    'enabled': False, 'matchScore': 0, 'score': 0,
                    'reasons': reasons, 'triggerPassed': False,
                    'matchTier': 'trigger_fail', 'stage': 'N/A',
                    '_fteImpact': 0, '_annualSaving': 0, '_effectiveImpact': 0,
                    '_linkedQueues': sum(1 for q in queues if q['channel'] in init['channels']),
                    '_rampPcts': [0, 0, 0], '_yearlyFactors': [0, 0, 0], '_contributionPct': 0,
                    'startMonth': 0, 'endMonth': 0, '_startMonth': 0, '_endMonth': 0, '_rampCompleteMonth': 0,
                })
                initiatives.append(init)
                raw_scores.append(0)
                continue

        init['triggerPassed'] = True

        # ══════════════════════════════════════════════
        #  EY SCORING: (Value × Alignment × Readiness) / (Complexity × Risk)
        # ══════════════════════════════════════════════

        # ── A. VALUE POTENTIAL (0.0 – 1.0) ──
        affected_roles = [r for r in roles if r['role'] in init['roles']]
        affected_fte = sum(r['headcount'] for r in affected_roles)
        if affected_fte > 0:
            weighted_cost = sum(r['headcount'] * r['costPerFTE'] for r in affected_roles) / affected_fte
        else:
            weighted_cost = 55000  # fallback
        raw_saving = affected_fte * init['impact'] * init['adoption'] * weighted_cost
        value = min(1.0, raw_saving / max(max_possible_saving, 1))
        reasons.append(f'Value: {value:.2f} (${raw_saving:,.0f} potential saving)')

        # ── B. STRATEGIC ALIGNMENT (0.0 – 1.0) ──
        alignment = get_alignment(strategic_driver, lever)
        reasons.append(f'Alignment: {alignment:.2f} ({strategic_driver} × {lever})')

        # ── C. READINESS SCORE (0.0 – 1.0) ──
        readiness = readiness_map.get(init['layer'], 0.5)
        reasons.append(f'Readiness: {readiness:.2f} ({init["layer"]})')

        # ── D. COMPLEXITY (1.0 – 5.0) ──
        complexity = COMPLEXITY_MAP.get(init['effort'], 3.0)

        # ── E. RISK (1.0 – 5.0) ──
        risk = compute_risk(init)
        reasons.append(f'Risk: {risk:.2f} (effort={init["effort"]}, ramp={init["ramp"]}mo)')

        # ── COMPUTE RAW SCORE ──
        denominator = max(complexity * risk, 0.01)
        raw_score = (value * alignment * readiness) / denominator
        reasons.append(f'Raw: ({value:.3f} × {alignment:.2f} × {readiness:.2f}) / ({complexity:.1f} × {risk:.2f}) = {raw_score:.4f}')

        # ── Diagnostic bonus: +20% if lever matches a flagged problem area ──
        if lever in readiness_ctx.get('problemLevers', set()):
            raw_score *= 1.20
            reasons.append(f'Diagnostic bonus: +20% (lever matches problem area)')

        # ── Volume weight bonus: scale by how much volume this touches ──
        target_volume = sum(channel_volumes.get(ch, 0) for ch in matching_channels)
        volume_share = target_volume / max(total_volume, 1)
        vol_bonus = 1.0 + (volume_share * 0.3)  # up to +30% for high-volume targets
        raw_score *= vol_bonus
        reasons.append(f'Volume: {volume_share:.0%} of total (+{(vol_bonus-1)*100:.0f}% bonus)')

        # ── Stage classification ──
        layer_readiness = readiness_map.get(init['layer'], 0.5)
        stage, start_month, stage_desc = classify_stage(layer_readiness, init['effort'])
        init['stage'] = stage
        init['stageDescription'] = stage_desc
        init['startMonth'] = start_month
        init['endMonth'] = start_month + init['ramp']
        init['timeline'] = 'quick_win' if init['endMonth'] <= 6 else 'medium_term' if init['endMonth'] <= 12 else 'strategic'

        # ── Store intermediate values ──
        init['_rawScore'] = raw_score
        init['_value'] = round(value, 4)
        init['_alignment'] = alignment
        init['_readiness'] = readiness
        init['_complexity'] = complexity
        init['_risk'] = risk
        init['_volumeShare'] = round(volume_share, 4)
        init['matchScore'] = round(raw_score * 10000, 1)  # for backward compat
        init['reasons'] = reasons
        init['_fteImpact'] = 0  # filled by run_waterfall
        init['_annualSaving'] = 0
        init['_effectiveImpact'] = 0
        init['_linkedQueues'] = sum(1 for q in queues if q['channel'] in init['channels'])
        init['_rampPcts'] = [init.get('rampYear1', 0.30), init.get('rampYear2', 0.70), init.get('rampYear3', 0.95)]
        init['_yearlyFactors'] = []  # Populated by run_waterfall with monthly-phased factors
        init['_startMonth'] = init.get('startMonth', 1)
        init['_endMonth'] = 0  # Populated by run_waterfall
        init['_rampCompleteMonth'] = init.get('startMonth', 1) + init.get('ramp', 12)
        init['_contributionPct'] = 0

        initiatives.append(init)
        raw_scores.append(raw_score)

    # ══ NORMALIZE TO 0-100 ══
    max_raw = max(raw_scores) if raw_scores else 0.001
    if max_raw <= 0:
        max_raw = 0.001

    for init, rs in zip(initiatives, raw_scores):
        if rs > 0:
            normalized = round((rs / max_raw) * 100, 1)
        else:
            normalized = 0
        init['score'] = normalized
        init['relevanceScore'] = normalized  # frontend compat

        # ── Selection threshold ──
        # CR-030: Lowered from 50→20 to enable more initiatives.
        # Pool-based netting prevents over-counting; broader enablement
        # gives a more realistic transformation scope.
        if normalized >= 65:
            init['enabled'] = True
            init['matchTier'] = 'strong'
        elif normalized >= 45:
            init['enabled'] = True
            init['matchTier'] = 'moderate'
        elif normalized >= 20:
            init['enabled'] = True
            init['matchTier'] = 'recommended'
        elif normalized >= 8:
            init['enabled'] = False
            init['matchTier'] = 'weak'
        else:
            if init.get('matchTier') not in ('excluded', 'trigger_fail'):
                init['matchTier'] = 'poor'
            init['enabled'] = False

    # ── Sort by score descending ──
    initiatives.sort(key=lambda x: x['score'], reverse=True)

    # ── Assign staggered start months for enabled initiatives ──
    month_cursor = 1
    for init in sorted([x for x in initiatives if x['enabled']], key=lambda x: x['score'], reverse=True):
        # Respect stage-based start month as minimum
        stage_min = init.get('startMonth', 1)
        init['startMonth'] = max(stage_min, month_cursor)
        init['endMonth'] = init['startMonth'] + init['ramp']
        # CR-038: Re-classify timeline AFTER staggered start recalculation
        duration = init['endMonth'] - init['startMonth']
        init['timeline'] = 'quick_win' if duration <= 6 and init['startMonth'] <= 3 else \
                           'medium_term' if init['endMonth'] <= 12 else 'strategic'
        month_cursor += {'low': 1, 'medium': 2, 'high': 3}.get(init['effort'], 2)

    return initiatives


def run_waterfall(data, initiatives, _skip_sensitivity=False, _skip_scenarios=False):
    """
    Execute pool-based waterfall cascade (CR-015v2).
    
    Flow:
      1. Enrich intents with deflection eligibility, AHT decomposition, etc.
      2. Compute opportunity pools (finite ceilings per lever)
      3. For each enabled initiative (sorted by layer → lever → score):
         a. Compute gross impact using lever-specific physics
         b. Net against remaining pool: net = min(gross, remaining_pool)
         c. Apply safety caps (per-initiative, per-role)
         d. Consume from pool
         e. Apply ramp-up for yearly phasing
      4. Financial projection (NPV, investment, IRR)
    
    Returns same output structure as before for backward compatibility.
    """
    horizon = data['params']['horizon']
    roles = data['roles']
    queues = data['queues']
    params = data['params']
    enabled = [i for i in initiatives if i.get('enabled')]
    total_fte = data['totalFTE']
    
    # ── Step 1: Enrich intents ──
    try:
        enriched_queues = enrich_intents(queues, params)
    except Exception:
        enriched_queues = queues  # graceful fallback
    
    # ── Step 2: Compute opportunity pools ──
    # Inject benchmark defaults into params for CSAT pool computation (v10)
    benchmarks = data.get('benchmarks', {})
    if isinstance(benchmarks, dict):
        params['_benchmarks_defaults'] = benchmarks.get('_defaults', benchmarks)
    
    # P2-1: Get cost matrix for location-aware pool computation
    cost_matrix = data.get('locationCostMatrix')
    
    pool_result = None
    try:
        pool_result = compute_pools(enriched_queues, roles, params, cost_matrix=cost_matrix)
        pools = pool_result['pools']
        pool_summary = pool_result['summary']
    except Exception:
        pools = {}
        pool_summary = {}
    
    # ── Step 2b: Reuse annualization factor from pools for gross impact calculations ──
    # This ensures pools and gross use identical volume scaling (CR-021 dedup fix)
    annualization = pool_result.get('annualization_factor', 1.0) if pool_result else 1.0
    
    # Create annualized queue copies for gross impact computation
    annual_queues = []
    for q in enriched_queues:
        aq = dict(q)
        aq['volume'] = round(q['volume'] * annualization)
        annual_queues.append(aq)
    
    # ── Step 3: Sort initiatives ──
    # Layer order: AI & Automation → Operating Model → Location Strategy
    # Within each layer, sort by lever order then score descending
    layer_order = {'AI & Automation': 0, 'Operating Model': 1, 'Location Strategy': 2}
    lever_order_map = {lv: i for i, lv in enumerate(LEVER_ORDER)}
    
    sorted_inits = sorted(enabled, key=lambda x: (
        layer_order.get(x.get('layer', ''), 9),
        lever_order_map.get(x.get('lever', ''), 9),
        -x.get('matchScore', 0)
    ))
    
    # ── Per-role tracking ──
    role_impact = {r['role']: {'baseline': r['headcount'], 'yearly': [0.0] * horizon} for r in roles}
    role_cum = {r['role']: 0.0 for r in roles}
    
    # ── P2-1: Per-BU tracking ──
    bus = pool_summary.get('bus', sorted(set(q.get('bu', 'Default') for q in queues)))
    bu_weighted_costs = pool_summary.get('bu_weighted_costs', {})
    bu_fte_baseline = pool_summary.get('bu_fte', {})
    bu_impact = {bu: {'baseline_fte': bu_fte_baseline.get(bu, 0), 'yearly_fte': [0.0] * horizon,
                       'yearly_saving': [0.0] * horizon, 'weighted_cost': bu_weighted_costs.get(bu, 55000),
                       'initiative_attrib': []}  # CR-030: per-initiative attribution
                 for bu in bus}
    
    # CR-032: Per-BU pool utilization tracking
    bu_pool_util = {bu: {pk: {'consumed': 0.0, 'ceiling': 0.0} for pk in ['deflection', 'aht_reduction', 'transfer_reduction', 'escalation_reduction', 'repeat_reduction', 'shrinkage_reduction']} for bu in bus}
    
    # Audit trail for pool consumption
    audit_trail = []
    
    # ── Step 4: Waterfall cascade ──
    for init in sorted_inits:
        affected = [r for r in roles if r['role'] in init['roles']]
        tot_aff = sum(r['headcount'] for r in affected)
        if tot_aff == 0:
            init.update({'_fteImpact': 0, '_annualSaving': 0, '_effectiveImpact': 0,
                         '_contributionPct': 0, '_mechanism': 'No affected roles',
                         '_poolConsumed': 0, '_poolCapped': False})
            continue
        
        lever = init.get('lever', 'aht_reduction')
        
        # ── 4a: Compute gross impact using lever-specific physics ──
        try:
            gross = compute_gross_impact(init, annual_queues, roles, pool_result, params, cost_matrix=cost_matrix)
        except Exception:
            # Fallback: use old-style generic formula
            weighted_cost = sum(r['headcount'] * r['costPerFTE'] for r in affected) / max(tot_aff, 1)
            gross_fte = tot_aff * init.get('impact', 0) * init.get('adoption', 0.8) * 0.50
            gross = {
                'gross_fte': round(gross_fte, 1), 'gross_contacts': 0, 'gross_seconds': 0,
                'gross_saving': round(gross_fte * weighted_cost),
                'mechanism': f'Fallback: {tot_aff} × {init.get("impact",0):.0%} × {init.get("adoption",0.8):.0%} × 50%',
            }
        
        raw_fte = gross.get('gross_fte', 0)
        is_location = gross.get('_is_location', False)
        
        if is_location:
            # Location initiatives: cost saving only, no FTE reduction
            # BUT still consume the location pool to prevent over-migration
            start_m = max(1, init.get('startMonth', 1))
            benefit_end = init.get('benefitEndMonth', 0)
            ramp_months = init.get('ramp', 12)
            loc_yearly_factors = _compute_yearly_factors(start_m, benefit_end, horizon, ramp_months)
            
            gross_migrated = gross.get('gross_fte_migrated', 0)
            gross_saving = gross.get('gross_saving', 0)
            pool_capped = False
            
            # Consume from location pool (cost_reduction)
            if pools and 'cost_reduction' in pools and gross_migrated > 0:
                try:
                    loc_consumption = consume_pool(
                        pools, 'cost_reduction', gross_migrated, 0, 0)
                    consumed_fte = loc_consumption.get('consumed_fte', gross_migrated)
                    pool_capped = loc_consumption.get('capped', False)
                    # Scale savings proportionally if pool-capped
                    if consumed_fte < gross_migrated and gross_migrated > 0:
                        scale = consumed_fte / gross_migrated
                        gross_saving = gross_saving * scale
                        gross_migrated = consumed_fte
                except Exception:
                    pass  # If pool consumption fails, use uncapped values
            
            init['_fteImpact'] = 0
            init['_annualSaving'] = round(gross_saving)
            init['_effectiveImpact'] = 0
            init['_mechanism'] = gross.get('mechanism', '')
            init['_poolConsumed'] = round(gross_migrated, 1)
            init['_poolCapped'] = pool_capped
            init['_grossFTE'] = 0
            init['_grossSaving'] = gross.get('gross_saving', 0)
            init['_rampPcts'] = loc_yearly_factors[:horizon]
            init['_yearlyFactors'] = loc_yearly_factors[:horizon]
            init['_startMonth'] = start_m
            init['_endMonth'] = benefit_end if benefit_end > 0 else horizon * 12
            init['_rampCompleteMonth'] = start_m + ramp_months
            init['_linkedQueues'] = sum(1 for q in queues if q['channel'] in init['channels'])
            init['_capApplied'] = pool_capped
            
            audit_trail.append({
                'id': init['id'], 'name': init['name'], 'lever': lever,
                'gross_fte': 0, 'net_fte': 0, 'saving': init['_annualSaving'],
                'mechanism': init['_mechanism'],
                'pool_capped': pool_capped,
                'gross_migrated': gross.get('gross_fte_migrated', 0),
                'net_migrated': round(gross_migrated, 1),
            })
            continue
        
        # ── 4b: Net against remaining pool ──
        try:
            consumption = consume_pool(
                pools, lever,
                amount_fte=raw_fte,
                amount_contacts=gross.get('gross_contacts', 0),
                amount_seconds=gross.get('gross_seconds', 0),
            )
            net_fte = consumption.get('consumed_fte', raw_fte)
            pool_capped = consumption.get('capped', False)
        except Exception:
            net_fte = raw_fte
            pool_capped = False
        
        # ── 4c: Apply safety caps (3-layer: pool ceiling already applied, now absolute + per-role) ──
        # Per-lever initiative caps REMOVED in CR-014v2 — redundant with pool netting
        abs_cap = ABSOLUTE_SINGLE_INIT_CAP * tot_aff
        red = min(net_fte, abs_cap)
        
        # ── 4c2: Compute secondary lever impacts (CR-025) ──
        secondary_results = []
        secondary_total_fte = 0
        try:
            secondaries = compute_secondary_impacts(init, annual_queues, roles, pool_result, params)
            for sec in secondaries:
                sec_lever = sec['lever']
                sec_gross = sec['gross_fte']
                # Consume secondary from its own pool
                try:
                    sec_consumption = consume_pool(pools, sec_lever, sec_gross, 0, 0)
                    sec_net = sec_consumption.get('consumed_fte', sec_gross)
                    sec_capped = sec_consumption.get('capped', False)
                except Exception:
                    sec_net = sec_gross
                    sec_capped = False
                # Cap secondary at half the absolute cap (secondary is subordinate)
                sec_net = min(sec_net, abs_cap * 0.5)
                if sec_net > 0.1:
                    secondary_results.append({
                        'lever': sec_lever, 'fte': round(sec_net, 1),
                        'saving': round(sec_net * (sum(r['headcount'] * r['costPerFTE'] for r in affected) / max(tot_aff, 1))),
                        'mechanism': sec['mechanism'], 'field': sec['field'],
                        'pool_capped': sec_capped,
                    })
                    secondary_total_fte += sec_net
        except Exception:
            pass  # If secondary computation fails, proceed with primary only
        
        # ── 4c3: Combined cap check (primary + secondary vs per-role availability) ──
        combined_fte = red + secondary_total_fte
        avail = sum(max(0, r['headcount'] * PER_ROLE_MAX_REDUCTION - role_cum[r['role']]) for r in affected)
        if combined_fte > avail:
            # Scale both primary and secondary proportionally
            scale = avail / max(combined_fte, 0.01)
            red = max(0, red * scale)
            secondary_total_fte = max(0, secondary_total_fte * scale)
            for sr in secondary_results:
                sr['fte'] = round(sr['fte'] * scale, 1)
                sr['saving'] = round(sr['saving'] * scale)
        red = max(0, red)
        
        # Update per-role cumulative (primary + secondary combined)
        for r in affected:
            role_cum[r['role']] += combined_fte * (r['headcount'] / tot_aff)
        
        # ── 4d: Apply monthly-phased ramp (CR-020) ──
        start_m = max(1, init.get('startMonth', 1))
        benefit_end = init.get('benefitEndMonth', 0)
        ramp_months = init.get('ramp', 12)
        yearly_factors = _compute_yearly_factors(start_m, benefit_end, horizon, ramp_months)
        
        total_init_fte = red + secondary_total_fte
        
        # Weighted cost per affected FTE (needed for per-BU and storing results)
        wtd = sum(r['headcount'] * r['costPerFTE'] for r in affected) / max(tot_aff, 1)
        
        for yr in range(horizon):
            factor = yearly_factors[yr] if yr < len(yearly_factors) else 0.0
            for r in affected:
                role_impact[r['role']]['yearly'][yr] += total_init_fte * factor * (r['headcount'] / tot_aff)
        
        # ── P2-1: Per-BU FTE attribution ──
        # Distribute initiative's impact across BUs based on matching queue volume
        init_channels = set(init.get('channels', []))
        init_target_bus = set(init.get('targetBUs', []))
        bu_volumes = {}
        for q in queues:
            if q.get('channel', '') in init_channels and (not init_target_bus or q.get('bu', '') in init_target_bus):
                b = q.get('bu', 'Default')
                bu_volumes[b] = bu_volumes.get(b, 0) + q['volume']
        total_bu_vol = sum(bu_volumes.values()) or 1
        for bu_name, bu_vol in bu_volumes.items():
            if bu_name in bu_impact:
                bu_share = bu_vol / total_bu_vol
                for yr in range(horizon):
                    factor = yearly_factors[yr] if yr < len(yearly_factors) else 0.0
                    bu_impact[bu_name]['yearly_fte'][yr] += total_init_fte * factor * bu_share
                    bu_impact[bu_name]['yearly_saving'][yr] += total_init_fte * factor * bu_share * wtd
                # CR-030: Track per-initiative attribution for BU cards
                bu_init_fte = round(total_init_fte * bu_share, 1)
                bu_init_saving = round(total_init_fte * bu_share * wtd)
                if bu_init_fte > 0 or bu_init_saving > 0:
                    bu_impact[bu_name]['initiative_attrib'].append({
                        'id': init['id'], 'name': init['name'], 'layer': init['layer'],
                        'fte': bu_init_fte, 'saving': bu_init_saving, 'lever': init.get('lever', ''),
                    })
                # CR-032: Track per-BU pool consumption
                lever = init.get('lever', '')
                if lever in bu_pool_util.get(bu_name, {}):
                    bu_pool_util[bu_name][lever]['consumed'] += bu_init_fte
        
        # ── Store results on initiative ──
        init['_fteImpact'] = round(total_init_fte, 1)
        init['_primaryFTE'] = round(red, 1)
        init['_secondaryFTE'] = round(secondary_total_fte, 1)
        init['_secondaryDetails'] = secondary_results
        init['_annualSaving'] = round(total_init_fte * wtd, 0)
        init['_effectiveImpact'] = round(total_init_fte / max(tot_aff, 1), 4)
        init['_rampPcts'] = yearly_factors[:horizon]
        init['_yearlyFactors'] = yearly_factors[:horizon]
        init['_startMonth'] = start_m
        init['_endMonth'] = benefit_end if benefit_end > 0 else horizon * 12
        init['_rampCompleteMonth'] = start_m + ramp_months
        init['_linkedQueues'] = sum(1 for q in queues if q['channel'] in init['channels'])
        init['_capApplied'] = total_init_fte < raw_fte * 0.95
        init['_mechanism'] = gross.get('mechanism', '')
        init['_poolConsumed'] = round(red, 1)
        init['_poolCapped'] = pool_capped
        init['_grossFTE'] = round(raw_fte, 1)
        init['_grossSaving'] = round(raw_fte * wtd, 0)
        
        audit_trail.append({
            'id': init['id'], 'name': init['name'], 'lever': lever,
            'gross_fte': round(raw_fte, 1), 'net_fte': round(red, 1),
            'secondary_fte': round(secondary_total_fte, 1),
            'total_fte': round(total_init_fte, 1),
            'saving': round(total_init_fte * wtd), 'mechanism': init['_mechanism'],
            'secondary_mechanisms': [s['mechanism'] for s in secondary_results],
            'pool_capped': pool_capped,
            'safety_capped': init['_capApplied'],
        })
    
    # ══════════════════════════════════════════════════════════════
    # V10: CSAT / CUSTOMER EXPERIENCE AGGREGATION ENGINE
    # ══════════════════════════════════════════════════════════════
    # Aggregate csatImpact from all enabled initiatives, apply diminishing
    # returns curve (logarithmic saturation against CSAT gap ceiling),
    # and compute revenue/retention monetisation.
    #
    # Sources:
    #   Industry benchmark: 10% CSAT → 2-3% revenue; CX leaders grow 4-8% above market
    #   Industry research: 5% retention → 25-95% profit increase
    #   Industry: 1% FCR increase ≈ 1% CSAT increase
    # ══════════════════════════════════════════════════════════════
    csat_pool = pools.get('csat_experience', {})
    csat_gap = csat_pool.get('portfolio_gap', 0)
    
    # Collect per-initiative CSAT contributions
    raw_csat_sum = 0.0
    csat_contributors = []
    for init in enabled:
        csat_val = init.get('csatImpact', 0)
        if csat_val <= 0:
            continue
        # Weight by adoption (consistent with how FTE impacts use adoption)
        adoption = init.get('adoption', 0.80)
        weighted_csat = csat_val * adoption
        raw_csat_sum += weighted_csat
        csat_contributors.append({
            'id': init['id'], 'name': init['name'],
            'raw_csat': round(csat_val, 3),
            'weighted_csat': round(weighted_csat, 3),
        })
    
    # Apply diminishing returns: effective = gap × (1 - e^(-k × raw / gap))
    # k = 1.5 tuned so: at raw = gap, effective ≈ 78% of gap (realistic)
    #                    at raw = 2×gap, effective ≈ 95% (near saturation)
    K_CSAT = 1.5
    if csat_gap > 0.01 and raw_csat_sum > 0:
        effective_csat_uplift = csat_gap * (1 - math.exp(-K_CSAT * raw_csat_sum / csat_gap))
    else:
        effective_csat_uplift = min(raw_csat_sum, csat_gap) if csat_gap > 0 else 0
    
    # Monetise the effective uplift
    rev_per_point = csat_pool.get('revenue_per_point', 0)
    ret_per_point = csat_pool.get('retention_per_point', 0.025)
    customer_base = csat_pool.get('customer_base', 0)
    annual_churn = csat_pool.get('annual_churn', 0.12)
    rev_per_cust = params.get('revenuePerCustomer', 0)
    clv_proxy = rev_per_cust * 3 if rev_per_cust > 0 else 0
    
    csat_revenue_impact = effective_csat_uplift * rev_per_point
    csat_churn_reduction = effective_csat_uplift * ret_per_point
    csat_retained_customers = customer_base * annual_churn * csat_churn_reduction if customer_base > 0 else 0
    csat_retention_value = csat_retained_customers * clv_proxy
    csat_total_value = csat_revenue_impact + csat_retention_value
    
    # Update CSAT pool consumption
    if 'csat_experience' in pools:
        pools['csat_experience']['consumed_csat_points'] = round(effective_csat_uplift, 3)
        pools['csat_experience']['consumed_revenue'] = round(csat_total_value)
        pools['csat_experience']['remaining_csat_points'] = round(max(0, csat_gap - effective_csat_uplift), 3)
        pools['csat_experience']['remaining_revenue'] = round(max(0, csat_pool.get('ceiling_total_value', 0) - csat_total_value))
    
    # Compute per-initiative share of effective CSAT (for bucket display)
    if raw_csat_sum > 0:
        for contrib in csat_contributors:
            share = contrib['weighted_csat'] / raw_csat_sum
            contrib['effective_csat'] = round(effective_csat_uplift * share, 4)
            contrib['revenue_share'] = round(csat_total_value * share)
    
    # Store CSAT contribution on each initiative
    for init in enabled:
        csat_val = init.get('csatImpact', 0) * init.get('adoption', 0.80)
        if csat_val > 0 and raw_csat_sum > 0:
            share = csat_val / raw_csat_sum
            init['_csatContribution'] = round(effective_csat_uplift * share, 4)
            init['_csatRevenueShare'] = round(csat_total_value * share)
        else:
            init['_csatContribution'] = 0
            init['_csatRevenueShare'] = 0
    
    # CSAT-gap based prioritisation boost (v10)
    # Intents with worst CSAT gaps get higher relevance for their initiatives
    if csat_gap > 0 and csat_pool.get('breakdown'):
        intent_csat_gaps = {}
        for entry in csat_pool['breakdown']:
            intent_csat_gaps[entry['intent']] = entry['gap']
        # Boost match score for initiatives targeting high-gap intents
        for init in initiatives:
            matching_gap = 0
            for q in queues:
                if q['channel'] in init.get('channels', []):
                    matching_gap = max(matching_gap, intent_csat_gaps.get(q.get('intent', ''), 0))
            if matching_gap > 0.1:
                boost = 1.0 + matching_gap * 0.5  # 0.5 point gap → 25% score boost
                init['_csatRelevanceBoost'] = round(boost, 2)
                if 'matchScore' in init:
                    init['matchScore'] = round(init['matchScore'] * boost, 1)
    
    # Build CSAT summary for return dict
    csat_summary = {
        'currentCSAT': round(csat_pool.get('current_csat', 0), 2),
        'benchmarkCSAT': round(csat_pool.get('benchmark_csat', 0), 2),
        'portfolioGap': round(csat_gap, 3),
        'rawInitiativeSum': round(raw_csat_sum, 3),
        'effectiveUplift': round(effective_csat_uplift, 3),
        'projectedCSAT': round(csat_pool.get('current_csat', 0) + effective_csat_uplift, 2),
        'saturationPct': round(effective_csat_uplift / max(csat_gap, 0.001) * 100, 1),
        'revenueImpact': round(csat_revenue_impact),
        'churnReduction': round(csat_churn_reduction, 4),
        'retainedCustomers': round(csat_retained_customers),
        'retentionValue': round(csat_retention_value),
        'totalCXValue': round(csat_total_value),
        'revenuePerPoint': round(rev_per_point),
        'contributors': sorted(csat_contributors, key=lambda x: x.get('effective_csat', 0), reverse=True)[:15],
    }
    
    # ── Clamp per-role yearly ──
    for ri in role_impact.values():
        ri['yearly'] = [min(v, ri['baseline'] * PER_ROLE_MAX_REDUCTION) for v in ri['yearly']]
    
    # ── Yearly financial projections ──
    yearly = []
    cum = 0
    # Compute per-year location savings using each initiative's own time-phased factors
    # IMPORTANT (CR-021 v5): Location savings (lever='cost_reduction') are SEPARATE from
    # role_impact. They do NOT reduce FTE — they reduce COST per FTE via geo arbitrage.
    # The `continue` at the end of the is_location block ensures these never enter role_impact.
    # Non-cost_reduction initiatives in Layer 3 (e.g. LS08 shrinkage, LS09 AHT) flow through
    # role_impact normally because they DO reduce actual workload.
    location_yearly = [0.0] * horizon
    bu_location_yearly = {bu: [0.0] * horizon for bu in bus}
    for i in sorted_inits:
        if i.get('lever') == 'cost_reduction' and i.get('_annualSaving', 0) > 0:
            factors = i.get('_yearlyFactors', [0.30, 0.70, 0.95])
            # P2-1: Attribute location savings to BUs
            i_channels = set(i.get('channels', []))
            i_target_bus = set(i.get('targetBUs', []))
            bv = {}
            for q in queues:
                if q.get('channel', '') in i_channels and (not i_target_bus or q.get('bu', '') in i_target_bus):
                    b = q.get('bu', 'Default')
                    bv[b] = bv.get(b, 0) + q['volume']
            tbv = sum(bv.values()) or 1
            for yr in range(horizon):
                f = factors[yr] if yr < len(factors) else (factors[-1] if factors else 0.95)
                location_yearly[yr] += i['_annualSaving'] * f
                for bu_name, vol in bv.items():
                    if bu_name in bu_location_yearly:
                        bu_location_yearly[bu_name][yr] += i['_annualSaving'] * f * (vol / tbv)
    
    for yr in range(horizon):
        reduction = sum(ri['yearly'][yr] for ri in role_impact.values())
        base_cost = sum(r['headcount'] * r['costPerFTE'] for r in roles)
        saving = sum(role_impact[r['role']]['yearly'][yr] * r['costPerFTE']
                     for r in roles if r['role'] in role_impact)
        
        # Add time-phased location savings
        saving += location_yearly[yr]
        
        wg = params['wageInflation']
        inf_cost = base_cost * (1 + wg) ** (yr + 1)
        fut_cost = (base_cost - saving) * (1 + wg) ** (yr + 1)
        net = inf_cost - fut_cost
        cum += net
        npv_f = 1 / (1 + params['discountRate']) ** (yr + 1)
        yearly.append({
            'year': yr + 1, 'fteReduction': round(reduction),
            'finalFTE': total_fte - round(reduction),
            'annualSaving': round(net), 'cumSaving': round(cum),
            'npv': round(net * npv_f),
            'inflatedCost': round(inf_cost), 'futureCost': round(fut_cost),
        })
    
    total_npv = sum(y['npv'] for y in yearly)
    total_saving = sum(y['annualSaving'] for y in yearly)
    total_red = yearly[-1]['fteReduction'] if yearly else 0
    
    # Gross FTE reduction (sum of all _fteImpact, pre-ramp) vs Net (waterfall Year-N, post-ramp)
    gross_fte_reduction = round(sum(i.get('_fteImpact', 0) for i in enabled), 1)
    # NOTE: `total_red` is the post-ramp figure (correct for waterfall/financials)
    # `gross_fte_reduction` is the full potential if all ramps complete at 100%
    # Cards should use `total_red` (post-ramp) to stay consistent with waterfall
    
    # ── Safe contribution % ──
    tot_isav = sum(i.get('_annualSaving', 0) for i in enabled if i.get('_annualSaving', 0) > 0)
    for i in enabled:
        s = i.get('_annualSaving', 0)
        i['_contributionPct'] = min(100, max(0, round(s / max(tot_isav, 1) * 100, 1))) if tot_isav > 0 else 0
    for i in initiatives:
        if not i.get('enabled'):
            i['_contributionPct'] = 0
    
    # ══════════════════════════════════════════════════════════════
    # V7: CALIBRATED INVESTMENT MODEL
    # ══════════════════════════════════════════════════════════════
    # Fixes:
    #   1. Size scaling — Excel costs calibrated ~3,000 FTE; scale by √(actual/ref)
    #   2. Tech stack deduction — reduce tech cost if client already has platform
    #   3. Platform pooling — shared platform family → 1st init full, rest marginal
    #   4. Payback — use steady-state saving, not Year-1 ramp
    # ══════════════════════════════════════════════════════════════
    
    tc = data.get('techInvestment', {}).get('costs', {})
    cd = data.get('techInvestment', {}).get('cost_defaults', {})
    tech_stack = data.get('techInvestment', {}).get('tech_stack', [])
    
    # ── 1. Size scaling ──
    import math as _math
    _total_fte = data.get('totalFTE', sum(r['headcount'] for r in data.get('roles', [])))
    _ref_fte = params.get('investmentRefFTE', 3000)
    size_scale = max(0.30, min(2.0, _math.sqrt(_total_fte / max(_ref_fte, 1))))
    impl_scale = max(0.50, 0.40 + 0.60 * size_scale)  # impl has fixed component
    
    # ── 2. Tech stack coverage lookup ──
    _stack_cov = {}
    for ts in tech_stack:
        cat = (ts.get('category') or '').lower().strip()
        cov = float(ts.get('coverage', 0) or 0) / 100.0
        status = (ts.get('status') or '').lower()
        if status in ('active', 'deploying', 'pilot'):
            _stack_cov[cat] = max(_stack_cov.get(cat, 0), cov)
    
    _INIT_PLATFORM = {
        'AI01':'chatbot','AI04':'chatbot','AI09':'chatbot','AI10':'chatbot',
        'AI14':'chatbot','AI25':'chatbot','AI18':'chatbot',
        'AI02':'agent assist','AI05':'agent assist','AI27':'agent assist',
        'AI06':'speech analytics','AI07':'qa tool','AI15':'speech analytics','AI21':'speech analytics',
        'AI08':'knowledge base','AI19':'knowledge base',
        'AI12':'wfm','OP04':'wfm','OP11':'wfm',
        'AI22':'crm','AI16':'crm',
        'AI13':'rpa','AI28':'rpa',
        'AI03':'ccaas platform','AI23':'ccaas platform','AI24':'ccaas platform','AI11':'ccaas platform',
        'OP16':'bi/reporting','OP09':'bi/reporting',
    }
    
    def _ts_discount(iid):
        plat = _INIT_PLATFORM.get(iid)
        if not plat:
            return 0.0
        cov = _stack_cov.get(plat, 0)
        return 0.50 if cov >= 0.70 else 0.25 if cov >= 0.40 else 0.0
    
    # ── 3. Platform pooling families ──
    _FAMILIES = {
        'conv_ai': ['AI01','AI04','AI09','AI10','AI14','AI25','AI18'],
        'assist_ai': ['AI02','AI05','AI27','AI06'],
        'analytics': ['AI07','AI21','AI15'],
        'knowledge': ['AI08','AI19'],
        'wfm': ['AI12','OP04','OP11'],
        'crm_ext': ['AI22','AI16'],
        'rpa': ['AI13','AI28'],
        'ccaas': ['AI03','AI23','AI24','AI11'],
    }
    _init_family = {}
    for fam, ids in _FAMILIES.items():
        for iid in ids:
            _init_family[iid] = fam
    _family_costed = set()
    
    tech_inv = 0
    ann_maint = 0
    inv_items = []
    
    # Process highest-score first so the best initiative anchors each platform family
    for i in sorted(enabled, key=lambda x: x.get('score', 0), reverse=True):
        iid = i['id']
        
        if iid in tc:
            t = tc[iid]
            raw_tech = t.get('techCost', t['totalOneTime'] * 0.65)
            raw_impl = t.get('implCost', t['totalOneTime'] * 0.35)
            raw_annual = t['annualCost']
        else:
            eff = i.get('effort', 'medium').lower()
            d = cd.get(eff, {'techCost': 100000, 'annualCost': 30000, 'implCost': 50000})
            raw_tech = d['techCost']
            raw_impl = d.get('implCost', 0)
            raw_annual = d['annualCost']
        
        # Apply size scaling
        s_tech = raw_tech * size_scale
        s_impl = raw_impl * impl_scale
        s_annual = raw_annual * size_scale
        
        # Apply tech stack discount
        tsd = _ts_discount(iid)
        s_tech *= (1.0 - tsd)
        s_annual *= (1.0 - tsd * 0.50)
        
        # Apply platform pooling
        fam = _init_family.get(iid)
        is_pooled = False
        if fam:
            if fam in _family_costed:
                s_tech *= 0.25
                s_annual *= 0.40
                is_pooled = True
            else:
                _family_costed.add(fam)
        
        one_time = round(s_tech + s_impl)
        recurring = round(s_annual)
        tech_inv += one_time
        ann_maint += recurring
        
        src = 'technology_investment.xlsx' if iid in tc else 'Default estimate'
        adj = []
        if size_scale < 0.95:
            adj.append(f'size×{size_scale:.2f}')
        if tsd > 0:
            adj.append(f'stack-{tsd:.0%}')
        if is_pooled:
            adj.append('pooled')
        if adj:
            src += f' ({", ".join(adj)})'
        
        inv_items.append({
            'id': iid, 'name': i['name'], 'layer': i['layer'],
            'oneTime': one_time, 'recurring': recurring,
            'rawOneTime': round(raw_tech + raw_impl), 'rawRecurring': round(raw_annual),
            'source': src,
        })
    
    # Re-sort to match enabled order
    _inv_order = {i['id']: idx for idx, i in enumerate(enabled)}
    inv_items.sort(key=lambda x: _inv_order.get(x['id'], 999))
    
    p = params
    cm = tech_inv * p.get('changeMgmtPct', 0.10)
    tr = tech_inv * p.get('trainingPct', 0.05)
    ct = tech_inv * p.get('contingencyPct', 0.10)
    total_inv = tech_inv + cm + tr + ct
    roi = ((total_npv - total_inv) / max(total_inv, 1)) * 100 if total_inv > 0 else 0
    roi_g = ((total_saving - total_inv) / max(total_inv, 1)) * 100 if total_inv > 0 else 0
    # V7: payback uses steady-state saving (last year), not Year-1 ramp
    steady_sav = yearly[-1]['annualSaving'] if yearly else 0
    payback = (total_inv / max(steady_sav, 1)) * 12 if steady_sav > 0 else 0
    irr = _estimate_irr([-total_inv] + [y['annualSaving'] for y in yearly])
    
    # Investment yearly
    inv_yearly = []
    for yr in range(1, horizon + 1):
        otp = 0.60 if yr == 1 else 0.30 if yr == 2 else 0.10
        inv_yearly.append({
            'year': yr, 'oneTime': round(tech_inv * otp), 'recurring': round(ann_maint),
            'changeMgmt': round(cm / horizon), 'training': round(tr / horizon),
            'contingency': round(ct / horizon),
            'total': round(tech_inv * otp + ann_maint + (cm + tr + ct) / horizon),
        })
    
    # Scenarios & sensitivity
    scenarios = {} if _skip_scenarios else _compute_scenarios(data, initiatives, yearly, total_npv, total_red, total_inv, total_saving)
    sensitivity = [] if _skip_sensitivity else _run_sensitivity(data, initiatives, total_npv)
    
    # ── Layer-level FTE breakdown ──
    layer_fte = {'AI & Automation': 0, 'Operating Model': 0, 'Location Strategy': 0}
    layer_saving = {'AI & Automation': 0, 'Operating Model': 0, 'Location Strategy': 0}
    layer_fte_migrated = {'AI & Automation': 0, 'Operating Model': 0, 'Location Strategy': 0}
    for i in enabled:
        layer = i.get('layer', 'AI & Automation')
        if layer in layer_fte:
            layer_fte[layer] += i.get('_fteImpact', 0)
            layer_saving[layer] += i.get('_annualSaving', 0)
            layer_fte_migrated[layer] += i.get('_poolConsumed', 0)
    layer_fte = {k: round(v, 1) for k, v in layer_fte.items()}
    layer_saving = {k: round(v) for k, v in layer_saving.items()}
    layer_fte_migrated = {k: round(v, 1) for k, v in layer_fte_migrated.items()}
    
    # ── Pool utilization summary ──
    pool_utilization = {}

    # ── P2-1: Build per-BU impact summary ──
    # Add location savings to BU impact tracking
    for bu_name in bus:
        if bu_name in bu_impact and bu_name in bu_location_yearly:
            for yr in range(horizon):
                bu_impact[bu_name]['yearly_saving'][yr] += bu_location_yearly[bu_name][yr]
    
    bu_summary = {}
    
    for bu_name, bi in bu_impact.items():
        bu_yearly = []
        for yr in range(horizon):
            fte_red = round(bi['yearly_fte'][yr], 1)
            saving = round(bi['yearly_saving'][yr])
            bu_yearly.append({
                'year': yr + 1,
                'fteReduction': fte_red,
                'annualSaving': saving,
            })
        bu_summary[bu_name] = {
            'baselineFTE': round(bi['baseline_fte'], 1),
            'weightedCost': round(bi['weighted_cost']),
            'yearly': bu_yearly,
            'totalFTEReduction': round(bi['yearly_fte'][-1], 1) if bi['yearly_fte'] else 0,
            'totalSaving': round(sum(y['annualSaving'] for y in bu_yearly)),
            'initiativeAttrib': sorted(bi.get('initiative_attrib', []), key=lambda x: x['fte'], reverse=True),
            'poolUtilization': bu_pool_util.get(bu_name, {}),
        }
    for pk, pv in pools.items():
        if pk == 'csat_experience':
            # CSAT pool is revenue-denominated, not FTE
            ceil_val = pv.get('ceiling_total_value', 0)
            cons_val = pv.get('consumed_revenue', 0)
            pool_utilization[pk] = {
                'ceiling_fte': 0,  # not FTE
                'consumed_fte': 0,
                'remaining_fte': 0,
                'utilization_pct': round(cons_val / max(ceil_val, 1) * 100, 1),
                # CSAT-specific fields
                'ceiling_csat_points': pv.get('ceiling_csat_points', 0),
                'consumed_csat_points': pv.get('consumed_csat_points', 0),
                'remaining_csat_points': pv.get('remaining_csat_points', 0),
                'ceiling_revenue': round(ceil_val),
                'consumed_revenue': round(cons_val),
                'remaining_revenue': round(max(0, ceil_val - cons_val)),
                'current_csat': pv.get('current_csat', 0),
                'benchmark_csat': pv.get('benchmark_csat', 0),
                'portfolio_gap': pv.get('portfolio_gap', 0),
                'breakdown': pv.get('breakdown', []),
            }
        else:
            ceiling = pv.get('ceiling_fte', 0)
            remaining = pv.get('remaining_fte', 0)
            consumed = ceiling - remaining
            pool_utilization[pk] = {
                'ceiling_fte': ceiling,
                'consumed_fte': round(consumed, 1),
                'remaining_fte': round(remaining, 1),
                'utilization_pct': round(consumed / max(ceiling, 0.1) * 100, 1),
            }
    
    # CR-016 fix: Distribute pool ceilings across BUs (must run AFTER pool_utilization is populated)
    total_base_fte = sum(bi['baseline_fte'] for bi in bu_impact.values()) or 1
    for bu_name, bi in bu_impact.items():
        bu_share = bi['baseline_fte'] / total_base_fte
        if bu_name in bu_pool_util:
            for pk in bu_pool_util[bu_name]:
                pu = pool_utilization.get(pk, {})
                bu_pool_util[bu_name][pk]['ceiling'] = round((pu.get('ceiling_fte', 0)) * bu_share, 1)
    
    return {
        'roleImpact': {k: {'baseline': v['baseline'], 'yearly': [round(y) for y in v['yearly']]}
                       for k, v in role_impact.items()},
        'layerFTE': layer_fte, 'layerSaving': layer_saving, 'layerFTEMigrated': layer_fte_migrated,
        'yearly': yearly, 'totalNPV': round(total_npv), 'totalSaving': round(total_saving),
        'totalReduction': total_red,
        'grossFTEReduction': gross_fte_reduction,
        'techInvestment': round(tech_inv), 'annualMaintenance': round(ann_maint),
        'changeMgmt': round(cm), 'training': round(tr), 'contingency': round(ct),
        'totalInvestment': round(total_inv),
        'roi': round(roi, 1), 'roiGross': round(roi_g, 1), 'payback': round(payback, 1),
        'irr': round(irr, 1),
        'scenarios': scenarios, 'enabledInits': [i['id'] for i in enabled],
        'leverAccum': {},  # deprecated — replaced by pool utilization
        'sensitivity': sensitivity,
        'investmentItems': inv_items, 'investmentYearly': inv_yearly,
        'investmentSummary': {
            'totalTech': round(tech_inv), 'changeMgmt': round(cm), 'training': round(tr),
            'contingency': round(ct), 'grandTotal': round(total_inv),
            'annualRecurring': round(ann_maint),
        },
        # ── New v4 fields ──
        'poolUtilization': pool_utilization,
        'poolSummary': pool_summary,
        'auditTrail': audit_trail,
        # ── v10: CSAT / CX fields ──
        'csatSummary': csat_summary,
        # ── P2-1: Dimensional fields ──
        'buSummary': bu_summary,
        'locations': data.get('locations', ['Onshore']),
        'sourcingTypes': data.get('sourcingTypes', ['In-house']),
    }


def _compute_scenarios(data, initiatives, yearly, base_npv, base_red, base_inv, base_sav):
    """CR-018: Three scenarios with actual re-computation."""
    import copy as _c
    scenarios = {}
    yr3 = yearly[-1]['annualSaving'] if yearly else 0
    scenarios['base'] = {'label':'Base Case','description':'Expected values from diagnostic',
        'npv':round(base_npv),'fteReduction':round(base_red),'investment':round(base_inv),
        'irr':round(_estimate_irr([-base_inv]+[y['annualSaving'] for y in yearly]),1),
        'annualSaving':round(yr3),'totalSaving':round(base_sav)}

    for label, amult, imult, ramp_mult in [('conservative',0.70,1.15, 1.50),
                                            ('aggressive',1.30,0.90, 0.60)]:
        try:
            mi = _c.deepcopy(initiatives)
            for i in mi:
                i['adoption'] = min(1.0, i.get('adoption',0.8)*amult)
                # Adjust ramp duration (conservative = slower ramp, aggressive = faster)
                i['ramp'] = max(2, round(i.get('ramp', 12) * ramp_mult))
            wf = run_waterfall(data, mi, _skip_sensitivity=True, _skip_scenarios=True)
            ai = base_inv * imult; sn = wf['totalNPV']; y3 = wf['yearly'][-1]['annualSaving'] if wf['yearly'] else 0
            sc_irr = _estimate_irr([-ai]+[y['annualSaving'] for y in wf['yearly']])
            scenarios[label] = {'label':label.capitalize(),'description':f'{"Lower" if amult<1 else "Higher"} adoption, {"higher" if imult>1 else "lower"} costs',
                'npv':round(sn),'fteReduction':round(wf['totalReduction']),'investment':round(ai),
                'irr':round(sc_irr,1),
                'annualSaving':round(y3),'totalSaving':round(wf['totalSaving'])}
        except Exception:
            m = amult; ai = base_inv*imult
            scenarios[label] = {'label':label.capitalize(),'description':'Estimated','npv':round(base_npv*m),
                'fteReduction':round(base_red*m),'investment':round(ai),
                'irr':0,'annualSaving':round(yr3*m),'totalSaving':round(base_sav*m)}
    return scenarios


def _estimate_irr(cashflows, guess=0.10, max_iter=100):
    if not cashflows or all(cf==0 for cf in cashflows): return 0.0
    rate = guess
    for _ in range(max_iter):
        npv = sum(cf/(1+rate)**t for t,cf in enumerate(cashflows))
        dnpv = sum(-t*cf/(1+rate)**(t+1) for t,cf in enumerate(cashflows))
        if abs(dnpv) < 1e-10: break
        rate = max(-0.5, min(10.0, rate - npv/dnpv))
        if abs(npv) < 1: break
    return round(rate*100, 1)


def _run_sensitivity(data, initiatives, base_npv):
    import copy as _c
    if abs(base_npv) < 1: return []
    variables = [('Volume Growth','volumeGrowth',0.20),('Wage Inflation','wageInflation',0.20),
                 ('Discount Rate','discountRate',0.20),('Attrition Rate','attritionMonthly',0.20),
                 ('Adoption Speed','_adoption',0.20),('Redeployment %','redeploymentPct',0.20)]
    results = []
    for label, pk, sf in variables:
        try:
            if pk == '_adoption':
                li = _c.deepcopy(initiatives); hi = _c.deepcopy(initiatives)
                for i in li: i['adoption'] = i.get('adoption',0.8)*(1-sf)
                for i in hi: i['adoption'] = min(1.0, i.get('adoption',0.8)*(1+sf))
                ln = run_waterfall(data, li, _skip_sensitivity=True, _skip_scenarios=True)['totalNPV']; hn = run_waterfall(data, hi, _skip_sensitivity=True, _skip_scenarios=True)['totalNPV']
            else:
                bv = data['params'].get(pk, 0)
                dl = _c.deepcopy(data); dl['params'][pk] = bv*(1-sf)
                dh = _c.deepcopy(data); dh['params'][pk] = bv*(1+sf)
                ln = run_waterfall(dl, initiatives, _skip_sensitivity=True, _skip_scenarios=True)['totalNPV']
                hn = run_waterfall(dh, initiatives, _skip_sensitivity=True, _skip_scenarios=True)['totalNPV']
            sw = abs(hn-ln)
            results.append({'variable':label,'swing':round(sw),'lowNPV':round(ln),'highNPV':round(hn),
                           'baseNPV':round(base_npv),'swingPct':round(sw/max(abs(base_npv),1)*100,1)})
        except Exception:
            sp = 15 if pk in ('discountRate','wageInflation') else 10
            results.append({'variable':label,'swing':round(base_npv*sp/50),'lowNPV':round(base_npv*(1-sp/100)),
                           'highNPV':round(base_npv*(1+sp/100)),'baseNPV':round(base_npv),'swingPct':sp*2})
    results.sort(key=lambda x: x['swing'], reverse=True)
    return results
