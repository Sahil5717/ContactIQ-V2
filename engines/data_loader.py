"""
ContactIQ — Data Loader & ETL Engine
Reads raw CCaaS, CRM, HRIS, Survey exports + consultant config files.
Transforms into analytical schemas matching spec Section 3.
Industry-agnostic: all industry/client labels come from parameters.xlsx.
"""
import os, re, math, logging
from collections import defaultdict
from datetime import datetime, timedelta
import openpyxl

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

# ── Path Override System ──
# Allows uploaded files to replace default data files.
# Keys are relative paths like 'raw/ccaas_interaction_records.xlsx'
# or category names like 'ccaas_interactions'.
_PATH_OVERRIDES = {}

# Maps file categories to their default relative paths
_CATEGORY_TO_PATH = {
    'ccaas_interactions': 'raw/ccaas_interaction_records.xlsx',
    'crm_cases': 'raw/crm_case_export.xlsx',
    'hr_workforce': 'raw/hr_workforce_data.xlsx',
    'interaction_transcripts': 'raw/interaction_transcripts.xlsx',
    'survey_responses': 'raw/survey_responses.xlsx',
    'parameters': 'config/parameters.xlsx',
    'benchmarks': 'config/benchmarks.xlsx',
    'technology_investment': 'config/technology_investment.xlsx',
}


def set_path_overrides(overrides):
    """Set file path overrides. Keys can be category names or relative paths."""
    global _PATH_OVERRIDES
    _PATH_OVERRIDES = dict(overrides) if overrides else {}


def _resolve_path(relative_path):
    """Resolve a data file path, checking overrides first."""
    # Check by relative path
    if relative_path in _PATH_OVERRIDES:
        p = _PATH_OVERRIDES[relative_path]
        if os.path.exists(p):
            return p
    # Check by category name
    for cat, default_rel in _CATEGORY_TO_PATH.items():
        if default_rel == relative_path and cat in _PATH_OVERRIDES:
            p = _PATH_OVERRIDES[cat]
            if os.path.exists(p):
                return p
    # Default
    return os.path.join(DATA_DIR, relative_path)

# ── Queue Name -> (BU, Intent, Channel) Mapping ──
# These map raw CCaaS queue names to standardised (BU, Intent, Channel).
# Extend or replace via config when onboarding a new client dataset.
QUEUE_MAP = {
    'CSR_BILL_PAY_EN': ('Consumer', 'Billing & Payments', 'Voice'),
    'CSR_BILL_PAY_ES': ('Consumer', 'Billing & Payments', 'Voice'),
    'CSR_TECHSUP_T1': ('Consumer', 'Technical Support', 'Voice'),
    'CSR_TECHSUP_T2': ('Consumer', 'Technical Support', 'Voice'),
    'CSR_NETWORK_OUT': ('Consumer', 'Network Outage', 'Voice'),
    'CSR_PLAN_CHG': ('Consumer', 'Plan Change', 'Voice'),
    'CSR_DEVICE_TS': ('Consumer', 'Device Troubleshooting', 'Voice'),
    'CSR_CANCEL': ('Consumer', 'Service Cancellation', 'Voice'),
    'CSR_ROAMING': ('Consumer', 'Roaming', 'Voice'),
    'CSR_DATA_USE': ('Consumer', 'Data Usage', 'Voice'),
    'CSR_RENEW': ('Consumer', 'Contract Renewal', 'Voice'),
    'CSR_COMPLAINT': ('Consumer', 'Complaints', 'Voice'),
    'CSR_NEW_CONN': ('Consumer', 'New Connection', 'Voice'),
    'CSR_SIM_REPL': ('Consumer', 'SIM Replacement', 'Voice'),
    'CSR_PORT': ('Consumer', 'Number Portability', 'Voice'),
    'CSR_VAS': ('Consumer', 'Value-Added Services', 'Voice'),
    'CSR_GEN_ENQ': ('Consumer', 'General Enquiry', 'Voice'),
    'BIZ_BILL_PAY': ('Business', 'Billing & Payments', 'Voice'),
    'BIZ_TECHSUP': ('Business', 'Technical Support', 'Voice'),
    'BIZ_NETWORK': ('Business', 'Network Outage', 'Voice'),
    'BIZ_PLAN_CHG': ('Business', 'Plan Change', 'Voice'),
    'BIZ_CANCEL': ('Business', 'Service Cancellation', 'Voice'),
    'BIZ_RENEW': ('Business', 'Contract Renewal', 'Voice'),
    'BIZ_GEN_ENQ': ('Business', 'General Enquiry', 'Voice'),
    'ENT_VOICE_BILLING': ('Enterprise', 'Billing & Payments', 'Voice'),
    'ENT_VOICE_TECHSUP': ('Enterprise', 'Technical Support', 'Voice'),
    'ENT_GEN_ENQ': ('Enterprise', 'General Enquiry', 'Voice'),
    'CHAT_BILL': ('Consumer', 'Billing & Payments', 'Chat'),
    'CHAT_TECHSUP': ('Consumer', 'Technical Support', 'Chat'),
    'CHAT_PLAN': ('Consumer', 'Plan Change', 'Chat'),
    'CHAT_GEN': ('Consumer', 'General Enquiry', 'Chat'),
    'CHAT_CANCEL': ('Consumer', 'Service Cancellation', 'Chat'),
    'CHAT_DEVICE': ('Consumer', 'Device Troubleshooting', 'Chat'),
    'CHAT_DATA': ('Consumer', 'Data Usage', 'Chat'),
    'CHAT_VAS': ('Consumer', 'Value-Added Services', 'Chat'),
    'BIZ_CHAT_ALL': ('Business', 'General Enquiry', 'Chat'),
    'EMAIL_BILLING': ('Consumer', 'Billing & Payments', 'Email'),
    'EMAIL_COMPLAINT': ('Consumer', 'Complaints', 'Email'),
    'EMAIL_TECHSUP': ('Consumer', 'Technical Support', 'Email'),
    'EMAIL_GENERAL': ('Consumer', 'General Enquiry', 'Email'),
    'BIZ_EMAIL': ('Business', 'General Enquiry', 'Email'),
    'IVR_MAIN': ('Consumer', 'General Enquiry', 'IVR'),
    'IVR_BILLING': ('Consumer', 'Billing & Payments', 'IVR'),
    'IVR_TECHSUP': ('Consumer', 'Technical Support', 'IVR'),
    'APP_SELF_BILL': ('Consumer', 'Billing & Payments', 'App/Self-Service'),
    'APP_SELF_DATA': ('Consumer', 'Data Usage', 'App/Self-Service'),
    'APP_SELF_PLAN': ('Consumer', 'Plan Change', 'App/Self-Service'),
    'SM_GEN_ENQ': ('Consumer', 'General Enquiry', 'Social Media'),
    'SM_COMPLAINT': ('Consumer', 'Complaints', 'Social Media'),
    'WA_BILL': ('Consumer', 'Billing & Payments', 'SMS/WhatsApp'),
    'WA_TECHSUP': ('Consumer', 'Technical Support', 'SMS/WhatsApp'),
    'WA_GEN': ('Consumer', 'General Enquiry', 'SMS/WhatsApp'),
    'RETAIL_ALL': ('Consumer', 'General Enquiry', 'Retail/Walk-in'),
    'RETAIL_SALES': ('Consumer', 'New Connection', 'Retail/Walk-in'),
}

CHANNEL_SYNONYMS = {
    'VOICE': 'Voice', 'Phone': 'Voice', 'Phone Call': 'Voice', 'Telephone': 'Voice',
    'CHAT': 'Chat', 'Web': 'Chat', 'Live Chat': 'Chat', 'Webchat': 'Chat',
    'EMAIL': 'Email', 'Email': 'Email', 'E-mail': 'Email',
    'IVR': 'IVR', 'Automated Phone': 'IVR', 'Interactive Voice': 'IVR',
    'APP': 'App/Self-Service', 'Mobile App': 'App/Self-Service', 'Self-Service': 'App/Self-Service',
    'SOCIAL': 'Social Media', 'Social': 'Social Media', 'Facebook': 'Social Media', 'Twitter': 'Social Media',
    'SMS': 'SMS/WhatsApp', 'WhatsApp': 'SMS/WhatsApp', 'Text': 'SMS/WhatsApp', 'Messaging': 'SMS/WhatsApp',
    'RETAIL': 'Retail/Walk-in', 'Walk-In': 'Retail/Walk-in', 'Store Visit': 'Retail/Walk-in', 'In-Store': 'Retail/Walk-in',
}

TITLE_ROLE_MAP = {
    'agent l1': 'Agent L1', 'customer service representative': 'Agent L1',
    'customer care associate': 'Agent L1', 'contact centre agent': 'Agent L1',
    'csr': 'Agent L1', 'service desk agent': 'Agent L1',
    'contact center agent': 'Agent L1', 'customer support agent': 'Agent L1',
    'agent l2': 'Agent L2 / Specialist', 'senior customer service': 'Agent L2 / Specialist',
    'technical support specialist': 'Agent L2 / Specialist', 'specialist': 'Agent L2 / Specialist',
    'senior csr': 'Agent L2 / Specialist', 'escalation specialist': 'Agent L2 / Specialist',
    'agent l3': 'Agent L3 / Expert', 'expert': 'Agent L3 / Expert',
    'compliance specialist': 'Agent L3 / Expert',
    'supervisor': 'Supervisor / Team Lead', 'team lead': 'Supervisor / Team Lead',
    'team leader': 'Supervisor / Team Lead',
    'qa analyst': 'QA Analyst', 'quality analyst': 'QA Analyst', 'quality assurance': 'QA Analyst',
    'wfm analyst': 'WFM Analyst', 'workforce management': 'WFM Analyst', 'wfm': 'WFM Analyst',
    'scheduler': 'WFM Analyst', 'workforce planner': 'WFM Analyst',
    'back-office': 'Back-Office / Processing', 'processing': 'Back-Office / Processing',
    'back office': 'Back-Office / Processing', 'case processor': 'Back-Office / Processing',
    'trainer': 'Trainer', 'training': 'Trainer', 'learning': 'Trainer',
    'knowledge manager': 'Knowledge Manager', 'knowledge': 'Knowledge Manager',
    'content manager': 'Knowledge Manager',
    'reporting': 'Reporting / Analytics', 'analytics': 'Reporting / Analytics',
    'bi analyst': 'Reporting / Analytics', 'data analyst': 'Reporting / Analytics',
}

INTENT_COMPLEXITY = {
    'Billing & Payments': 0.30, 'Technical Support': 0.65, 'Network Outage': 0.70,
    'Plan Change': 0.25, 'Device Troubleshooting': 0.60, 'Service Cancellation': 0.55,
    'Roaming': 0.45, 'Data Usage': 0.20, 'Contract Renewal': 0.35,
    'Complaints': 0.70, 'New Connection': 0.40, 'SIM Replacement': 0.15,
    'Number Portability': 0.50, 'Value-Added Services': 0.30, 'General Enquiry': 0.15,
}

CHANNEL_CAPABILITY = {
    'Voice': 0.90, 'IVR': 0.30, 'Chat': 0.75, 'Email': 0.60,
    'App/Self-Service': 0.50, 'Social Media': 0.40, 'SMS/WhatsApp': 0.55, 'Retail/Walk-in': 0.85,
}
CHANNEL_COST_TIER = {
    'Voice': 1.0, 'IVR': 0.15, 'Chat': 0.55, 'Email': 0.45,
    'App/Self-Service': 0.12, 'Social Media': 0.35, 'SMS/WhatsApp': 0.25, 'Retail/Walk-in': 1.20,
}

# ── KPI polarity — CR-005: proper direction logic ──
KPI_DIRECTION = {
    'CSAT': 'higher', 'FCR': 'higher',
    'AHT': 'lower', 'CPC': 'lower',
    'Repeat': 'lower', 'Escalation': 'lower', 'CES': 'lower',
}


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def normalize_channel(raw):
    if not raw:
        return None
    raw_clean = str(raw).strip()
    return CHANNEL_SYNONYMS.get(raw_clean, CHANNEL_SYNONYMS.get(raw_clean.upper(), raw_clean))


def fuzzy_match_role(title):
    if not title:
        return 'Agent L1'
    t = str(title).strip().lower()
    for keyword, role in TITLE_ROLE_MAP.items():
        if keyword in t:
            return role
    return 'Agent L1'


def read_xlsx_sheet(filepath, sheet_name=None):
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb[sheet_name] if sheet_name else wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if len(rows) < 2:
        return []
    headers = [str(h).strip() if h else f'col_{i}' for i, h in enumerate(rows[0])]
    return [dict(zip(headers, row)) for row in rows[1:]]


def load_parameters():
    """Load enterprise parameters from config/parameters.xlsx.
    Industry and clientName are ONLY sourced from the file — no hardcoded defaults.
    """
    path = _resolve_path('config/parameters.xlsx')
    if not os.path.exists(path):
        return _default_params()
    rows = read_xlsx_sheet(path)
    p = _default_params()
    param_map = {
        'Program Name': 'programName', 'Client Name': 'clientName',
        'Industry': 'industry',
        'Planning Horizon': 'horizon', 'Discount Rate': 'discountRate',
        'Volume Growth %': 'volumeGrowth', 'Wage Inflation %': 'wageInflation',
        'Attrition Rate (monthly)': 'attritionMonthly', 'Redeployment %': 'redeploymentPct',
        'Currency': 'currency', 'Salary Onshore': 'salaryOnshore',
        'Salary Nearshore': 'salaryNearshore', 'Salary Offshore': 'salaryOffshore',
        'Salary 3rd Party': 'salary3rdParty', 'Hiring Cost': 'hiringCost',
        'Customer Base': 'customerBase', 'Revenue per Customer': 'revenuePerCustomer',
        'Annual Churn Rate': 'annualChurnRate', 'CSAT Baseline': 'csatBaseline',
        'CSAT Target': 'csatTarget', 'FCR Baseline': 'fcrBaseline', 'FCR Target': 'fcrTarget',
        'Change Mgmt Cost %': 'changeMgmtPct', 'Training Cost %': 'trainingPct',
        'Contingency %': 'contingencyPct',
        'Volume Annualization Factor': 'volumeAnnualizationFactor',
    }
    for row in rows:
        key = str(row.get('Parameter', '')).strip()
        val = row.get('Value')
        if key in param_map and val is not None:
            mapped = param_map[key]
            if isinstance(val, str) and mapped not in ('programName', 'industry', 'currency', 'clientName'):
                try:
                    val = float(val)
                except ValueError:
                    pass
            if mapped == 'horizon':
                val = int(val)
            p[mapped] = val
    return p


def _default_params():
    """Industry-agnostic defaults — no hardcoded telecom."""
    return {
        'programName': 'CC Transformation', 'clientName': 'Client',
        'industry': 'Custom', 'horizon': 3,
        'discountRate': 0.10, 'volumeGrowth': 0.02, 'wageInflation': 0.03,
        'attritionMonthly': 0.03, 'redeploymentPct': 0.10, 'currency': 'USD',
        'salaryOnshore': 60000, 'salaryNearshore': 30000, 'salaryOffshore': 18000,
        'salary3rdParty': 45000, 'hiringCost': 5000, 'customerBase': 0,
        'revenuePerCustomer': 0, 'annualChurnRate': 0.12, 'csatBaseline': 3.5,
        'csatTarget': 4.2, 'fcrBaseline': 0.70, 'fcrTarget': 0.82,
        'changeMgmtPct': 0.10, 'trainingPct': 0.05, 'contingencyPct': 0.10,
        # CR-003: default ramp-up percentages
        'rampYear1': 0.30, 'rampYear2': 0.70, 'rampYear3': 0.95,
        # CR-021 v5: explicit volume annualization (12 = monthly data, 1 = annual)
        'volumeAnnualizationFactor': 12,
    }


def load_benchmarks():
    path = _resolve_path('config/benchmarks.xlsx')
    defaults = {
        'CSAT': {'Voice': 4.0, 'Chat': 4.0, 'Email': 3.8, 'IVR': 3.5, '_dir': 'higher', '_default': 3.8},
        'FCR': {'Voice': 0.78, 'Chat': 0.80, 'Email': 0.75, 'IVR': 0.70, '_dir': 'higher', '_default': 0.75},
        'AHT': {'Voice': 5.0, 'Chat': 8.0, 'Email': 12.0, 'IVR': 3.0, '_dir': 'lower', '_default': 6.0},
        'CPC': {'Voice': 8.50, 'Chat': 5.00, 'Email': 4.00, 'IVR': 1.20, '_dir': 'lower', '_default': 5.0},
        'Repeat': {'Voice': 0.10, 'Chat': 0.12, 'Email': 0.15, 'IVR': 0.18, '_dir': 'lower', '_default': 0.12},
        'Escalation': {'Voice': 0.08, 'Chat': 0.06, 'Email': 0.10, 'IVR': 0.12, '_dir': 'lower', '_default': 0.08},
        'CES': {'Voice': 2.5, 'Chat': 2.0, 'Email': 2.5, 'IVR': 1.5, '_dir': 'lower', '_default': 2.2},
    }
    benchmarks = {'_raw': [], '_defaults': defaults}
    if os.path.exists(path):
        rows = read_xlsx_sheet(path, 'Benchmarks')
        for r in rows:
            scope = str(r.get('Scope', '')).strip().lower()
            intent = r.get('Intent')
            channel = r.get('Channel')
            metric = str(r.get('Metric', '')).strip()
            avg = r.get('Industry Average')
            tq = r.get('Top Quartile')
            bq = r.get('Bottom Quartile')
            source = r.get('Source', 'System Default')
            if metric and avg is not None:
                benchmarks['_raw'].append({
                    'scope': scope, 'intent': intent, 'channel': channel,
                    'metric': metric, 'average': float(avg),
                    'topQuartile': float(tq) if tq else None,
                    'bottomQuartile': float(bq) if bq else None,
                    'source': source,
                })
    return benchmarks


def resolve_benchmark(benchmarks, metric, intent=None, channel=None):
    raw = benchmarks.get('_raw', [])
    defaults = benchmarks.get('_defaults', {})
    for r in raw:
        if r['metric'] == metric and r['intent'] == intent and r['channel'] == channel and r['scope'] == 'intent-channel':
            return r['average'], r.get('topQuartile'), r.get('source', 'File')
    for r in raw:
        if r['metric'] == metric and r['intent'] == intent and r['scope'] == 'intent':
            return r['average'], r.get('topQuartile'), r.get('source', 'File')
    for r in raw:
        if r['metric'] == metric and r['channel'] == channel and r['scope'] == 'channel':
            return r['average'], r.get('topQuartile'), r.get('source', 'File')
    for r in raw:
        if r['metric'] == metric and r['scope'] == 'global':
            return r['average'], r.get('topQuartile'), r.get('source', 'File')
    d = defaults.get(metric, {})
    val = d.get(channel, d.get('_default', 0))
    return val, None, 'System Default'


def load_tech_investment():
    path = _resolve_path('config/technology_investment.xlsx')
    result = {'costs': {}, 'cost_defaults': {}, 'tech_stack': [], 'maturity_overrides': {}}
    if not os.path.exists(path):
        return result
    rows = read_xlsx_sheet(path, 'Initiative Costs')
    for r in rows:
        iid = str(r.get('Initiative ID', '')).strip()
        if iid:
            tech_cost = r.get('Technology Cost ($)') or 0
            annual = r.get('Annual License/Maintenance ($)') or 0
            impl = r.get('Implementation Services ($)') or 0
            result['costs'][iid] = {
                'techCost': float(tech_cost), 'annualCost': float(annual),
                'implCost': float(impl),
                'totalOneTime': float(tech_cost) + float(impl),
                'timeline': r.get('Timeline (months)') or 6,
            }
    try:
        rows = read_xlsx_sheet(path, 'Cost Defaults')
        for r in rows:
            level = str(r.get('Effort Level', '')).strip().lower()
            if level:
                result['cost_defaults'][level] = {
                    'techCost': float(r.get('Default Tech Cost ($)') or 0),
                    'annualCost': float(r.get('Default Annual Maintenance ($)') or 0),
                    'implCost': float(r.get('Default Implementation ($)') or 0),
                }
    except Exception:
        pass
    try:
        rows = read_xlsx_sheet(path, 'Current Tech Stack')
        for r in rows:
            result['tech_stack'].append({
                'category': r.get('Category'), 'platform': r.get('Technology/Platform'),
                'status': r.get('Status'), 'coverage': r.get('Coverage %'),
                'maturityImpact': r.get('Maturity Impact'),
            })
    except Exception:
        pass
    try:
        rows = read_xlsx_sheet(path, 'Maturity Overrides')
        for r in rows:
            dim = str(r.get('Dimension', '')).strip()
            override = r.get('Manual Override')
            if dim and override is not None and override != '':
                result['maturity_overrides'][dim] = {
                    'score': float(override),
                    'justification': r.get('Override Justification', ''),
                }
    except Exception:
        pass
    return result


def load_location_costs():
    """Load location × sourcing cost matrix from parameters.xlsx 'Location Cost Matrix' sheet.
    Returns dict: cost_matrix[location][sourcing] = {costPerFTE, hiringCost, attritionRate, shrinkageRate}
    Falls back to flat salary params if sheet is missing.
    """
    path = _resolve_path('config/parameters.xlsx')
    matrix = {}
    if os.path.exists(path):
        try:
            rows = read_xlsx_sheet(path, 'Location Cost Matrix')
            for r in rows:
                loc = str(r.get('Location', '')).strip()
                src = str(r.get('Sourcing', '')).strip()
                if loc and src:
                    if loc not in matrix:
                        matrix[loc] = {}
                    matrix[loc][src] = {
                        'costPerFTE': float(r.get('Cost Per FTE', 55000) or 55000),
                        'hiringCost': float(r.get('Hiring Cost', 5000) or 5000),
                        'attritionRate': float(r.get('Monthly Attrition Rate', 0.03) or 0.03),
                        'shrinkageRate': float(r.get('Shrinkage Rate', 0.30) or 0.30),
                    }
        except Exception:
            pass
    return matrix if matrix else None


def load_queue_dimension_map():
    """Load queue → (BU, Intent, Channel, Location, Sourcing) overrides from parameters.xlsx.
    Returns dict: queue_name → (bu, intent, channel, location, sourcing) or None if sheet missing.
    """
    path = _resolve_path('config/parameters.xlsx')
    dim_map = {}
    if os.path.exists(path):
        try:
            rows = read_xlsx_sheet(path, 'Queue Dimension Map')
            for r in rows:
                qname = str(r.get('Queue Name', '')).strip()
                if qname:
                    dim_map[qname] = {
                        'bu': str(r.get('BU', '')).strip(),
                        'intent': str(r.get('Intent', '')).strip(),
                        'channel': str(r.get('Channel', '')).strip(),
                        'location': str(r.get('Location', 'Onshore')).strip() or 'Onshore',
                        'sourcing': str(r.get('Sourcing', 'In-house')).strip() or 'In-house',
                        'team': str(r.get('Team', '')).strip(),
                    }
        except Exception:
            pass
    return dim_map if dim_map else None


def _build_cost_matrix_from_params(params):
    """Fallback: build a simple cost matrix from flat salary params when Location Cost Matrix sheet is missing."""
    return {
        'Onshore': {
            'In-house': {'costPerFTE': params.get('salaryOnshore', 60000), 'hiringCost': params.get('hiringCost', 5000),
                         'attritionRate': params.get('attritionMonthly', 0.03), 'shrinkageRate': params.get('shrinkage', 0.30)},
            'Outsourced': {'costPerFTE': params.get('salary3rdParty', 45000), 'hiringCost': 2000,
                           'attritionRate': 0.045, 'shrinkageRate': 0.32},
        },
        'Nearshore': {
            'In-house': {'costPerFTE': params.get('salaryNearshore', 30000), 'hiringCost': 3000,
                         'attritionRate': 0.035, 'shrinkageRate': 0.28},
            'Outsourced': {'costPerFTE': params.get('salaryNearshore', 30000) * 0.73, 'hiringCost': 1500,
                           'attritionRate': 0.055, 'shrinkageRate': 0.30},
        },
        'Offshore': {
            'In-house': {'costPerFTE': params.get('salaryOffshore', 18000), 'hiringCost': 2000,
                         'attritionRate': 0.050, 'shrinkageRate': 0.25},
            'Outsourced': {'costPerFTE': params.get('salaryOffshore', 18000) * 0.78, 'hiringCost': 1000,
                           'attritionRate': 0.065, 'shrinkageRate': 0.28},
        },
    }


def _resolve_queue_location(queue_name, bu, intent, channel, queue_dim_map):
    """Resolve location, sourcing, and team for a queue using the dimension map.
    Returns (location, sourcing, team). Defaults to Onshore/In-house/'' if unmapped.
    """
    if queue_dim_map:
        if queue_name in queue_dim_map:
            dm = queue_dim_map[queue_name]
            return dm.get('location', 'Onshore'), dm.get('sourcing', 'In-house'), dm.get('team', '')
        # Try matching by BU|Intent|Channel composite
        composite = f"{bu}|{intent}|{channel}"
        for qn, dm in queue_dim_map.items():
            if f"{dm['bu']}|{dm['intent']}|{dm['channel']}" == composite:
                return dm.get('location', 'Onshore'), dm.get('sourcing', 'In-house'), dm.get('team', '')
    return 'Onshore', 'In-house', ''


def run_etl():
    """Main ETL pipeline: reads raw files -> produces queues, roles, params, benchmarks."""
    params = load_parameters()
    benchmarks = load_benchmarks()
    tech_investment = load_tech_investment()

    # ── P2-1: Load dimensional data ──
    location_cost_matrix = load_location_costs()
    queue_dim_map = load_queue_dimension_map()
    if location_cost_matrix is None:
        location_cost_matrix = _build_cost_matrix_from_params(params)
        logging.info("[P2-1] No Location Cost Matrix sheet — using fallback from salary params")

    queues, bus, intents_set, channels_set = _etl_ccaas(queue_dim_map=queue_dim_map)

    # Fallback: generate demo queues if no raw data available
    if not queues:
        queues, bus, intents_set, channels_set = _generate_demo_queues()
    _etl_surveys(queues)
    # CR-FIX-TAG: Build metric provenance dict early so CRM ETL can update it
    _metric_sources = {
        'volume':     {'source': 'ccaas', 'confidence': 'actual', 'note': 'From CCaaS interaction records'},
        'aht':        {'source': 'ccaas', 'confidence': 'actual', 'note': 'From Total_Handle_Time_Sec in CCaaS records'},
        'escalation': {'source': 'ccaas', 'confidence': 'actual', 'note': 'From Escalated_Flag in CCaaS records'},
        'transfer':   {'source': 'ccaas', 'confidence': 'actual', 'note': 'From Transfer_Flag in CCaaS records'},
        'abandon':    {'source': 'ccaas', 'confidence': 'actual', 'note': 'From Abandoned flag in CCaaS records'},
        'repeat':     {'source': 'ccaas', 'confidence': 'actual', 'note': 'From customer contact frequency analysis'},
        'cpc':        {'source': 'derived', 'confidence': 'derived', 'note': 'Derived from channel cost tier × complexity. Not from source data.'},
        'fcr':        {'source': 'derived', 'confidence': 'derived', 'note': 'Derived from channel capability × complexity. Not from source data.'},
        'csat':       {'source': 'survey', 'confidence': 'survey_backed', 'note': 'Initial: derived. Overwritten by survey at channel level.'},
        'ces':        {'source': 'survey', 'confidence': 'survey_backed', 'note': 'Initial: derived. Overwritten by survey at channel level.'},
    }
    _etl_crm(queues, metric_sources=_metric_sources)  # CR-FIX-CRM: overlay real FCR/escalation/repeat from CRM
    roles = _etl_workforce(params)
    _etl_wfm(params)  # CR-FIX-WFM: override shrinkage/occupancy with WFM actuals

    # ── CR-FIX-CPC: Compute CPC from role cost × AHT, not heuristic ──
    # CPC = cost_per_productive_minute × AHT_minutes
    total_role_cost = sum(r['headcount'] * r['costPerFTE'] for r in roles)
    total_role_fte = sum(r['headcount'] for r in roles) or 1
    avg_cost_per_fte = total_role_cost / total_role_fte
    gross_hrs = params.get('grossHoursPerYear', 2080)
    shrinkage = params.get('shrinkage', 0.30)
    productive_mins_per_year = gross_hrs * (1 - shrinkage) * 60
    cost_per_productive_min = avg_cost_per_fte / max(productive_mins_per_year, 1)
    for q in queues:
        aht_min = q.get('aht', 5.0)  # already in minutes
        q['cpc'] = round(cost_per_productive_min * aht_min, 2)
        q['_cpc_source'] = 'role_cost'
    _metric_sources['cpc'] = {'source': 'role_cost', 'confidence': 'actual',
                               'note': f'CPC = avg cost/FTE (${avg_cost_per_fte:,.0f}) ÷ productive mins ({productive_mins_per_year:,.0f}/yr) × AHT'}

    # ── P2-1: Collect location/sourcing dimensions ──
    locations_set = set()
    sourcing_set = set()
    for q in queues:
        locations_set.add(q.get('location', 'Onshore'))
        sourcing_set.add(q.get('sourcing', 'In-house'))
    for r in roles:
        locations_set.add(r.get('location', 'Onshore'))

    # ── CR-FIX-VALID: Pre-engine queue validation ──
    from engines.constants import validate_queue_metrics
    validation_issues = []
    for q in queues:
        issues = validate_queue_metrics(q)
        if issues:
            validation_issues.extend([{**iss, 'queue': q.get('queue', 'Unknown')} for iss in issues])
    if validation_issues:
        logging.warning(f'[VALIDATION] {len(validation_issues)} metric issues found across queues')
        for iss in validation_issues[:5]:
            logging.warning(f"  {iss['queue']}: {iss['field']}={iss['value']} — {iss['message']}")

    # ── CR-FIX-N: Queue confidence model ──
    # Each metric on each queue gets a confidence score based on source, sample size, mapping quality
    for q in queues:
        q_conf = {}
        vol = q.get('rawVolume', q.get('volume', 0))
        # Sample size factor: 100+ contacts = high, 20-100 = medium, <20 = low
        sample_factor = 1.0 if vol >= 100 else (0.7 if vol >= 20 else 0.3)
        
        for metric in ['aht', 'fcr', 'escalation', 'repeat', 'csat', 'ces', 'cpc']:
            source = q.get(f'_{metric}_source', '')
            if source == 'crm':
                base_conf = 0.95
            elif source == 'survey':
                base_conf = 0.75
            elif source == 'role_cost':
                base_conf = 0.70
            elif metric in ('aht', 'escalation', 'transfer', 'abandon'):
                base_conf = 0.85  # direct from CCaaS
            else:
                base_conf = 0.40  # derived/synthetic
            q_conf[metric] = round(base_conf * sample_factor, 2)
        
        q['_confidence'] = q_conf
        q['_overallConfidence'] = round(sum(q_conf.values()) / max(len(q_conf), 1), 2)

    # ── CR-030v2: Dual-basis volume — NEVER silently inflate base case ──
    # Compute both source and capacity-normalized volumes.
    # Default: source. Normalized available as scenario option.
    total_fte_raw = sum(r['headcount'] for r in roles)
    raw_vol = sum(q['volume'] for q in queues)
    vol_scale = 1.0
    implied_monthly = raw_vol
    if raw_vol > 0 and total_fte_raw > 0:
        shrinkage = params.get('shrinkage', 0.30)
        gross_hrs = params.get('grossHoursPerYear', 2080)
        net_prod_hrs = gross_hrs * (1 - shrinkage)
        avg_aht_hrs = sum(q['aht'] * q['volume'] for q in queues) / raw_vol / 60
        target_occupancy = params.get('targetOccupancy', 0.75)
        implied_monthly = (total_fte_raw * net_prod_hrs * target_occupancy) / (avg_aht_hrs * 12)
        vol_scale = max(1.0, implied_monthly / raw_vol)

    # Store BOTH bases on every queue — volume stays as source (never overwritten)
    for q in queues:
        q['rawVolume'] = q['volume']  # source truth
        q['normalizedVolume'] = round(q['volume'] * vol_scale) if vol_scale > 1.0 else q['volume']
        q['volumeBasis'] = 'source'  # active basis — engines read this

    # Determine active basis from params (consultant can switch)
    active_basis = params.get('volumeBasis', 'source')
    if active_basis == 'capacity_normalized' and vol_scale > 1.5:
        for q in queues:
            q['volume'] = q['normalizedVolume']
            q['volumeBasis'] = 'capacity_normalized'
        logging.info(f"[CR-030v2] Volume basis: CAPACITY-NORMALIZED ({vol_scale:.1f}x: {raw_vol:,} → {sum(q['volume'] for q in queues):,})")
    else:
        active_basis = 'source'
        logging.info(f"[CR-030v2] Volume basis: SOURCE ({raw_vol:,}/mo). "
                     f"Capacity-normalized would be {vol_scale:.1f}x ({round(implied_monthly):,}/mo)")

    intent_list = sorted(intents_set)
    channel_list = sorted(channels_set)
    bu_list = sorted(bus)

    total_volume = sum(q['volume'] for q in queues)
    total_fte = sum(r['headcount'] for r in roles)
    total_monthly_cost = sum(r['headcount'] * r['costPerFTE'] / 12 for r in roles)
    total_annual_cost = sum(r['headcount'] * r['costPerFTE'] for r in roles)
    avg_csat = sum(q['csat'] * q['volume'] for q in queues) / max(total_volume, 1)
    avg_fcr = sum(q['fcr'] * q['volume'] for q in queues) / max(total_volume, 1)
    avg_aht = sum(q['aht'] * q['volume'] for q in queues) / max(total_volume, 1)  # unit: MINUTES

    # CR-021 v5: Compute annualized volume for CPC and financial consistency
    ann_factor = params.get('volumeAnnualizationFactor', 12)
    total_volume_annual = total_volume * ann_factor

    for q in queues:
        q['complexity'] = INTENT_COMPLEXITY.get(q['intent'], 0.40)
        q['capability'] = CHANNEL_CAPABILITY.get(q['channel'], 0.50)
        q['costTier'] = CHANNEL_COST_TIER.get(q['channel'], 0.50)

    return {
        'queues': queues, 'roles': roles, 'params': params,
        'benchmarks': benchmarks, 'techInvestment': tech_investment,
        'intents': intent_list, 'channels': channel_list, 'bus': bu_list,
        'intentComplexity': INTENT_COMPLEXITY,
        'channelCap': CHANNEL_CAPABILITY,
        'channelCost': CHANNEL_COST_TIER,
        'kpiDirection': KPI_DIRECTION,
        'totalFTE': total_fte, 'totalMonthlyCost': total_monthly_cost,
        'totalCost': total_annual_cost,
        'totalVolume': total_volume,
        'totalVolumeAnnual': total_volume_annual,
        'volumeAnnualizationFactor': ann_factor,
        'avgCSAT': avg_csat,
        'avgFCR': avg_fcr,
        'avgAHT': avg_aht,  # UNIT: MINUTES (pools use q['aht']*60 for seconds)
        'avgAHT_unit': 'minutes',
        # ── P2-1: Dimensional fields ──
        'locations': sorted(locations_set),
        'sourcingTypes': sorted(sourcing_set),
        'locationCostMatrix': location_cost_matrix,
        # ── CR-FIX-VOL v2: Dual-basis volume metadata ──
        'volumeScaling': {
            'activeBasis': active_basis,
            'factor': round(vol_scale, 1),
            'rawMonthlyVolume': raw_vol,
            'normalizedMonthlyVolume': round(implied_monthly),
            'scaledMonthlyVolume': total_volume,  # whatever is active
            'applied': active_basis == 'capacity_normalized',
            'basisOptions': {
                'source': {'volume': raw_vol, 'label': 'Source (CCaaS records)'},
                'capacity_normalized': {'volume': round(implied_monthly), 'label': f'Capacity-normalized ({vol_scale:.1f}x)'},
            },
            'explanation': f'Source: {raw_vol:,}/mo from CCaaS. Normalized: {round(implied_monthly):,}/mo '
                          f'({vol_scale:.1f}x to match {total_fte} FTE at '
                          f'{params.get("targetOccupancy", 0.75):.0%} occupancy).',
        },
        # ── Calculation basis object ──
        'calculationBasis': {
            'volume': {'selected': active_basis, 'available': ['source', 'capacity_normalized']},
            'cpc': {'selected': 'role_cost', 'available': ['role_cost', 'observed']},
            'fcr': {'selected': 'crm_actual' if any(q.get('_fcr_source') == 'crm' for q in queues) else 'derived',
                    'available': ['crm_actual', 'derived']},
            'repeat': {'selected': 'crm_actual' if any(q.get('_repeat_source') == 'crm' for q in queues) else 'derived',
                       'available': ['crm_actual', 'fcr_derived', 'ccaas_observed']},
            'occupancy': {'selected': 'wfm_actual' if params.get('_wfmActuals') else 'parameter_default',
                          'available': ['wfm_actual', 'parameter_default']},
            'shrinkage': {'selected': 'wfm_actual' if params.get('_wfmActuals') else 'parameter_default',
                          'available': ['wfm_actual', 'parameter_default']},
        },
        # ── CR-FIX-TAG v2: Metric provenance with honest confidence ──
        'metricSources': dict(_metric_sources, volume={
            'source': 'ccaas',
            'basis': active_basis,
            'confidence': 'actual' if active_basis == 'source' else 'actual_transformed',
            'transformation': 'none' if active_basis == 'source' else 'capacity_implied_scaling',
            'note': f'From CCaaS interaction records ({raw_vol:,}/mo)' + (
                f' — capacity-normalized {vol_scale:.1f}x' if active_basis == 'capacity_normalized' else ''),
        }, cpc={
            'source': 'role_cost', 'confidence': 'derived',
            'basis': 'modeled',
            'note': f'Modeled: cost/FTE (${avg_cost_per_fte:,.0f}) ÷ productive mins ({productive_mins_per_year:,.0f}/yr) × AHT. Not observed transactional CPC.',
        }),
        # ── CR-FIX-VALID: Validation issues ──
        'validationIssues': validation_issues,
    }


def _etl_ccaas(queue_dim_map=None):
    path = _resolve_path('raw/ccaas_interaction_records.xlsx')
    if not os.path.exists(path):
        return [], set(), set(), set()
    rows = read_xlsx_sheet(path, 'Interaction_Detail_Report')
    queue_data = defaultdict(lambda: {
        'count': 0, 'aht_sum': 0, 'acw_sum': 0, 'escalated': 0,
        'transferred': 0, 'abandoned': 0, 'customers': set(),
        'repeat_contacts': 0, 'queue_names': set(),
    })
    interactions_by_customer = defaultdict(list)
    for r in rows:
        queue_name = str(r.get('Queue_Name', '')).strip()
        if queue_name not in QUEUE_MAP:
            matched = _fuzzy_queue_match(queue_name)
            if not matched:
                continue
            QUEUE_MAP[queue_name] = matched
        bu, intent, channel_base = QUEUE_MAP[queue_name]
        media = str(r.get('Media_Type', '')).strip().upper()
        channel = normalize_channel(media) or channel_base
        queue_key = f"{bu}|{intent}|{channel}"
        aht_sec = r.get('Total_Handle_Time_Sec') or 0
        acw_sec = r.get('ACW_Duration_Sec') or 0
        aht_sec = clamp(float(aht_sec), 0, 7200)
        acw_sec = clamp(float(acw_sec), 0, 3600)
        aht_min = aht_sec / 60
        qd = queue_data[queue_key]
        qd['count'] += 1
        qd['queue_names'].add(queue_name)  # CR-FIX-QDIM: track raw queue names for dimension mapping
        qd['aht_sum'] += aht_min
        qd['acw_sum'] += acw_sec / 60
        qd['escalated'] += 1 if r.get('Escalated_Flag') else 0
        qd['transferred'] += 1 if r.get('Transfer_Flag') else 0
        qd['abandoned'] += 1 if r.get('Abandoned') else 0
        cust_id = r.get('Customer_ID')
        if cust_id:
            qd['customers'].add(str(cust_id))
            ts = r.get('Timestamp') or r.get('Date')
            interactions_by_customer[str(cust_id)].append({
                'queue_key': queue_key, 'ts': str(ts),
            })
    for cid, ints in interactions_by_customer.items():
        if len(ints) < 2:
            continue
        sorted_ints = sorted(ints, key=lambda x: x['ts'])
        for i in range(1, len(sorted_ints)):
            queue_data[sorted_ints[i]['queue_key']]['repeat_contacts'] += 1

    queues = []
    bus = set()
    intents_set = set()
    channels_set = set()
    for qk, qd in queue_data.items():
        if qd['count'] < 5:
            continue
        bu, intent, channel = qk.split('|')
        count = qd['count']
        avg_aht = qd['aht_sum'] / count
        avg_acw = qd['acw_sum'] / count
        esc_rate = qd['escalated'] / count
        transfer_rate = qd['transferred'] / count
        abandon_rate = qd['abandoned'] / count
        repeat_rate = clamp(qd['repeat_contacts'] / max(count, 1), 0, 0.5)
        base_cost = 8.50 * CHANNEL_COST_TIER.get(channel, 0.50)
        complexity = INTENT_COMPLEXITY.get(intent, 0.40)
        cpc = clamp(base_cost * (1 + complexity * 0.5), 0.30, 25.0)
        match_score = max(0, CHANNEL_CAPABILITY.get(channel, 0.5) - complexity * 0.5)
        csat = clamp(3.0 + match_score * 2.0, 2.0, 5.0)
        fcr = clamp(0.55 + match_score * 0.35, 0.30, 0.95)
        ces = clamp(2.0 + (1 - match_score) * 2.5, 1.0, 5.0)
        # P2-1: Resolve location and sourcing from dimension map
        # CR-FIX-QDIM: Pass first raw queue name for exact matching
        primary_queue_name = next(iter(qd['queue_names']), '')
        location, sourcing, team = _resolve_queue_location(
            primary_queue_name, bu, intent, channel, queue_dim_map)
        queues.append({
            'bu': bu, 'intent': intent, 'channel': channel,
            'location': location, 'sourcing': sourcing, 'team': team,
            'volume': count, 'csat': round(csat, 2), 'fcr': round(fcr, 3),
            'aht': round(avg_aht, 1), 'acw': round(avg_acw, 1),
            'cpc': round(cpc, 2), 'repeat': round(repeat_rate, 3),
            'escalation': round(esc_rate, 3), 'ces': round(ces, 2),
            'transfer': round(transfer_rate, 3), 'abandon': round(abandon_rate, 3),
            'queueId': f"{bu}_{intent}_{channel}".replace(' ', '_').lower(),
            'queue': f"{bu} — {intent} — {channel}",
        })
        bus.add(bu)
        intents_set.add(intent)
        channels_set.add(channel)
    return queues, bus, intents_set, channels_set


def _etl_surveys(queues):
    path = _resolve_path('raw/survey_responses.xlsx')
    if not os.path.exists(path):
        return
    try:
        rows = read_xlsx_sheet(path)
    except Exception:
        return
    channel_csat = defaultdict(list)
    channel_ces = defaultdict(list)
    for r in rows:
        status = str(r.get('Response_Status', '')).strip()
        if status not in ('Complete', 'Partial'):
            continue
        ch = normalize_channel(r.get('Channel'))
        csat_val = r.get('Q1_CSAT_Overall')
        ces_val = r.get('Q3_CES')
        if ch and csat_val is not None:
            try:
                v = float(csat_val)
                if 1 <= v <= 5:
                    channel_csat[ch].append(v)
            except (ValueError, TypeError):
                pass
        if ch and ces_val is not None:
            try:
                v = float(ces_val)
                if 1 <= v <= 5:
                    channel_ces[ch].append(v)
            except (ValueError, TypeError):
                pass
    for q in queues:
        ch = q['channel']
        if ch in channel_csat and channel_csat[ch]:
            q['csat'] = round(sum(channel_csat[ch]) / len(channel_csat[ch]), 2)
            q['_csat_source'] = 'survey'
        if ch in channel_ces and channel_ces[ch]:
            q['ces'] = round(sum(channel_ces[ch]) / len(channel_ces[ch]), 2)
            q['_ces_source'] = 'survey'


# ═══════════════════════════════════════════════════════════════
# CR-FIX-CRM: CRM Case Data Integration
# Overlays real FCR, escalation, repeat rates from CRM case records
# onto queue metrics, replacing synthetic/derived values.
# ═══════════════════════════════════════════════════════════════
_CRM_REASON_TO_INTENT = {
    'Billing': 'Billing & Payments', 'Technical': 'Technical Support',
    'Network': 'Network Outage', 'Device': 'Device Troubleshooting',
    'General': 'General Enquiry', 'Complaint': 'Complaints',
    'Account Changes': 'Plan Change', 'Retention': 'Service Cancellation',
    'Sales': 'New Connection',
}

def _etl_crm(queues, metric_sources=None):
    """Overlay real CRM metrics onto queue data where available."""
    path = _resolve_path('raw/crm_case_export.xlsx')
    if not os.path.exists(path):
        return
    try:
        rows = read_xlsx_sheet(path, 'Case_Export')
    except Exception:
        logging.warning('[CRM] Failed to read crm_case_export.xlsx')
        return

    # Aggregate CRM metrics by BU + intent + channel
    crm_agg = defaultdict(lambda: {
        'total': 0, 'fcr_yes': 0, 'escalated': 0, 'reopened': 0,
        'resolution_hours': [], 'sla_met': 0, 'sla_total': 0,
        'csat_scores': [], 'ces_scores': [], 'nps_scores': [],
    })

    for r in rows:
        reason = str(r.get('Contact_Reason__c', '')).strip()
        intent = _CRM_REASON_TO_INTENT.get(reason)
        if not intent:
            continue
        bu = str(r.get('Business_Unit__c', '')).strip()
        channel = normalize_channel(r.get('Channel__c') or r.get('Origin'))
        if not channel:
            continue
        key = f"{bu}|{intent}|{channel}"
        agg = crm_agg[key]
        agg['total'] += 1

        # FCR
        fcr_val = str(r.get('First_Contact_Resolution__c', '')).strip()
        if fcr_val == 'Yes':
            agg['fcr_yes'] += 1

        # Escalation
        esc_val = str(r.get('Escalated__c', '')).strip()
        if esc_val == 'Yes':
            agg['escalated'] += 1

        # Repeat (reopen)
        reopen_val = str(r.get('Reopened__c', '')).strip()
        if reopen_val == 'Yes':
            agg['reopened'] += 1

        # Resolution hours
        res_hrs = r.get('Resolution_Time_Hours__c')
        if res_hrs is not None:
            try:
                agg['resolution_hours'].append(float(res_hrs))
            except (ValueError, TypeError):
                pass

        # SLA
        sla_val = str(r.get('SLA_Met__c', '')).strip()
        if sla_val in ('Yes', 'No'):
            agg['sla_total'] += 1
            if sla_val == 'Yes':
                agg['sla_met'] += 1

        # CSAT/CES/NPS from CRM (case-level, more granular than survey)
        for field, dest in [('CSAT_Score__c', 'csat_scores'), ('CES_Score__c', 'ces_scores'), ('NPS_Score__c', 'nps_scores')]:
            v = r.get(field)
            if v is not None:
                try:
                    agg[dest].append(float(v))
                except (ValueError, TypeError):
                    pass

    # Overlay onto queues
    overlaid = 0
    for q in queues:
        key = f"{q['bu']}|{q['intent']}|{q['channel']}"
        agg = crm_agg.get(key)
        if not agg or agg['total'] < 10:  # minimum sample size
            continue

        n = agg['total']
        # FCR: real rate from CRM
        q['fcr'] = round(agg['fcr_yes'] / n, 3)
        q['_fcr_source'] = 'crm'

        # Escalation: real rate from CRM (if enough cases have the flag)
        q['escalation'] = round(agg['escalated'] / n, 3)
        q['_escalation_source'] = 'crm'

        # Repeat: real rate from reopened cases
        q['repeat'] = round(agg['reopened'] / n, 3)
        q['_repeat_source'] = 'crm'

        # Resolution hours (store as supplementary metric)
        if agg['resolution_hours']:
            q['resolution_hours'] = round(sum(agg['resolution_hours']) / len(agg['resolution_hours']), 1)

        # SLA compliance
        if agg['sla_total'] > 0:
            q['sla_met'] = round(agg['sla_met'] / agg['sla_total'], 3)

        # Case-level CSAT (more granular than channel-level survey)
        if len(agg['csat_scores']) >= 5:
            q['csat'] = round(sum(agg['csat_scores']) / len(agg['csat_scores']), 2)
            q['_csat_source'] = 'crm'

        if len(agg['ces_scores']) >= 5:
            q['ces'] = round(sum(agg['ces_scores']) / len(agg['ces_scores']), 2)
            q['_ces_source'] = 'crm'

        # NPS from CRM
        if len(agg['nps_scores']) >= 5:
            q['nps'] = round(sum(agg['nps_scores']) / len(agg['nps_scores']), 1)
            # Also compute NPS category breakdown
            promoters = sum(1 for s in agg['nps_scores'] if s >= 9) / len(agg['nps_scores'])
            detractors = sum(1 for s in agg['nps_scores'] if s <= 6) / len(agg['nps_scores'])
            q['nps_score'] = round((promoters - detractors) * 100, 1)
            q['_nps_source'] = 'crm'

        overlaid += 1

    logging.info(f'[CRM] Overlaid real metrics onto {overlaid}/{len(queues)} queues '
                 f'from {sum(a["total"] for a in crm_agg.values()):,} CRM cases')

    # Update metric sources if provided
    if metric_sources:
        if overlaid > 0:
            metric_sources['fcr'] = {'source': 'crm', 'confidence': 'actual', 'note': f'From CRM First_Contact_Resolution__c ({overlaid} queues matched)'}
            metric_sources['escalation'] = {'source': 'crm', 'confidence': 'actual', 'note': f'From CRM Escalated__c ({overlaid} queues matched)'}
            metric_sources['repeat'] = {'source': 'crm', 'confidence': 'actual', 'note': f'From CRM Reopened__c ({overlaid} queues matched)'}
            metric_sources['nps'] = {'source': 'crm', 'confidence': 'actual', 'note': f'From CRM NPS_Score__c ({overlaid} queues matched)'}


# ═══════════════════════════════════════════════════════════════
# CR-FIX-WFM: WFM Monthly Summary Integration
# Replaces parameter assumptions with actual WFM-measured values
# for shrinkage, occupancy, utilization, adherence.
# ═══════════════════════════════════════════════════════════════
def _etl_wfm(params):
    """Read WFM monthly summary and override heuristic params with actuals."""
    path = _resolve_path('raw/hr_workforce_data.xlsx')
    if not os.path.exists(path):
        return
    try:
        rows = read_xlsx_sheet(path, 'WFM_Monthly_Summary')
    except Exception:
        logging.warning('[WFM] Failed to read WFM_Monthly_Summary sheet')
        return

    if not rows:
        return

    # Aggregate across all agents and months
    total_scheduled = 0
    total_actual = 0
    total_productive = 0
    total_training = 0
    total_meeting = 0
    total_break = 0
    total_absent = 0
    adherence_vals = []
    occupancy_vals = []
    utilization_vals = []
    schedule_eff_vals = []
    n = 0

    for r in rows:
        scheduled = float(r.get('Scheduled_Hours', 0) or 0)
        actual = float(r.get('Actual_Hours', 0) or 0)
        productive = float(r.get('Productive_Hours', 0) or 0)
        training = float(r.get('Training_Hours', 0) or 0)
        meeting = float(r.get('Meeting_Hours', 0) or 0)
        brk = float(r.get('Break_Hours', 0) or 0)
        absent = float(r.get('Absent_Hours', 0) or 0)

        total_scheduled += scheduled
        total_actual += actual
        total_productive += productive
        total_training += training
        total_meeting += meeting
        total_break += brk
        total_absent += absent

        adh = r.get('Adherence_%')
        occ = r.get('Occupancy_%')
        util = r.get('Utilization_%')
        sch_eff = r.get('Schedule_Efficiency_%')

        if adh is not None:
            adherence_vals.append(float(adh))
        if occ is not None:
            occupancy_vals.append(float(occ))
        if util is not None:
            utilization_vals.append(float(util))
        if sch_eff is not None:
            schedule_eff_vals.append(float(sch_eff))
        n += 1

    if n == 0:
        return

    # Compute actual shrinkage: (scheduled - productive) / scheduled
    actual_shrinkage = (total_scheduled - total_productive) / max(total_scheduled, 1)
    actual_occupancy = sum(occupancy_vals) / len(occupancy_vals) if occupancy_vals else 0
    actual_utilization = sum(utilization_vals) / len(utilization_vals) if utilization_vals else 0
    actual_adherence = sum(adherence_vals) / len(adherence_vals) if adherence_vals else 0
    actual_schedule_eff = sum(schedule_eff_vals) / len(schedule_eff_vals) if schedule_eff_vals else 0

    # Shrinkage decomposition
    shrinkage_decomp = {
        'training': round(total_training / max(total_scheduled, 1), 3),
        'meetings': round(total_meeting / max(total_scheduled, 1), 3),
        'breaks': round(total_break / max(total_scheduled, 1), 3),
        'absence': round(total_absent / max(total_scheduled, 1), 3),
        'other': round(max(0, actual_shrinkage - (total_training + total_meeting + total_break + total_absent) / max(total_scheduled, 1)), 3),
    }

    # Override params with actuals
    old_shrinkage = params.get('shrinkage', 0.30)
    old_occupancy = params.get('targetOccupancy', 0.75)

    params['shrinkage'] = round(actual_shrinkage, 3)
    params['targetOccupancy'] = round(actual_occupancy, 3)
    params['_wfmActuals'] = {
        'shrinkage': round(actual_shrinkage, 3),
        'occupancy': round(actual_occupancy, 3),
        'utilization': round(actual_utilization, 3),
        'adherence': round(actual_adherence, 3),
        'scheduleEfficiency': round(actual_schedule_eff, 3),
        'shrinkageDecomp': shrinkage_decomp,
        'sampleSize': n,
        'source': 'WFM_Monthly_Summary',
        'overrides': {
            'shrinkage': {'was': old_shrinkage, 'now': round(actual_shrinkage, 3)},
            'occupancy': {'was': old_occupancy, 'now': round(actual_occupancy, 3)},
        },
    }

    logging.info(f'[WFM] Actuals from {n} agent-months: shrinkage={actual_shrinkage:.1%} '
                 f'(was {old_shrinkage:.1%}), occupancy={actual_occupancy:.1%} '
                 f'(was {old_occupancy:.1%}), utilization={actual_utilization:.1%}, '
                 f'adherence={actual_adherence:.1%}')


def _etl_workforce(params):
    """Read HR workforce data and build role aggregation."""
    path = _resolve_path('raw/hr_workforce_data.xlsx')
    if not os.path.exists(path):
        return _default_roles()
    try:
        emp_rows = read_xlsx_sheet(path, 'Employee_Roster')
    except Exception:
        return _default_roles()
    role_agg = defaultdict(lambda: {'count': 0, 'cost_sum': 0, 'locations': defaultdict(int), 'skills': set(), 'sourcing': defaultdict(int)})
    for r in emp_rows:
        status = str(r.get('Employment_Status', '')).strip()
        if status not in ('Active', 'active', ''):
            continue
        title = r.get('Job_Title', '')
        role = fuzzy_match_role(title)
        location = str(r.get('Work_Location', 'Onshore')).strip()
        cost = r.get('Annual_Fully_Loaded_Cost')
        emp_type = str(r.get('Employment_Type', 'Full-Time')).strip()
        fte_mult = 0.5 if 'Part' in emp_type else 1.0
        # P2-1: Derive sourcing type from employment type
        sourcing = 'In-house'
        emp_lower = emp_type.lower()
        if any(kw in emp_lower for kw in ['contract', 'temp', 'contractor', 'outsource', 'bpo', 'vendor']):
            sourcing = 'Outsourced'
        elif any(kw in emp_lower for kw in ['managed', 'service']):
            sourcing = 'Managed Service'
        key = role
        ra = role_agg[key]
        ra['count'] += fte_mult
        if cost:
            try:
                ra['cost_sum'] += float(cost) * fte_mult
            except (ValueError, TypeError):
                ra['cost_sum'] += params.get('salaryOnshore', 60000) * fte_mult
        else:
            ra['cost_sum'] += params.get('salaryOnshore', 60000) * fte_mult
        ra['locations'][location] += 1
        ra['sourcing'][sourcing] += 1
        skills = r.get('Skills_Certified', '')
        if skills:
            for s in str(skills).split(','):
                ra['skills'].add(s.strip())
    roles = []
    for role_name, ra in role_agg.items():
        hc = max(1, round(ra['count']))
        cost_per = ra['cost_sum'] / max(ra['count'], 1)
        loc = max(ra['locations'], key=ra['locations'].get) if ra['locations'] else 'Onshore'
        # CR-07: Validate location — channel names (IVR, Chat, App/Self-Service) sometimes leak into location field
        _VALID_LOCATIONS = {'Onshore', 'Nearshore', 'Offshore', 'WFH', 'Hybrid'}
        if loc not in _VALID_LOCATIONS:
            loc = 'Onshore'
        src = max(ra['sourcing'], key=ra['sourcing'].get) if ra['sourcing'] else 'In-house'
        shared = len(ra['skills']) > 2
        roles.append({
            'role': role_name, 'headcount': hc, 'costPerFTE': round(cost_per, 0),
            'location': loc, 'sourcing': src, 'shared': shared,
            'channelSkills': list(ra['skills'])[:5],
        })
    roles.sort(key=lambda x: x['headcount'], reverse=True)
    return roles if roles else _default_roles()


def _default_roles():
    return [
        {'role': 'Agent L1', 'headcount': 450, 'costPerFTE': 55000, 'location': 'Onshore', 'sourcing': 'In-house', 'shared': False},
        {'role': 'Agent L2 / Specialist', 'headcount': 120, 'costPerFTE': 68000, 'location': 'Onshore', 'sourcing': 'In-house', 'shared': True},
        {'role': 'Agent L3 / Expert', 'headcount': 30, 'costPerFTE': 82000, 'location': 'Onshore', 'sourcing': 'In-house', 'shared': True},
        {'role': 'Supervisor / Team Lead', 'headcount': 50, 'costPerFTE': 75000, 'location': 'Onshore', 'sourcing': 'In-house', 'shared': False},
        {'role': 'QA Analyst', 'headcount': 20, 'costPerFTE': 62000, 'location': 'Onshore', 'sourcing': 'In-house', 'shared': False},
        {'role': 'WFM Analyst', 'headcount': 15, 'costPerFTE': 65000, 'location': 'Onshore', 'sourcing': 'In-house', 'shared': False},
        {'role': 'Back-Office / Processing', 'headcount': 80, 'costPerFTE': 48000, 'location': 'Nearshore', 'sourcing': 'Outsourced', 'shared': False},
        {'role': 'Trainer', 'headcount': 12, 'costPerFTE': 60000, 'location': 'Onshore', 'sourcing': 'In-house', 'shared': False},
        {'role': 'Knowledge Manager', 'headcount': 5, 'costPerFTE': 70000, 'location': 'Onshore', 'sourcing': 'In-house', 'shared': False},
        {'role': 'Reporting / Analytics', 'headcount': 10, 'costPerFTE': 72000, 'location': 'Onshore', 'sourcing': 'In-house', 'shared': False},
    ]


def _fuzzy_queue_match(queue_name):
    q = queue_name.upper().replace('-', '_').replace(' ', '_')
    for known, mapping in QUEUE_MAP.items():
        if q.startswith(known.split('_')[0]) and len(q) > 3:
            return mapping
    return None


def _generate_demo_queues():
    """Generate demo queue data when no raw files are available."""
    import random
    random.seed(42)

    intents = [
        'Billing & Payments', 'Technical Support', 'Network Outage', 'Plan Change',
        'Device Troubleshooting', 'Service Cancellation', 'Roaming', 'Data Usage',
        'Contract Renewal', 'Complaints', 'New Connection', 'SIM Replacement',
        'Number Portability', 'Value-Added Services', 'General Enquiry',
    ]
    channels = ['Voice', 'IVR', 'Chat', 'Email', 'App/Self-Service', 'Social Media', 'SMS/WhatsApp', 'Retail/Walk-in']
    bus_list = ['Consumer', 'Business', 'Enterprise']
    bu_share = {'Consumer': 0.6, 'Business': 0.3, 'Enterprise': 0.1}

    queues = []
    bus_set = set()
    intents_set = set()
    channels_set = set()
    base_cost = 8.50
    qid = 0

    # P2-1: Location/sourcing distribution per channel
    # Voice: mostly onshore in-house; Chat: mix; Email/Back-office: nearshore/offshore outsourced
    CHANNEL_LOCATION_MIX = {
        'Voice':           [('Onshore', 'In-house', 0.60), ('Nearshore', 'Outsourced', 0.25), ('Offshore', 'Outsourced', 0.15)],
        'IVR':             [('Onshore', 'In-house', 1.0)],
        'Chat':            [('Onshore', 'In-house', 0.40), ('Nearshore', 'Outsourced', 0.35), ('Offshore', 'Outsourced', 0.25)],
        'Email':           [('Onshore', 'In-house', 0.30), ('Nearshore', 'Outsourced', 0.30), ('Offshore', 'Outsourced', 0.40)],
        'App/Self-Service': [('Onshore', 'In-house', 1.0)],
        'Social Media':    [('Onshore', 'In-house', 0.50), ('Nearshore', 'Outsourced', 0.50)],
        'SMS/WhatsApp':    [('Onshore', 'In-house', 0.40), ('Offshore', 'Outsourced', 0.60)],
        'Retail/Walk-in':  [('Onshore', 'In-house', 1.0)],
    }

    for bu in bus_list:
        for intent in intents:
            cmplx = INTENT_COMPLEXITY.get(intent, 0.40)
            total_vol = random.randint(8000, 80000) * bu_share[bu]

            for ch in channels:
                cap = CHANNEL_CAPABILITY.get(ch, 0.50)
                cost_tier = CHANNEL_COST_TIER.get(ch, 0.50)

                # Skip unlikely combinations
                if cmplx > 0.6 and cap < 0.30 and random.random() < 0.6:
                    continue
                if random.random() < 0.15:
                    continue

                vol = max(100, round(total_vol * (random.random() * 0.3 + 0.05)))
                match = max(0, cap - cmplx * 0.5)
                csat = clamp(3.0 + match * 2 + (random.random() - 0.5) * 0.6, 1.5, 5.0)
                fcr = clamp(0.55 + match * 0.35 + (random.random() - 0.5) * 0.12, 0.25, 0.95)
                aht = clamp(3 + cmplx * 8 * (1.5 if ch == 'Email' else 1.2 if ch == 'Chat' else 1.0) + (random.random() - 0.5) * 2, 1, 25)
                cpc = clamp(base_cost * cost_tier * (1 + cmplx * 0.5) + (random.random() - 0.5) * 0.8, 0.3, 25)
                repeat = clamp((1 - fcr) * 0.5 + cmplx * 0.08 + (random.random() - 0.5) * 0.04, 0.03, 0.40)
                escalation = clamp(cmplx * 0.15 * (1 - cap * 0.3) + (random.random() - 0.5) * 0.03, 0.01, 0.30)
                ces = clamp(2 + (1 - match) * 2.5 + (random.random() - 0.5) * 0.4, 1, 5)
                transfer = clamp(escalation * 0.5 + (random.random() - 0.5) * 0.02, 0.01, 0.20)

                qid += 1
                loc_mix = CHANNEL_LOCATION_MIX.get(ch, [('Onshore', 'In-house', 1.0)])
                for loc, src, share in loc_mix:
                    loc_vol = max(50, round(vol * share))
                    if loc_vol < 50:
                        continue
                    queues.append({
                        'bu': bu, 'intent': intent, 'channel': ch,
                        'location': loc, 'sourcing': src,
                        'volume': loc_vol,
                        'csat': round(csat, 2), 'fcr': round(fcr, 3),
                        'aht': round(aht, 1), 'cpc': round(cpc, 2),
                        'repeat': round(repeat, 3), 'escalation': round(escalation, 3),
                        'ces': round(ces, 2), 'transfer': round(transfer, 3),
                        'abandon': round(random.random() * 0.08, 3),
                        'complexity': cmplx, 'capability': cap, 'costTier': cost_tier,
                        'queueId': f"{bu}_{intent}_{ch}_{loc}_{src}".replace(' ', '_').replace('/', '_').lower(),
                        'queue': f"{bu} — {intent} — {ch} — {loc}",
                    })
                bus_set.add(bu)
                intents_set.add(intent)
                channels_set.add(ch)

    return queues, bus_set, intents_set, channels_set
