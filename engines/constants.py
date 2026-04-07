"""
ContactIQ — Constants & Unit Registry
Central source of truth for all model constants, unit definitions,
conversion helpers, and validation assertions.
Prevents drift across modules.
"""

# ═══════════════════════════════════════════════════════════════
# TIME CONSTANTS (all in seconds unless noted)
# ═══════════════════════════════════════════════════════════════
EXTRA_SEC_PER_ESCALATION = 900    # 15 min — full L2/L3 avoidance
EXTRA_SEC_PER_TRANSFER = 180      # 3 min — transfer overhead
AHT_SANITY_CEILING_MIN = 120      # max plausible AHT in minutes

# ═══════════════════════════════════════════════════════════════
# POOL / CAP CONSTANTS
# ═══════════════════════════════════════════════════════════════
ABSOLUTE_SINGLE_INIT_CAP = 0.25
PER_ROLE_MAX_REDUCTION = 0.35
SECONDARY_WEIGHT = 0.50
REPEAT_FLOOR = 0.02

# ═══════════════════════════════════════════════════════════════
# METRIC UNIT REGISTRY
# ═══════════════════════════════════════════════════════════════
METRIC_UNITS = {
    'aht': 'minutes',         # stored in minutes, convert to seconds at boundary
    'acw': 'minutes',
    'fcr': 'ratio_0_1',       # 0.0 to 1.0
    'csat': 'score_1_5',      # 1.0 to 5.0
    'ces': 'score_1_5',
    'nps': 'score_neg100_100',
    'cpc': 'currency',
    'escalation': 'ratio_0_1',
    'repeat': 'ratio_0_1',
    'transfer': 'ratio_0_1',
    'abandon': 'ratio_0_1',
    'volume': 'count',
    'shrinkage': 'ratio_0_1',
    'occupancy': 'ratio_0_1',
    'utilization': 'ratio_0_1',
    'adherence': 'ratio_0_1',
}

# ═══════════════════════════════════════════════════════════════
# CONFIDENCE WEIGHTS (for data quality scoring)
# ═══════════════════════════════════════════════════════════════
CONFIDENCE_WEIGHTS = {
    'actual': 1.0,
    'actual_transformed': 0.80,
    'survey_backed': 0.70,
    'derived': 0.60,
    'benchmarked': 0.40,
    'assumed': 0.20,
}

CONFIDENCE_LABELS = {
    'actual': 'Actual',
    'actual_transformed': 'Actual (transformed)',
    'survey_backed': 'Survey-backed',
    'derived': 'Derived',
    'benchmarked': 'Benchmarked',
    'assumed': 'Assumed',
}

# ═══════════════════════════════════════════════════════════════
# LEVER UNCERTAINTY FACTORS (for confidence bands)
# ═══════════════════════════════════════════════════════════════
LEVER_UNCERTAINTY = {
    'deflection': 0.25,
    'aht_reduction': 0.15,
    'escalation_reduction': 0.20,
    'repeat_reduction': 0.25,
    'cost_reduction': 0.20,
    'shrinkage_reduction': 0.15,
}

# ═══════════════════════════════════════════════════════════════
# CONVERSION HELPERS
# ═══════════════════════════════════════════════════════════════
def minutes_to_seconds(v):
    """Convert minutes to seconds."""
    return v * 60

def seconds_to_minutes(v):
    """Convert seconds to minutes."""
    return v / 60

def pct(v):
    """Convert ratio (0-1) to percentage."""
    return v * 100

def ratio(v):
    """Convert percentage to ratio (0-1)."""
    return v / 100

# ═══════════════════════════════════════════════════════════════
# VALIDATION HELPERS
# ═══════════════════════════════════════════════════════════════
def validate_queue_metrics(q, raise_on_fail=False):
    """Validate queue metric ranges. Returns list of issues."""
    issues = []
    checks = [
        ('fcr', 0, 1, 'FCR must be 0-1'),
        ('escalation', 0, 1, 'Escalation rate must be 0-1'),
        ('repeat', 0, 1, 'Repeat rate must be 0-1'),
        ('transfer', 0, 1, 'Transfer rate must be 0-1'),
        ('abandon', 0, 1, 'Abandon rate must be 0-1'),
        ('csat', 0, 5.5, 'CSAT must be 0-5'),
        ('ces', 0, 5.5, 'CES must be 0-5'),
        ('aht', 0, AHT_SANITY_CEILING_MIN, f'AHT must be 0-{AHT_SANITY_CEILING_MIN} minutes'),
        ('cpc', 0, 100, 'CPC must be 0-100'),
        ('volume', 0, 1e9, 'Volume must be non-negative'),
    ]
    for field, lo, hi, msg in checks:
        val = q.get(field)
        if val is not None:
            try:
                v = float(val)
                if v < lo or v > hi:
                    issues.append({'field': field, 'value': v, 'expected': f'{lo}-{hi}', 'message': msg})
            except (ValueError, TypeError):
                issues.append({'field': field, 'value': val, 'message': f'{field} is not numeric'})
    if raise_on_fail and issues:
        raise ValueError(f"Queue validation failed: {issues}")
    return issues


def compute_data_quality_score(metric_sources):
    """Compute weighted data quality score from metric source tags."""
    if not metric_sources:
        return 0.0, 'Low', {}
    total = 0
    breakdown = {}
    for key, src in metric_sources.items():
        conf = src.get('confidence', 'assumed')
        weight = CONFIDENCE_WEIGHTS.get(conf, 0.2)
        total += weight
        breakdown[conf] = breakdown.get(conf, 0) + 1
    score = total / max(len(metric_sources), 1)
    label = 'High' if score >= 0.7 else 'Medium' if score >= 0.4 else 'Low'
    return round(score, 3), label, breakdown
