"""
ContactIQ — Maturity Assessment Engine
5-dimension assessment with 3-tier fallback scoring.
CR-004: Radar chart data structured for proper visualization
"""

DIMENSIONS = {
    'technology': {
        'label': 'Technology & AI',
        'weight': 0.25,
        'indicators': ['aiDeployment','crmIntegration','analyticsCapability','omniChannel','automationLevel'],
    },
    'process': {
        'label': 'Process Excellence',
        'weight': 0.20,
        'indicators': ['processStandard','qualityFramework','knowledgeMgmt','escalationDesign','continuousImprovement'],
    },
    'people': {
        'label': 'People & Skills',
        'weight': 0.20,
        'indicators': ['trainingProgram','careerPath','employeeEngagement','skillDiversity','leadershipCapability'],
    },
    'data': {
        'label': 'Data & Analytics',
        'weight': 0.20,
        'indicators': ['dataQuality','reportingMaturity','predictiveCapability','customerInsight','realTimeDashboard'],
    },
    'customer': {
        'label': 'Customer Experience',
        'weight': 0.15,
        'indicators': ['journeyMapping','voiceOfCustomer','personalization','channelExperience','proactiveCommunication'],
    },
}

MATURITY_LEVELS = {
    1: {'label':'Initial','description':'Ad-hoc processes, reactive approach, limited technology','color':'#dc3545'},
    2: {'label':'Developing','description':'Some standards, basic automation, emerging analytics','color':'#fd7e14'},
    3: {'label':'Defined','description':'Standardised processes, integrated technology, data-driven decisions','color':'#ffc107'},
    4: {'label':'Managed','description':'Optimised operations, advanced AI, predictive capabilities','color':'#28a745'},
    5: {'label':'Optimising','description':'Continuous innovation, AI-first, industry-leading CX','color':'#007bff'},
}


def run_maturity(data, diagnostic):
    """Assess maturity with 3-tier fallback: survey → diagnostic-inferred → industry defaults."""
    survey = data.get('maturitySurvey', {})
    params = data['params']
    industry = params.get('industry', 'general')

    dimension_scores = {}
    for dim_key, dim_def in DIMENSIONS.items():
        scores = []
        for ind in dim_def['indicators']:
            # Tier 1: Survey data
            if ind in survey and survey[ind] is not None:
                scores.append(min(5, max(1, float(survey[ind]))))
            # Tier 2: Diagnostic inference
            elif diagnostic:
                inferred = _infer_from_diagnostic(ind, dim_key, diagnostic, data)
                if inferred: scores.append(inferred)
            # Tier 3: Industry default
            if len(scores) < len(dim_def['indicators']):
                scores.append(_industry_default(dim_key, industry))

        # Take up to the number of indicators
        scores = scores[:len(dim_def['indicators'])]
        avg = sum(scores) / max(len(scores), 1)
        dimension_scores[dim_key] = {
            'label': dim_def['label'],
            'score': round(avg, 2),
            'level': min(5, max(1, round(avg))),
            'weight': dim_def['weight'],
            'indicators': scores,
            'description': MATURITY_LEVELS[min(5, max(1, round(avg)))]['description'],
            'color': MATURITY_LEVELS[min(5, max(1, round(avg)))]['color'],
        }

    # Overall
    overall = sum(ds['score'] * ds['weight'] for ds in dimension_scores.values())
    overall_level = min(5, max(1, round(overall)))

    # Gap analysis
    target = params.get('maturityTarget', 4)
    gaps = []
    for dk, ds in dimension_scores.items():
        gap = max(0, target - ds['score'])
        if gap > 0:
            gaps.append({
                'dimension': ds['label'], 'key': dk,
                'current': ds['score'], 'target': target, 'gap': round(gap, 2),
                'priority': 'high' if gap > 1.5 else 'medium' if gap > 0.5 else 'low',
                'recommendations': _gap_recommendations(dk, ds['score'], target),
            })
    gaps.sort(key=lambda x: x['gap'], reverse=True)

    # CR-004: Radar chart data
    radar = {
        'labels': [ds['label'] for ds in dimension_scores.values()],
        'current': [ds['score'] for ds in dimension_scores.values()],
        'target': [target] * len(dimension_scores),
        'industry': [_industry_default(dk, industry) for dk in dimension_scores],
    }

    return {
        'dimensions': dimension_scores,
        'overall': round(overall, 2),
        'overallLevel': overall_level,
        'levelInfo': MATURITY_LEVELS[overall_level],
        'gaps': gaps,
        'radar': radar,
        'target': target,
    }


def _infer_from_diagnostic(indicator, dimension, diagnostic, data):
    """Tier 2: Infer maturity from diagnostic scores."""
    avg_score = diagnostic.get('summary',{}).get('avgScore', 50)
    base = 1 + (avg_score / 100) * 4  # Map 0-100 → 1-5
    modifiers = {
        'technology': {'aiDeployment': -0.5, 'analyticsCapability': 0},
        'process': {'processStandard': 0.2, 'qualityFramework': -0.3},
        'people': {'trainingProgram': 0, 'employeeEngagement': 0.1},
        'data': {'dataQuality': 0.1, 'predictiveCapability': -0.5},
        'customer': {'journeyMapping': -0.3, 'personalization': -0.5},
    }
    mod = modifiers.get(dimension, {}).get(indicator, 0)
    return min(5, max(1, round(base + mod, 1)))


def _industry_default(dimension, industry):
    defaults = {
        'banking':     {'technology':3.2,'process':3.5,'people':3.0,'data':3.3,'customer':3.0},
        'insurance':   {'technology':2.8,'process':3.0,'people':2.8,'data':2.5,'customer':2.5},
        'telco':       {'technology':3.5,'process':3.0,'people':2.8,'data':3.0,'customer':3.2},
        'healthcare':  {'technology':2.5,'process':2.8,'people':3.0,'data':2.3,'customer':2.5},
        'retail':      {'technology':3.0,'process':2.8,'people':2.5,'data':2.8,'customer':3.0},
        'utilities':   {'technology':2.5,'process':3.0,'people':2.8,'data':2.5,'customer':2.3},
        'government':  {'technology':2.0,'process':2.5,'people':2.5,'data':2.0,'customer':2.0},
        'general':     {'technology':2.8,'process':2.8,'people':2.8,'data':2.5,'customer':2.5},
    }
    return defaults.get(industry, defaults['general']).get(dimension, 2.5)


def _gap_recommendations(dimension, current, target):
    recs = {
        'technology': [
            'Deploy conversational AI for top-volume query types',
            'Integrate CRM with contact centre platform for unified view',
            'Implement real-time analytics dashboard for supervisors',
        ],
        'process': [
            'Standardise call handling procedures across all queues',
            'Implement quality monitoring framework with calibration',
            'Design continuous improvement feedback loops',
        ],
        'people': [
            'Launch structured agent training and certification program',
            'Create career progression pathways for frontline staff',
            'Implement employee engagement and retention initiatives',
        ],
        'data': [
            'Establish data quality governance and cleansing routines',
            'Deploy predictive analytics for demand forecasting',
            'Create real-time performance dashboards for all levels',
        ],
        'customer': [
            'Map end-to-end customer journeys for top contact reasons',
            'Implement voice-of-customer program with actionable insights',
            'Deploy personalisation based on customer segment and history',
        ],
    }
    gap = target - current
    r = recs.get(dimension, ['Review and improve current capabilities'])
    return r[:3] if gap > 1.5 else r[:2] if gap > 0.5 else r[:1]
