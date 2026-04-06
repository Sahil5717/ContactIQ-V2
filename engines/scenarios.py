"""
ContactIQ — Scenario Comparison & Delta Engine
CR-FIX-R: Compare multiple scenarios side-by-side.
CR-FIX-S: Track what changed between runs and why.
"""
import copy
import logging


def compare_scenarios(data, initiatives, run_waterfall_fn):
    """Run source vs normalized vs conservative vs stretch scenarios."""
    results = []
    
    # Scenario 1: Source volume (current default)
    try:
        wf_source = run_waterfall_fn(data, initiatives, _skip_sensitivity=True, _skip_scenarios=True)
        results.append({
            'name': 'Source Volume',
            'basis': 'source',
            'saving': round(wf_source.get('totalSaving', 0)),
            'fte': wf_source.get('totalReduction', 0),
            'npv': round(wf_source.get('totalNPV', 0)),
            'roi': round(wf_source.get('roi', 0), 1),
            'irr': round(wf_source.get('irr', 0), 1),
            'payback': round(wf_source.get('payback', 0), 1),
            'investment': round(wf_source.get('totalInvestment', 0)),
        })
    except Exception as e:
        logging.warning(f'[Scenario] Source failed: {e}')
    
    # Scenario 2: Capacity-normalized volume
    try:
        data_norm = copy.deepcopy(data)
        data_norm['params']['volumeBasis'] = 'capacity_normalized'
        # Apply normalized volumes
        for q in data_norm['queues']:
            q['volume'] = q.get('normalizedVolume', q['volume'])
        data_norm['totalVolume'] = sum(q['volume'] for q in data_norm['queues'])
        wf_norm = run_waterfall_fn(data_norm, copy.deepcopy(initiatives), _skip_sensitivity=True, _skip_scenarios=True)
        results.append({
            'name': 'Capacity-Normalized',
            'basis': 'capacity_normalized',
            'saving': round(wf_norm.get('totalSaving', 0)),
            'fte': wf_norm.get('totalReduction', 0),
            'npv': round(wf_norm.get('totalNPV', 0)),
            'roi': round(wf_norm.get('roi', 0), 1),
            'irr': round(wf_norm.get('irr', 0), 1),
            'payback': round(wf_norm.get('payback', 0), 1),
            'investment': round(wf_norm.get('totalInvestment', 0)),
        })
    except Exception as e:
        logging.warning(f'[Scenario] Normalized failed: {e}')
    
    # Scenario 3: Conservative (70% adoption, 115% investment)
    try:
        inits_con = copy.deepcopy(initiatives)
        for i in inits_con:
            i['adoption'] = min(1.0, i.get('adoption', 0.8) * 0.70)
        wf_con = run_waterfall_fn(data, inits_con, _skip_sensitivity=True, _skip_scenarios=True)
        results.append({
            'name': 'Conservative',
            'basis': 'source',
            'saving': round(wf_con.get('totalSaving', 0)),
            'fte': wf_con.get('totalReduction', 0),
            'npv': round(wf_con.get('totalNPV', 0)),
            'roi': round(wf_con.get('roi', 0), 1),
            'irr': round(wf_con.get('irr', 0), 1),
            'payback': round(wf_con.get('payback', 0), 1),
            'investment': round(wf_con.get('totalInvestment', 0) * 1.15),
        })
    except Exception as e:
        logging.warning(f'[Scenario] Conservative failed: {e}')
    
    # Scenario 4: Stretch (130% adoption, 90% investment)
    try:
        inits_str = copy.deepcopy(initiatives)
        for i in inits_str:
            i['adoption'] = min(1.0, i.get('adoption', 0.8) * 1.30)
        wf_str = run_waterfall_fn(data, inits_str, _skip_sensitivity=True, _skip_scenarios=True)
        results.append({
            'name': 'Stretch',
            'basis': 'source',
            'saving': round(wf_str.get('totalSaving', 0)),
            'fte': wf_str.get('totalReduction', 0),
            'npv': round(wf_str.get('totalNPV', 0)),
            'roi': round(wf_str.get('roi', 0), 1),
            'irr': round(wf_str.get('irr', 0), 1),
            'payback': round(wf_str.get('payback', 0), 1),
            'investment': round(wf_str.get('totalInvestment', 0) * 0.90),
        })
    except Exception as e:
        logging.warning(f'[Scenario] Stretch failed: {e}')
    
    return results


def compute_delta(current_state, previous_state):
    """CR-FIX-S: Compute what changed between two states and why."""
    if not previous_state:
        return None
    
    delta = {'changes': [], 'mainDrivers': []}
    
    # Compare key metrics
    metrics = [
        ('totalSaving', 'Total Saving', '$'),
        ('totalReduction', 'FTE Reduction', 'int'),
        ('totalNPV', 'NPV', '$'),
        ('roi', 'ROI', '%'),
        ('totalInvestment', 'Investment', '$'),
    ]
    
    for key, label, fmt in metrics:
        curr = current_state.get(key, 0)
        prev = previous_state.get(key, 0)
        if prev != 0:
            change_pct = ((curr - prev) / abs(prev)) * 100
        else:
            change_pct = 100 if curr != 0 else 0
        if abs(change_pct) > 1:  # only report changes > 1%
            delta['changes'].append({
                'metric': label,
                'before': prev,
                'after': curr,
                'changePct': round(change_pct, 1),
                'direction': 'up' if curr > prev else 'down',
            })
    
    # Compare enabled initiative count
    curr_enabled = len(current_state.get('enabledInits', []))
    prev_enabled = len(previous_state.get('enabledInits', []))
    if curr_enabled != prev_enabled:
        delta['changes'].append({
            'metric': 'Enabled Initiatives',
            'before': prev_enabled,
            'after': curr_enabled,
            'changePct': round(((curr_enabled - prev_enabled) / max(prev_enabled, 1)) * 100, 1),
            'direction': 'up' if curr_enabled > prev_enabled else 'down',
        })
    
    # Infer main drivers
    drivers = []
    vol_basis_curr = current_state.get('_volumeBasis', 'source')
    vol_basis_prev = previous_state.get('_volumeBasis', '')
    if vol_basis_curr != vol_basis_prev and vol_basis_prev:
        drivers.append(f'Volume basis changed: {vol_basis_prev} → {vol_basis_curr}')
    
    saving_change = current_state.get('totalSaving', 0) - previous_state.get('totalSaving', 0)
    if abs(saving_change) > 100000:
        if curr_enabled != prev_enabled:
            drivers.append(f'Initiative count changed ({prev_enabled} → {curr_enabled})')
        if saving_change > 0:
            drivers.append('Saving increased — likely more initiatives enabled or assumptions loosened')
        else:
            drivers.append('Saving decreased — likely more conservative assumptions or fewer initiatives')
    
    delta['mainDrivers'] = drivers
    delta['summary'] = f'{len(delta["changes"])} metrics changed' + (f': {", ".join(drivers)}' if drivers else '')
    
    return delta
