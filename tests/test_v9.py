"""
ContactIQ V9 — Regression Tests
Validates all V9 changes: volume normalization, cap bridge,
auth fixes, channel synonyms, bcrypt, rate limiting.
Run: python -m pytest tests/test_v9.py -v
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.chdir(os.path.join(os.path.dirname(__file__), '..'))

import unittest


class TestChannelNormalization(unittest.TestCase):
    """V9: DIGITAL and MESSAGE channels must be normalized."""

    def test_no_unmapped_channels(self):
        from engines.data_loader import run_etl
        data = run_etl()
        valid = {'Voice', 'Chat', 'Email', 'IVR', 'App/Self-Service', 'Social Media', 'SMS/WhatsApp', 'Retail/Walk-in'}
        for q in data['queues']:
            self.assertIn(q['channel'], valid,
                f"Queue {q['queue']} has unmapped channel '{q['channel']}'")

    def test_digital_maps_to_self_service(self):
        from engines.data_loader import normalize_channel
        self.assertEqual(normalize_channel('DIGITAL'), 'App/Self-Service')
        self.assertEqual(normalize_channel('Digital'), 'App/Self-Service')

    def test_message_maps_to_sms(self):
        from engines.data_loader import normalize_channel
        self.assertEqual(normalize_channel('MESSAGE'), 'SMS/WhatsApp')
        self.assertEqual(normalize_channel('Message'), 'SMS/WhatsApp')


class TestVolumeNormalization(unittest.TestCase):
    """V9: Pool ceilings must auto-normalize when volume coverage is low."""

    def test_scaling_factor_in_params(self):
        from engines.data_loader import run_etl
        data = run_etl()
        self.assertIn('volumeScalingFactor', data['params'],
            "volumeScalingFactor must be injected into params")
        self.assertGreater(data['params']['volumeScalingFactor'], 1.0,
            "Scaling factor should be > 1 for this test data")

    def test_pool_ceilings_normalized(self):
        from engines.data_loader import run_etl
        from engines.intent_profile import enrich_intents
        from engines.pools import compute_pools
        data = run_etl()
        enriched = enrich_intents(data['queues'], data['params'])
        pools = compute_pools(enriched, data['roles'], data['params'])
        defl = pools['pools']['deflection']
        # With normalization, deflection ceiling should be >> 20 FTE
        self.assertGreater(defl['ceiling_fte'], 50,
            f"Deflection pool ceiling {defl['ceiling_fte']} too low — normalization may have failed")


class TestAuthFixes(unittest.TestCase):
    """V9: Auth/emotion penalties must not crush billing containment."""

    def test_billing_auth_moderate(self):
        from engines.intent_profile import _auth_required_from_complexity
        auth = _auth_required_from_complexity(0.30, 'Billing & Payments')
        self.assertLessEqual(auth, 0.50,
            f"Billing auth {auth} too high — should be moderate (<=0.50)")
        self.assertGreater(auth, 0.20,
            f"Billing auth {auth} too low — should reflect auth need")

    def test_billing_emotion_not_elevated(self):
        from engines.intent_profile import _emotional_risk_from_complexity
        emotion = _emotional_risk_from_complexity(0.30, 'Billing & Payments')
        self.assertLess(emotion, 0.50,
            f"Billing emotion {emotion} too high — billing is routine")

    def test_containment_no_halving(self):
        from engines.intent_profile import _containment_feasibility
        # High auth should reduce but not halve
        contain_high = _containment_feasibility(0.65, 0.25, 0.85, 0.30)
        contain_low = _containment_feasibility(0.65, 0.25, 0.20, 0.30)
        ratio = contain_high / contain_low
        self.assertGreater(ratio, 0.45,
            f"High-auth containment ratio {ratio:.2f} — halving penalty may still be active")

    def test_eligible_pct_is_pure_repeatability(self):
        from engines.data_loader import run_etl
        from engines.intent_profile import enrich_intents, _repeatability_from_complexity
        data = run_etl()
        enriched = enrich_intents(data['queues'], data['params'])
        for q in enriched:
            expected = round(_repeatability_from_complexity(q.get('complexity', 0.5)), 3)
            actual = q.get('deflection_eligible_pct', 0)
            self.assertAlmostEqual(actual, expected, places=2,
                msg=f"Queue {q.get('queue','?')}: eligible_pct {actual} != repeatability {expected}")
            break  # Just check first queue


class TestCapBridge(unittest.TestCase):
    """V9: Cap bridge must be computed and present in waterfall output."""

    def test_cap_bridge_exists(self):
        from engines.data_loader import run_etl
        from engines.waterfall import INITIATIVE_LIBRARY, run_waterfall
        import copy
        data = run_etl()
        inits = copy.deepcopy(INITIATIVE_LIBRARY)
        for i in inits: i['enabled'] = True
        wf = run_waterfall(data, inits)
        self.assertIn('capBridge', wf, "capBridge missing from waterfall output")

    def test_cap_bridge_fields(self):
        from engines.data_loader import run_etl
        from engines.waterfall import INITIATIVE_LIBRARY, run_waterfall
        import copy
        data = run_etl()
        inits = copy.deepcopy(INITIATIVE_LIBRARY)
        for i in inits: i['enabled'] = True
        wf = run_waterfall(data, inits)
        bridge = wf['capBridge']
        for field in ['simpleAddressable', 'physicsGross', 'dataAdjustment', 'netFTE', 'year1FTE']:
            self.assertIn(field, bridge, f"capBridge missing field '{field}'")
        self.assertGreater(bridge['simpleAddressable'], bridge['year1FTE'],
            "Addressable must be > year1FTE (caps reduce value)")

    def test_cap_constants_synced(self):
        from engines.waterfall import ABSOLUTE_SINGLE_INIT_CAP as wf_cap, PER_ROLE_MAX_REDUCTION as wf_role
        from engines.constants import ABSOLUTE_SINGLE_INIT_CAP as c_cap, PER_ROLE_MAX_REDUCTION as c_role
        self.assertEqual(wf_cap, c_cap, "ABSOLUTE_SINGLE_INIT_CAP mismatch between waterfall.py and constants.py")
        self.assertEqual(wf_role, c_role, "PER_ROLE_MAX_REDUCTION mismatch between waterfall.py and constants.py")


class TestFTEOutput(unittest.TestCase):
    """V9: FTE output must be in defensible range."""

    def test_default_config_fte_range(self):
        from engines.data_loader import run_etl
        from engines.diagnostic import run_diagnostic
        from engines.maturity import run_maturity
        from engines.readiness import compute_readiness
        from engines.waterfall import score_initiatives, run_waterfall
        data = run_etl()
        diag = run_diagnostic(data)
        mat = run_maturity(data, diag)
        readiness = compute_readiness(data, diag, mat)
        inits = score_initiatives(data, diag, readiness)
        wf = run_waterfall(data, inits)
        fte = wf['totalReduction']
        self.assertGreater(fte, 200,
            f"FTE {fte} too low — cap choke may have regressed")
        self.assertLess(fte, 600,
            f"FTE {fte} too high — caps may be too loose")

    def test_confidence_bands_meaningful(self):
        from engines.data_loader import run_etl
        from engines.diagnostic import run_diagnostic
        from engines.maturity import run_maturity
        from engines.readiness import compute_readiness
        from engines.waterfall import score_initiatives, run_waterfall
        data = run_etl()
        diag = run_diagnostic(data)
        mat = run_maturity(data, diag)
        readiness = compute_readiness(data, diag, mat)
        inits = score_initiatives(data, diag, readiness)
        wf = run_waterfall(data, inits)
        cb = wf.get('confidenceBands', {})
        fte_band = cb.get('fteReduction', {})
        if fte_band.get('base', 0) > 0:
            spread = abs(fte_band.get('high', 0) - fte_band.get('low', 0))
            pct = spread / fte_band['base'] * 100
            self.assertGreater(pct, 10,
                f"Confidence band spread {pct:.0f}% too narrow — should be >10%")


class TestBcrypt(unittest.TestCase):
    """V9: Password hashing must use bcrypt with legacy migration."""

    def test_new_hash_is_bcrypt(self):
        from infrastructure.database import _hash_password
        hashed, salt = _hash_password('testpassword')
        self.assertTrue(hashed.startswith('$2'), f"Hash {hashed[:10]} is not bcrypt")
        self.assertEqual(salt, 'bcrypt')

    def test_bcrypt_verify(self):
        from infrastructure.database import _hash_password, _verify_bcrypt
        hashed, _ = _hash_password('testpassword')
        self.assertTrue(_verify_bcrypt('testpassword', hashed))
        self.assertFalse(_verify_bcrypt('wrongpassword', hashed))

    def test_legacy_sha256_verify(self):
        import hashlib, secrets
        from infrastructure.database import _verify_legacy_sha256
        salt = secrets.token_hex(16)
        legacy = hashlib.sha256((salt + 'admin123').encode()).hexdigest()
        self.assertTrue(_verify_legacy_sha256('admin123', legacy, salt))
        self.assertFalse(_verify_legacy_sha256('wrong', legacy, salt))


class TestRecommendationLevers(unittest.TestCase):
    """V9: SIGNAL_TO_LEVER must use real initiative levers."""

    def test_no_phantom_levers(self):
        from engines.recommendations import SIGNAL_TO_LEVER
        valid_levers = {'deflection', 'aht_reduction', 'escalation_reduction',
                        'repeat_reduction', 'cost_reduction', 'shrinkage_reduction',
                        'transfer_reduction'}
        for signal, levers in SIGNAL_TO_LEVER.items():
            for lever in levers:
                self.assertIn(lever, valid_levers,
                    f"Signal '{signal}' maps to phantom lever '{lever}' — not a real initiative lever")


class TestRiskDependencies(unittest.TestCase):
    """V9: Dependency map must cover more than 5 initiatives."""

    def test_dependency_map_expanded(self):
        from engines.risk import DEPENDENCY_MAP
        self.assertGreater(len(DEPENDENCY_MAP), 15,
            f"Dependency map has {len(DEPENDENCY_MAP)} entries — should be >15")


class TestFloorLogic(unittest.TestCase):
    """V9: Floor logic must preserve minimum value for high-confidence data."""

    def test_floor_field_exists(self):
        from engines.data_loader import run_etl
        from engines.waterfall import INITIATIVE_LIBRARY, run_waterfall
        import copy
        data = run_etl()
        inits = copy.deepcopy(INITIATIVE_LIBRARY)
        for i in inits: i['enabled'] = True
        wf = run_waterfall(data, inits)
        # At least one initiative should have _floorApplied field
        has_floor = any('_floorApplied' in i for i in inits if i.get('enabled'))
        self.assertTrue(has_floor, "_floorApplied field not found on any initiative")


if __name__ == '__main__':
    unittest.main()
