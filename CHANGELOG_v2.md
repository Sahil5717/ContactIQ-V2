# ContactIQ v2.0 — Release Notes

**Date:** 4 March 2026  
**Upgrade from:** v1.0  
**Total CRs Resolved:** 21 (4 Critical, 9 High, 8 Medium)

---

## Headline Metrics
| Metric | Value |
|--------|-------|
| Total 3-Year Savings | $34.9M |
| NPV | $27.7M |
| IRR | 127.2% |
| Total Investment | $5.3M |
| Enabled Initiatives | 31 / 58 |

---

## Sprint 1 — Data Integrity & Metric Reconciliation (8 CRs)

### CR-003 [High] — Remove FTE columns from initiative tables globally
FTE Impact column removed from Exec Summary, Initiative Roadmap, and all client-facing tables. Savings column retained as sole impact metric.

### CR-005 [Critical] — Fix sub-intent rows showing channel names in FCR/CSAT
Data binding corrected: sub-intent rows now display actual FCR % and CSAT scores instead of recommended channel text.

### CR-006 [Critical] — Fix Feasibility % display (8500% → 85%)
Removed double-multiplication: raw decimals (0.85) no longer multiplied by 100 twice.

### CR-007 [Critical] — Reconcile FTE numbers across pages
All pages now reference `WATERFALL.totalReduction` and `WATERFALL.totalSaving` as single source of truth. Pool labels renamed from vague "Untapped"/"Out-of-Scope" to defensible descriptions.

### CR-008 [High] — Remove McKinsey/Gartner/Bain references
All external firm references replaced with "EY ServiceEdge methodology" or "industry benchmarks" across engines, templates, and PDF export.

### CR-012 [Critical] — Single source of truth for headline metrics
STATE-level computation ensures identical FTE, savings, and investment figures on every page. No independent re-computation.

---

## Sprint 2 — Feature Fixes & Export Repairs (8 CRs)

### CR-001 [High] — Move Business Case Summary to top of Executive Summary
Business Case card (Investment, NPV, Saving, IRR, CX Revenue) and D-S-R-P tracker relocated to top of page.

### CR-002 [Medium] — Replace Health Score gauge with Waterfall chart
Semicircular gauge replaced with FTE waterfall mini-chart showing cost bridge narrative.

### CR-010 [High] — Fix Initiatives not responding to BU/Location filter
Page 9 added to invalidation list. New `_buVolumeShare()` and `_isInitRelevantToBU()` helpers scale metrics and dim non-relevant initiatives when BU filter is active.

### CR-011 [High] — Redesign Cost-Effort Bridge into connected waterfall
New `drawCostWaterfall()`: Current Cost → −AI → −OpModel → −Location → Target Cost with dashed connectors, layer labels, and savings annotation arrow.

### CR-014 [High] — Fix Channel Migration Flow not populating
Rewritten deflection matcher with broader channel/lever matching, real volume estimation from queue data, and keyword-based target channel inference.

### CR-016 [High] — Fix BU Pool Utilization not populating
Client-side `runWaterfall()` now computes `buSummary` with per-BU pool utilization, yearly projections, and initiative attribution.

### CR-018 [High] — Fix Excel BU Impact all zeros
Resolved via CR-016 fix. Consumer Yr3 = $15.2M, Enterprise Yr3 = $1.9M.

### CR-019 [High] — Fix Excel Workforce Transition all zeros
Attrition absorption capped at 60% of natural rate. Result: att=378, redep=16, sep=118, cost=$1.85M across 12 transition rows.

---

## Sprint 3 — Column Additions, Section Removals & Export Cleanup (7 CRs)

### CR-004 [Medium] — Add Sub-Intent column to Above-Benchmark Cost Queues
New "Sub-Intents" column shows up to 3 sub-intents per intent, sourced from `subIntentEnriched` data.

### CR-009 [Medium] — Add 'Deflect To' column to Feasibility Analysis
New column shows recommended deflection channel based on complexity: <0.3 → App/Self-Service, <0.5 → Chat/IVR, ≥0.5 → Chat. Applied to both intent and sub-intent rows.

### CR-013 [Medium] — Add Sub-Intent column to Intent-Channel Portfolio
New "Sub-Intents" column in portfolio table, consistent with CR-004 implementation.

### CR-015 [Medium] — Remove Scenario Comparison and Sensitivity Tornado
Removed from: page 14 (Business Case), navigation sidebar (page 21), and PDF export. `drawTornado()` function retained as dead code (unreachable).

### CR-017 [Medium] — Remove Location Mix Strategy section
Removed: global onshore/nearshore/offshore sliders, per-BU target location mix sliders, BU vs Global comparison table, and linked location initiatives display. `_locMixState` and `_buLocMixState` fully cleaned up.

### CR-020 [Medium] — Remove FTE columns from Excel export
Removed: 'Total FTE' from Executive Summary sheet, 'FTE Reduction' and 'Final FTE' from Waterfall sheet, 'Baseline FTE' and 'Reduction' from BU Impact sheet. PDF initiatives sorted by Annual Saving instead of FTE Impact.

### CR-021 [Medium] — Fix PDF export data gaps and cleanup
Risk score now correctly computed (2.4/5 Medium from 31 initiatives). Total monthly volume populates (586,579 contacts). McKinsey/Gartner footer already cleaned in CR-008. Scenario Comparison section removed per CR-015.

---

## Files Modified
| File | Sprints | CRs |
|------|---------|-----|
| `templates/index.html` | 1, 2, 3 | CR-001–017 |
| `app.py` | 2, 3 | CR-015, CR-020, CR-021 |
| `engines/workforce.py` | 2 | CR-019 |
| `engines/waterfall.py` | 1 | CR-007, CR-012 |
| `engines/pools.py` | 1 | CR-008 |
| `engines/gross.py` | 1 | CR-008 |

## Architecture Notes
- **Version:** 2.0 (API `/api/health` returns `v2.0`)
- **Pages:** 20 active (page 21 Scenario Comparison hidden from nav)
- **Template:** 9,226 lines (reduced from 9,430 after section removals)
- **Engine pipeline:** ETL → Diagnostic → Maturity → Readiness → Score → Waterfall → Workforce → Risk
- **Deployment:** Railway-ready (Procfile + railway.toml unchanged)
