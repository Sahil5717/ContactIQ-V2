# ContactIQ v8.0 — Trusted Calculation Engine

**EY Contact Centre Opportunity Assessment**
Version 8.0 | April 2026 | EY GDS

## What's new in v8
- **Source volume as default** — no silent scaling; consultant toggles to capacity-normalized explicitly
- **CRM integration** — real FCR, escalation, repeat from 18,236 case records
- **WFM integration** — real shrinkage, occupancy, utilization from 599 agent-months
- **Transcript analysis** — voice silence detection, chat bot handling, email SLA breach
- **Insight engine** — hypothesis-driven diagnostics, not auto-solutioning
- **Scenario comparison** — source vs normalized vs conservative vs stretch
- **Evidence cards** — every recommendation shows data sources, assumptions, invalidators
- **Confidence bands** — per-initiative uncertainty ranges by lever type
- **Queue confidence model** — metric-level confidence scoring per queue
- **Data quality scoring** — weighted (actual=1.0, derived=0.6, assumed=0.2)
- **4 roles** — admin, supervisor, analyst, client (with calculation rights governance)
- **Override audit trail** — every change logged with who/what/when/why
- **25 regression tests** — AHT units, volume scaling, escalation consistency, pool caps
- **Pipeline refactored** — 6 named stages for testability

## Changes in V7.2

### §3 Read-Only Lock
- Pages gained in Delivery mode show "🔒 Read Only" banner for EY US and Client
- All buttons, inputs, checkboxes, selects on RO pages are disabled with visual dimming
- Supervisor RO pages in delivery: 9, 11, 12, 13
- Analyst RO pages in delivery: 9, 11, 12, 13, 14
- Admin never locked

### F.2.3 All Pillars Toggled Off
- Warning banner: "⚠️ No Opportunity Selected — All transformation pillars are currently disabled"
- Per-pillar greyed state: disabled pillars show $0 with "Pillar disabled — enable to see opportunity"
- Pillar drill-down links removed when pillar is off (no dead clicks)
- FTE/Savings range headers show $0 naturally

### Spec Coverage: Complete
All 14 sections + 6 appendices implemented. See V7.1 README for full matrix.

## Running
```bash
pip install flask openpyxl fpdf2 gunicorn
python app.py
```
