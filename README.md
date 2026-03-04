# ContactIQ — Intelligent Contact Center Optimization Platform

**Version:** 1.0 (Sprint 1)
**Stack:** Flask + HTML/JS (Chart.js)

## Overview

ContactIQ is a contact center transformation intelligence platform with 8 analytical modules:

1. **Executive Summary** — SCR narrative with headline metrics
2. **Diagnostic Engine** — Queue health scoring, cost analysis, sub-intent decomposition
3. **Maturity Assessment** — 5-dimension maturity model
4. **Channel Strategy** — Migration flows, feasibility analysis, portfolio optimization
5. **Initiative Roadmap** — 58-initiative library with relevance scoring
6. **Business Case** — Pool-based waterfall, NPV, IRR, scenario comparison
7. **Workforce Transition** — Role-level impact, attrition modelling, reskill matrix
8. **Operating Model** — Location strategy, tiered model, cost bridge

## Architecture

- **Pool-based netting:** Anti-double-counting via finite opportunity pools
- **3-layer framework:** AI & Automation → Operating Model → Location Strategy
- **BU×Location×Sourcing dimensional engine**
- **3-tier RBAC:** Viewer / Supervisor / Admin
- **Industry-configurable templates**

## Running

```bash
pip install flask openpyxl fpdf2
python app.py
```

Navigate to http://localhost:5000
