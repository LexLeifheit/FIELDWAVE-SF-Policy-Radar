**FIELDWAVE SF Policy Radar**

A lightweight legislative intelligence system that monitors San Francisco legislation for developments affecting arts, culture, creative economy, and cultural infrastructure.

This project is intentionally designed as a policy intelligence tool rather than a legislative database. The goal is not simply to mirror public records, but to surface meaningful changes that merit human attention.

Rather than simply collecting legislative data, the project transforms Legistar records into an actionable policy dashboard that helps identify emerging opportunities, funding changes, governance issues, and legislation requiring attention.

⸻

**Purpose**

FIELDWAVE SF Policy Radar was built to assist communities, organizations, policymakers, funders, and advocates identify important cultural infrastructure legislation proactively. 

The system automatically reviews San Francisco legislative activity, applies arts- and culture-specific filtering, prioritizes items using policy rules, and publishes structured records into a Notion database for ongoing analysis.

**Current Features**

* Daily automated monitoring using GitHub Actions
* Legistar API integration
* Automatic export to Notion
* Arts & culture keyword detection
* Department-based triggers
* Budget and funding detection
* Priority scoring (High / Medium / Low)
* Escalation logic
* Sponsor tracking
* Legislative status tracking

**Keyword Categories**

* Arts & Culture

* Artists & Practice

* Funding & Budget

⸻
**Department Triggers**

Additional review is triggered when legislation involves organizations such as:

* San Francisco Arts Commission
* Office of Economic and Workforce Development
* Fine Arts Museums of San Francisco
* Grants for the Arts

⸻

**Workflow**

San Francisco Legistar API
            │
            ▼
    Python Monitoring Script
            │
            ▼
Keyword + Department Analysis
            │
            ▼
 Priority Assignment
            │
            ▼
     Notion Database
            │
            ▼
FIELDWAVE SF Policy Radar

**Technology**

* Python 3
* GitHub Actions
* GitHub Secrets
* Notion API
* Legistar API

**Long-Term Vision**

FIELDWAVE Policy Radar is intended to become a scalable, replicable legislative intelligence framework for arts and cultural policy.

While the current implementation focuses on San Francisco, the architecture is designed to expand to additional cities and states, supporting policy monitoring across the broader cultural sector.
