# FIELDWAVE SF Policy Radar

FIELDWAVE SF Policy Radar is a lightweight legislative intelligence system for San Francisco arts, culture, creative economy, and cultural infrastructure policy.

The project is designed as a policy triage tool, not a public-records mirror. It scans current public Legistar agendas, enriches agenda items from Legistar detail pages, classifies culturally relevant policy signals, and publishes structured records into Notion for ongoing review.

## What It Does

- Monitors current and recent San Francisco Board of Supervisors, committee, and subcommittee agendas
- Uses public agenda pages as the operational source of truth for recent legislative activity
- Enriches each agenda item from its `LegislationDetail.aspx` page, including action history and final outcomes where available
- Detects arts, culture, creative economy, funding, venues, public space, housing, workforce, technology, governance, and cultural equity signals
- Applies a Notion-based taxonomy to populate `Primary Category`, `Subcategories`, `Impact Level`, `Policy Signal`, `Urgency`, and `Why It Matters`
- Marks `Communication` items as `Low` impact so informational filings do not crowd out actionable policy items
- Updates `Passed`, `Failed`, `Passed Date`, and `Failed Date` from current legislative action history
- De-duplicates Notion records by file number
- Preserves manually edited Notion judgment fields after initial population
- Runs daily through GitHub Actions and can also be triggered manually

## Why It Matters

Arts and cultural policy often appears inside broader budget, land use, public space, economic development, labor, governance, and equity items. FIELDWAVE helps surface those signals early enough for staff, advocates, funders, and community partners to decide whether action is needed.

For a policy director workflow, the tool supports:

- Agenda scanning before Board and committee meetings
- Early identification of funding, cultural district, venue, public space, and creative economy items
- A shared Notion review queue for staff or coalition partners
- Human-in-the-loop policy judgment without losing automated updates
- Lightweight institutional memory across legislative actions
- A reusable model for monitoring other jurisdictions or policy domains

## Current Monitor

The scheduled workflow runs:

```bash
python monitor_current_agenda_preserve_manual.py
```

That wrapper runs the agenda monitor while preserving manually edited Notion fields on later updates.

Protected manual fields:

- `Primary Category`
- `Subcategories`
- `Impact Level`
- `Policy Signal`
- `Urgency`
- `Why It Matters`

The monitor may still update live factual fields such as `Latest Action`, `Action Date`, `Status`, `Passed`, `Failed`, `Passed Date`, `Failed Date`, `Date Checked`, sponsors, committees, and source links.

Optional review fields supported in Notion:

- `Needs Review`
- `Last Machine Primary Category`
- `Last Machine Subcategories`
- `Last Machine Impact Level`
- `Last Machine Policy Signal`
- `Last Machine Urgency`

These fields let the system show its latest machine read without overwriting manual policy analysis.

## Running Locally

Install dependencies:

```bash
pip install requests beautifulsoup4
```

Preview matches without exporting to Notion:

```bash
python monitor_current_agenda_preserve_manual.py --dry-run --limit 5
```

Preview as JSON:

```bash
python monitor_current_agenda_preserve_manual.py --dry-run --json --limit 5
```

Export to Notion:

```bash
export NOTION_TOKEN="secret_..."
export NOTION_DATABASE_ID="..."
python monitor_current_agenda_preserve_manual.py
```

Useful options:

- `--dry-run`: preview matches without creating or updating Notion pages
- `--json`: print dry-run results as JSON
- `--limit 5`: stop after collecting a number of matching items
- `--agenda-days 14`: change how many days of recent agendas to scan

## Data Source Note

San Francisco's public Legistar `/matters` API can lag or omit the newest legislative activity. FIELDWAVE therefore treats current public meeting agendas and linked Legistar detail pages as the operational source of truth for recent monitoring.

The older `monitor_legistar.py` script remains in the repository for historical API diagnostics and comparison, but the active scheduled monitor is `monitor_current_agenda_preserve_manual.py`.

## Automation

The included GitHub Actions workflow runs daily and can also be triggered manually. Notion credentials should be stored as repository secrets:

- `NOTION_TOKEN`
- `NOTION_DATABASE_ID`

## Current Scope

This version focuses on San Francisco cultural policy. The same pattern can be extended to additional jurisdictions by swapping the Legistar source and tuning the taxonomy, department triggers, and policy signal dictionary.
