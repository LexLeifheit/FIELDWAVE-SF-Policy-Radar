# FIELDWAVE SF Policy Radar

FIELDWAVE SF Policy Radar is a lightweight legislative intelligence tool for San Francisco arts, culture, creative economy, and cultural infrastructure policy.

The project is designed as a policy triage system, not a public-records mirror. It monitors current public Legistar agendas, detects culturally relevant signals, assigns priority, and can publish structured items into a Notion database for ongoing review.

## Quick Demo

Run a credential-free preview:

```bash
python monitor_legistar.py --dry-run --years 10 --limit 5
```

Example output:

```text
# FIELDWAVE SF Policy Radar Preview

| Priority | File | Title | Status | Signals |
| --- | --- | --- | --- | --- |
| HIGH | 260741 | Resolution designating the San Francisco Arts Commission as the City and County of San Francisco's agency serving as the state-local partner... | For Immediate Adoption | arts_culture, arts commission |
```

For machine-readable output:

```bash
python monitor_legistar.py --dry-run --json --years 10 --limit 5
```

The default monitor scans current and recent public agendas. The older Legistar `/matters` API can still be used with `--source api`, but it is not the default because the SF endpoint does not reliably expose the newest legislative activity.

## What It Does

- Pulls current San Francisco meeting agendas from the public Legistar site
- Uses agenda dates so recently scheduled, adopted, or pending items surface first
- Detects arts, culture, creative economy, funding, and cultural infrastructure signals
- Applies department triggers for cultural policy bodies such as the Arts Commission, Fine Arts Museums, Grants for the Arts, and OEWD
- Assigns priority levels for policy review
- Supports a fast dry-run preview without Notion credentials
- Exports enriched records to Notion when credentials are configured
- Retries transient API failures so a temporary remote disconnect does not fail the whole run

## Why It Matters

Arts and cultural policy often appears inside broader budget, land use, economic development, and governance items. FIELDWAVE helps surface those items early enough for staff, advocates, funders, and community partners to decide whether action is needed.

For a policy director workflow, the tool supports:

- Agenda scanning before committee meetings
- Early identification of funding or district-impact items
- A shared review queue for staff or coalition partners
- Lightweight institutional memory in Notion
- Replicable monitoring logic for other cities or policy domains

## Running the Monitor

Install runtime dependencies:

```bash
pip install requests beautifulsoup4
```

Preview matches without exporting:

```bash
python monitor_legistar.py --dry-run
```

Export to Notion:

```bash
export NOTION_TOKEN="secret_..."
export NOTION_DATABASE_ID="..."
python monitor_legistar.py
```

Optional flags:

- `--dry-run`: print matches without creating Notion pages
- `--json`: print dry-run results as JSON
- `--limit 5`: stop after collecting a number of matching items
- `--years 10`: adjust the freshness window
- `--include-details`: fetch slower sponsor and history details during dry runs
- `--source agenda`: scan public agendas, the default and recommended source
- `--source api`: use the older Legistar `/matters` endpoint for diagnostics or historical demos
- `--source auto`: use agendas first, then fall back to the API if no agenda matches are found
- `--agenda-days 14`: change how many days of recent agendas to scan

## Data Source Note

San Francisco's public Legistar `/matters` API endpoint may lag or omit current legislative activity. For example, current agendas in the browser UI can show 2026 files while `/v1/sfgov/matters` returns older matter IDs and no 2026 `MatterIntroDate` records. FIELDWAVE therefore treats public meeting agendas as the operational source of truth for recent policy monitoring.

## Automation

The included GitHub Actions workflow runs the monitor daily and can also be triggered manually. Notion credentials should be stored as repository secrets:

- `NOTION_TOKEN`
- `NOTION_DATABASE_ID`

## Current Scope

This version focuses on San Francisco. The same pattern can be extended to additional jurisdictions by swapping the Legistar base URL and tuning the policy signal dictionary.
