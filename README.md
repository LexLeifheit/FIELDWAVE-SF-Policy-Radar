# FIELDWAVE SF Policy Radar

FIELDWAVE SF Policy Radar is a lightweight legislative intelligence tool for San Francisco arts, culture, creative economy, and cultural infrastructure policy.

The project is designed as a policy triage system, not a public-records mirror. It monitors Legistar records, detects culturally relevant signals, assigns priority, and can publish structured items into a Notion database for ongoing review.

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
| HIGH | 181080 | Ordinance amending the Administrative Code to establish the African American Arts and Cultural District... | Unfinished Business-Final Passage | arts_culture, cultural_infrastructure |
```

For machine-readable output:

```bash
python monitor_legistar.py --dry-run --json --years 10 --limit 5
```

The default monitor uses a three-year lookback. The ten-year option is useful for portfolio review because the public SF Legistar API currently returns recently modified historical records in this endpoint.

## What It Does

- Pulls San Francisco legislative records from the Legistar API
- Sorts by recently modified records so updates surface first
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

Install the only runtime dependency:

```bash
pip install requests
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

## Automation

The included GitHub Actions workflow runs the monitor daily and can also be triggered manually. Notion credentials should be stored as repository secrets:

- `NOTION_TOKEN`
- `NOTION_DATABASE_ID`

## Current Scope

This version focuses on San Francisco. The same pattern can be extended to additional jurisdictions by swapping the Legistar base URL and tuning the policy signal dictionary.
