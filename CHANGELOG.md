### Current Agenda Source
- Switch the default monitor source to current public Legistar meeting agendas.
- Keep the older `/matters` API path available with `--source api` for diagnostics.
- Add `--agenda-days` and `--source auto` options for recent agenda scanning and fallback behavior.
- Document why the public SF `/matters` API is not reliable enough for current policy monitoring.

### Portfolio Demo
- Add a credential-free dry-run mode for previewing policy matches without Notion secrets.
- Sort Legistar matters by last modified date and use configurable lookback windows for review.
- Expand cultural policy signal detection and tighten keyword matching to reduce false positives.
- Rewrite README around a quick reviewer-friendly demo and policy director workflow.

### Maintenance
- Update GitHub Actions to Node.js 24-compatible action versions before September 2026.
