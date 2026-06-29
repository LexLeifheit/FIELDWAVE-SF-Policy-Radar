import argparse
from datetime import datetime
import json
import os
import re

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

LEGISTAR_BASE = "https://webapi.legistar.com/v1/sfgov"

NOTION_API = "https://api.notion.com/v1/pages"
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")

REQUEST_TIMEOUT = (10, 30)
DEFAULT_LOOKBACK_YEARS = 3
DEFAULT_MATTER_LIMIT = 1000


def build_session():
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = build_session()

# -------------------------------------------------
# KEYWORDS
# -------------------------------------------------

KEYWORD_GROUPS = {
    "arts_culture": [
        "cultural",
        "arts",
        "artistic",
        "entertainment",
        "museum",
        "museums",
        "performing arts",
        "public art",
        "theater",
        "theatre",
    ],
    "artists_practice": [
        "artist",
        "artists",
        "creative",
        "creative economy",
        "film",
        "music",
        "nightlife",
        "performance",
    ],
    "funding_budget": [
        "appropriation",
        "budget",
        "grant",
        "grants",
        "fund",
        "funding",
        "tax",
    ],
    "cultural_infrastructure": [
        "cultural district",
        "cultural districts",
        "historic preservation",
        "legacy business",
        "small business",
        "venue",
        "zoning",
    ]
}

DEPARTMENT_TRIGGERS = [
    "arts commission",
    "fine arts museums",
    "grants for the arts",
    "office of economic and workforce development",
    "oeewd",
]
KNOWN_STATUSES = {
    "30 Day Rule",
    "Completed",
    "Consent Agenda",
    "Disapproved",
    "Discussed and Filed",
    "Failed",
    "Filed",
    "First Reading",
    "First Reading, Consent",
    "For Immediate Adoption",
    "Heard",
    "Introduced",
    "Killed",
    "Litigation-Attorney",
    "Mayors Office",
    "New Business",
    "Passed",
    "Pending",
    "Pending Committee Action",
    "Special Order",
    "To be Scheduled for Public Hearing",
    "Unfinished Business",
    "Unfinished Business-Final Passage",
    "Vetoed",
    "Withdrawn"
}

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def fetch_matters():
    try:
        r = SESSION.get(
            f"{LEGISTAR_BASE}/matters",
            params={
                "$orderby": "MatterLastModifiedUtc desc",
                "$top": DEFAULT_MATTER_LIMIT,
            },
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except RequestException as exc:
        raise RuntimeError(f"Failed to fetch matters: {exc}") from exc

def fetch_history(matter_id):
    try:
        r = SESSION.get(
            f"{LEGISTAR_BASE}/matters/{matter_id}/history",
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            return []
        return r.json()
    except RequestException as exc:
        print(f"Warning: failed to fetch history for {matter_id}: {exc}")
        return []

def fetch_sponsors(matter_id):
    """
    Returns (primary_sponsor, secondary_sponsors[])
    """
    try:
        r = SESSION.get(
            f"{LEGISTAR_BASE}/matters/{matter_id}/sponsors",
            timeout=REQUEST_TIMEOUT,
        )
    except RequestException as exc:
        print(f"Warning: failed to fetch sponsors for {matter_id}: {exc}")
        return None, []

    if r.status_code != 200:
        return None, []

    sponsors = r.json()
    primary = None
    secondary = []

    for s in sponsors:
        name = s.get("MatterSponsorName")
        seq = s.get("MatterSponsorSequence")
        is_primary = s.get("MatterSponsorPrimary")

        if not name:
            continue

        seq_num = None
        if isinstance(seq, int):
            seq_num = seq
        elif isinstance(seq, str) and seq.isdigit():
            seq_num = int(seq)

        if is_primary is True or seq_num == 1:
            primary = name
        elif seq_num and seq_num > 1:
            secondary.append(name)

    return primary, secondary

def parse_legistar_date(value):
    if not value:
        return ""

    if isinstance(value, str):
        if value.startswith("/Date(") and value.endswith(")/"):
            inner = value[len("/Date("):-len(")/")]
            digits = "".join(ch for ch in inner if ch.isdigit())
            if digits:
                timestamp_ms = int(digits)
                return datetime.utcfromtimestamp(timestamp_ms / 1000).date().isoformat()
        try:
            normalized = value.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized).date().isoformat()
        except ValueError:
            return ""

    return ""

def subtract_years(date_value, years):
    try:
        return date_value.replace(year=date_value.year - years)
    except ValueError:
        if date_value.month == 2 and date_value.day == 29:
            return date_value.replace(year=date_value.year - years, day=28)
        raise
        
def match_keywords(text):
    text = text.lower()
    hits = {}
    for group, words in KEYWORD_GROUPS.items():
        matches = [
            word for word in words
            if re.search(rf"\b{re.escape(word)}\b", text)
        ]
        if matches:
            hits[group] = matches
    return hits

def match_department_triggers(text):
    text = text.lower()
    return [
        trigger for trigger in DEPARTMENT_TRIGGERS
        if re.search(rf"\b{re.escape(trigger)}\b", text)
    ]

def is_policy_relevant(keyword_hits, department_hits):
    if department_hits:
        return True

    signal_groups = set(keyword_hits)
    if "funding_budget" in signal_groups and len(signal_groups) == 1:
        return False

    return bool(signal_groups)

def truncate_text(value, max_length=180):
    value = " ".join(value.split())
    if len(value) <= max_length:
        return value
    return f"{value[:max_length - 3]}..."

def assign_priority(keyword_hits, department_hits):
    if "funding_budget" in keyword_hits and (
        department_hits or len(keyword_hits) > 1
    ):
        return "HIGH"
    if department_hits and keyword_hits:
        return "HIGH"
    if len(keyword_hits) > 1:
        return "HIGH"
    if keyword_hits or department_hits:
        return "MEDIUM"
    return "LOW"

def format_markdown_report(items):
    if not items:
        return "No matching cultural policy items found in the current Legistar feed."

    lines = [
        "# FIELDWAVE SF Policy Radar Preview",
        "",
        f"Generated: {datetime.utcnow().date().isoformat()} UTC",
        "",
        "| Priority | File | Title | Status | Signals |",
        "| --- | --- | --- | --- | --- |",
    ]

    for item in items:
        signals = item["keyword_groups"] + item["department_triggers"]
        lines.append(
            "| {priority} | {file_number} | {title} | {status} | {signals} |".format(
                priority=item["priority"],
                file_number=item["file_number"] or "-",
                title=truncate_text(item["title"]).replace("|", "\\|"),
                status=item["status"].replace("|", "\\|"),
                signals=", ".join(signals) or "-",
            )
        )

    return "\n".join(lines)

# -------------------------------------------------
# NOTION EXPORT
# -------------------------------------------------

def push_to_notion(item):
    if not NOTION_TOKEN or not NOTION_DATABASE_ID:
        raise RuntimeError(
            "Missing NOTION_TOKEN or NOTION_DATABASE_ID. "
            "Use --dry-run to preview matches without Notion credentials."
        )

    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    # Base properties (always safe)
    properties = {
        "Title": {
            "title": [{"text": {"content": item["title"]}}]
        },
        "Matter ID": {
            "number": item["matter_id"]
        },
        "File Number": {
            "rich_text": [{"text": {"content": item["file_number"]}}]
        },
        "Priority": {
            "select": {"name": item["priority"]}
        },
        "Department": {
            "rich_text": [{"text": {"content": item["department"]}}]
        },
        "In Control": {
            "rich_text": [{"text": {"content": item["in_control"]}}]
        },
        "Action": {
            "rich_text": [{"text": {"content": item["action"]}}]
        },
        "Primary Sponsor": {
            "rich_text": [{"text": {"content": item["primary_sponsor"]}}]
        },
        "Secondary Sponsors": {
            "multi_select": [{"name": s} for s in item["secondary_sponsors"]]
        },
        "Committees": {
            "multi_select": [
                {"name": c.strip()}
                for c in item["committees"]
                if c and c.strip()
            ]
        },
        "Keyword Groups": {
            "multi_select": [{"name": k} for k in item["keyword_groups"]]
        },
        "Status": {
            "select": {"name": item["status"]}
        },
        "Legistar URL": {
            "url": item["url"]
        },
        "Date Checked": {
            "date": {"start": item["date_checked"]}
        }
    }

    # Guarded fields (avoid Notion 400s)
    if item["type"]:
        properties["Type of Legislation"] = {
            "select": {"name": item["type"]}
        }

    if item["action_date"]:
        properties["Action Date"] = {
            "date": {"start": item["action_date"]}
        }

    if item["introduced_date"]:
        properties["Introduced Date"] = {
            "date": {"start": item["introduced_date"]}
        }

    if item["final_action_date"]:
        properties["Final Action"] = {
            "date": {"start": item["final_action_date"]}
        }

    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": properties
    }

    try:
        r = SESSION.post(
            NOTION_API,
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
    except RequestException as exc:
        raise RuntimeError(f"Failed to push to Notion: {exc}") from exc

    if not r.ok:
        raise RuntimeError(f"Notion API error {r.status_code}: {r.text}")




# -------------------------------------------------
# MAIN
# -------------------------------------------------

def build_radar_items(limit=None, include_details=True, lookback_years=DEFAULT_LOOKBACK_YEARS):
    matters = fetch_matters()
    items = []
    seen_matter_ids = set()
    cutoff_date = subtract_years(datetime.utcnow().date(), lookback_years)

    for m in matters:
        if limit and len(items) >= limit:
            break

        matter_id = m.get("MatterId")
        if matter_id in seen_matter_ids:
            continue
        seen_matter_ids.add(matter_id)

        text = " ".join(filter(None, [
            m.get("MatterName", ""),
            m.get("MatterTitle", ""),
            m.get("MatterText", ""),
            m.get("MatterText1", ""),
            m.get("MatterText2", ""),
            m.get("MatterText3", ""),
            m.get("MatterText4", ""),
            m.get("MatterText5", ""),
            m.get("MatterNotes", ""),
            m.get("MatterRequester", ""),
            m.get("MatterBodyName", ""),
            m.get("MatterTypeName", ""),
            m.get("MatterStatusName", ""),
            m.get("MatterFile", "")
        ]))

        keyword_hits = match_keywords(text)
        department_hits = match_department_triggers(text)

        if not is_policy_relevant(keyword_hits, department_hits):
            continue

        committees = []
        primary_sponsor = None
        secondary_sponsors = []

        if include_details:
            history = fetch_history(m["MatterId"])
            committees = list({
                h.get("MatterHistoryActionName", "")
                for h in history if h.get("MatterHistoryActionName")
            })

        priority = assign_priority(keyword_hits, department_hits)
        
        status = m.get("MatterStatusName", "Pending")
        if status not in KNOWN_STATUSES:
            status = "Pending"

        if include_details:
            primary_sponsor, secondary_sponsors = fetch_sponsors(m["MatterId"])

        item = {
            "matter_id": m["MatterId"],
            "file_number": m.get("MatterFile", ""),
            "title": m.get("MatterTitle") or m.get("MatterName", "Untitled"),
            "type": m.get("MatterTypeName", ""),
            "priority": priority,
            "department": m.get("Department", ""),
            "in_control": m.get("MatterInControlName", ""),
            "action": m.get("MatterLastActionName")
            or m.get("MatterFinalActionName", ""),
            "action_date": parse_legistar_date(
                m.get("MatterLastActionDate")
                or m.get("MatterFinalActionDate", "")
            ),
             "introduced_date": parse_legistar_date(
                m.get("MatterIntroDate", "")
            ),
            "final_action_date": parse_legistar_date(
                m.get("MatterPassedDate", "")
            ),
            "last_modified_date": parse_legistar_date(
                m.get("MatterLastModifiedUtc", "")
            ),
            "primary_sponsor": primary_sponsor or "",
            "secondary_sponsors": secondary_sponsors,
            "committees": committees,
            "keyword_groups": list(keyword_hits.keys()),
            "department_triggers": department_hits,
            "status": status,
            "url": f"https://sfgov.legistar.com/LegislationDetail.aspx?ID={m['MatterId']}",
            "date_checked": datetime.utcnow().date().isoformat()
        }

        eligible_dates = [
            datetime.fromisoformat(item[field]).date()
            for field in (
                "action_date",
                "final_action_date",
                "introduced_date",
                "last_modified_date",
            )
            if item[field]
        ]

        if not eligible_dates or max(eligible_dates) < cutoff_date:
            continue

        items.append(item)

    return items

def run_monitor(
    dry_run=False,
    output_json=False,
    limit=None,
    include_details=True,
    lookback_years=DEFAULT_LOOKBACK_YEARS,
):
    items = build_radar_items(
        limit=limit,
        include_details=include_details,
        lookback_years=lookback_years,
    )

    if dry_run:
        if output_json:
            print(json.dumps(items, indent=2))
        else:
            print(format_markdown_report(items))
        print(f"\nPreview complete: {len(items)} matching item(s).")
        return

    for item in items:
        push_to_notion(item)

    print(f"Legistar monitor run complete. Exported {len(items)} item(s) to Notion.")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Monitor San Francisco Legistar for arts and cultural policy signals."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview matching items without exporting to Notion.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print dry-run results as JSON.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after collecting this many matching items.",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=DEFAULT_LOOKBACK_YEARS,
        help="Include items introduced, acted on, passed, or modified within this many years.",
    )
    parser.add_argument(
        "--include-details",
        action="store_true",
        help="Fetch slower history and sponsor detail during dry runs.",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run_monitor(
        dry_run=args.dry_run,
        output_json=args.json,
        limit=args.limit,
        include_details=(not args.dry_run) or args.include_details,
        lookback_years=args.years,
    )
