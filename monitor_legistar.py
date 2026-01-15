import os
import requests
from datetime import datetime

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

LEGISTAR_BASE = "https://webapi.legistar.com/v1/sfgov"

NOTION_API = "https://api.notion.com/v1/pages"
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")

if not NOTION_TOKEN or not NOTION_DATABASE_ID:
    raise RuntimeError("Missing NOTION_TOKEN or NOTION_DATABASE_ID")

# -------------------------------------------------
# KEYWORDS
# -------------------------------------------------

KEYWORD_GROUPS = {
    "arts_culture": [
        "art", "artwork", "artworks", "arts", "public art",
        "mural", "visual art", "culture", "cultural",
        "monument", "sculpture", "painting"
    ],
    "artists_practice": [
        "artist", "artists", "dance", "literary", "music", "film"
    ],
    "funding_tax": [
        "hotel tax",
        "transient occupancy tax",
        "tot",
        "budget and appropriation",
        "appropriation ordinance",
        "annual salary ordinance"
    ]
}

SECONDARY_TRIGGER_DEPARTMENTS = [
    "arts commission",
    "public art program",
    "asian art museum",
    "department of children, youth and their families",
    "economic and workforce development",
    "office of economic and workforce development",
    "fine arts museum",
    "fine arts museums",
    "grants for the arts",
    "museum of the african diaspora",
    "yerba buena center for the arts"
]

ESCALATION_COMMITTEES = [
    "budget and finance",
    "budget and appropriations",
    "appropriations",
    "government audit and oversight",
    "rules committee"
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
    r = requests.get(f"{LEGISTAR_BASE}/matters")
    r.raise_for_status()
    return r.json()

def fetch_history(matter_id):
    r = requests.get(f"{LEGISTAR_BASE}/matters/{matter_id}/history")
    if r.status_code != 200:
        return []
    return r.json()

def fetch_sponsors(matter_id):
    """
    Returns (primary_sponsor, secondary_sponsors[])
    """
    r = requests.get(f"{LEGISTAR_BASE}/matters/{matter_id}/sponsors")
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

def match_keywords(text):
    text = text.lower()
    hits = {}
    for group, words in KEYWORD_GROUPS.items():
        matches = [w for w in words if w in text]
        if matches:
            hits[group] = matches
    return hits

def department_trigger(dept):
    if not dept:
        return False
    d = dept.lower()
    return any(x in d for x in SECONDARY_TRIGGER_DEPARTMENTS)

def committee_escalation(committees):
    for c in committees:
        c = c.lower()
        if any(e in c for e in ESCALATION_COMMITTEES):
            return True
    return False

def assign_priority(keyword_hits, dept_hit, committee_hit):
    funding_hit = "funding_tax" in keyword_hits
    if funding_hit and (dept_hit or committee_hit):
        return "HIGH"
    if funding_hit or dept_hit or committee_hit:
        return "MEDIUM"
    return "LOW"

# -------------------------------------------------
# NOTION EXPORT
# -------------------------------------------------

def push_to_notion(item):
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

    if item["final_action_date"]:
        properties["Final Action"] = {
            "date": {"start": item["final_action_date"]}
        }

    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": properties
    }

    r = requests.post(
        NOTION_API,
        headers=headers,
        json=payload
    )

    if not r.ok:
        raise RuntimeError(f"Notion API error {r.status_code}: {r.text}")




# -------------------------------------------------
# MAIN
# -------------------------------------------------

def run_monitor():
    matters = fetch_matters()
    seen_matter_ids = set()

    for m in matters:
        matter_id = m.get("MatterId")
        if matter_id in seen_matter_ids:
            continue
        seen_matter_ids.add(matter_id)

        text = " ".join(filter(None, [
            m.get("MatterName", ""),
            m.get("MatterTitle", ""),
            m.get("MatterText", "")
        ]))

        keyword_hits = match_keywords(text)
        dept_hit = department_trigger(m.get("Department", ""))

        history = fetch_history(m["MatterId"])
        committees = list({
            h.get("MatterHistoryActionName", "")
            for h in history if h.get("MatterHistoryActionName")
        })

        committee_hit = committee_escalation(committees)

        if not keyword_hits and not dept_hit:
            continue

        priority = assign_priority(keyword_hits, dept_hit, committee_hit)

        status = m.get("MatterStatusName", "Pending")
        if status not in KNOWN_STATUSES:
            status = "Pending"

        primary_sponsor, secondary_sponsors = fetch_sponsors(m["MatterId"])

        item = {
            "matter_id": m["MatterId"],
            "file_number": m.get("MatterFile", ""),
            "title": m.get("MatterName", "Untitled"),
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
            "final_action_date": parse_legistar_date(
                m.get("MatterPassedDate", "")
            ),
            "primary_sponsor": primary_sponsor or "",
            "secondary_sponsors": secondary_sponsors,
            "committees": committees,
            "keyword_groups": list(keyword_hits.keys()),
            "status": status,
            "url": f"https://sfgov.legistar.com/LegislationDetail.aspx?ID={m['MatterId']}",
            "date_checked": datetime.utcnow().date().isoformat()
        }

        push_to_notion(item)


    print("✅ Legistar monitor run complete.")

if __name__ == "__main__":
    run_monitor()
