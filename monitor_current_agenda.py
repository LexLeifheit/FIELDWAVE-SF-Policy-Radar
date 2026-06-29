import argparse
from datetime import datetime, timedelta
from html import unescape
import json
import os
import re
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry


LEGISTAR_SITE = "https://sfgov.legistar.com"
NOTION_PAGES_API = "https://api.notion.com/v1/pages"
NOTION_DATA_SOURCES_API = "https://api.notion.com/v1/data_sources"
NOTION_DATABASES_API = "https://api.notion.com/v1/databases"
NOTION_VERSION = "2025-09-03"
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")
NOTION_DATA_SOURCE_ID = os.environ.get("NOTION_DATA_SOURCE_ID")
REQUEST_TIMEOUT = (10, 30)
DEFAULT_AGENDA_LOOKBACK_DAYS = 14


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
    ],
}

DEPARTMENT_TRIGGERS = [
    "arts commission",
    "fine arts museums",
    "grants for the arts",
    "office of economic and workforce development",
    "oeewd",
]


FIELD_SPECS = [
    ("title", ["Title", "Name", "Matter", "Legislation"], lambda item: item["title"]),
    ("matter_id", ["Matter ID", "MatterId", "Legistar ID"], lambda item: item["matter_id"]),
    ("file_number", ["File Number", "File", "File No.", "File No"], lambda item: item["file_number"]),
    ("priority", ["Priority"], lambda item: item["priority"]),
    ("department", ["Department", "Department Trigger"], lambda item: item["department"]),
    ("in_control", ["In Control", "Committee", "Current Committee"], lambda item: item["in_control"]),
    ("action", ["Action", "Latest Action", "Recent Action"], lambda item: item["action"]),
    ("primary_sponsor", ["Primary Sponsor", "Sponsor"], lambda item: item["primary_sponsor"]),
    ("secondary_sponsors", ["Secondary Sponsors", "Co-Sponsors", "Cosponsors"], lambda item: item["secondary_sponsors"]),
    ("committees", ["Committees", "Committee History", "Bodies"], lambda item: item["committees"]),
    ("keyword_groups", ["Keyword Groups", "Keywords", "Policy Signals", "Signals"], lambda item: item["keyword_groups"]),
    ("status", ["Status"], lambda item: item["status"]),
    ("url", ["Legistar URL", "URL", "Link", "Source"], lambda item: item["url"]),
    ("date_checked", ["Date Checked", "Checked", "Last Checked"], lambda item: item["date_checked"]),
    ("type", ["Type of Legislation", "Type", "Legislation Type"], lambda item: item["type"]),
    ("action_date", ["Action Date", "Latest Action Date", "Meeting Date"], lambda item: item["action_date"]),
    ("introduced_date", ["Introduced Date", "Introduction Date"], lambda item: item["introduced_date"]),
    ("final_action_date", ["Final Action", "Final Action Date"], lambda item: item["final_action_date"]),
    ("passed", ["Passed", "Passed?"], lambda item: item["passed"]),
    ("failed", ["Failed", "Failed?"], lambda item: item["failed"]),
    ("passed_date", ["Passed Date"], lambda item: item["passed_date"]),
    ("failed_date", ["Failed Date"], lambda item: item["failed_date"]),
]


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
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = build_session()


def fetch_page(path_or_url):
    url = urljoin(f"{LEGISTAR_SITE}/", path_or_url)
    try:
        response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.text
    except RequestException as exc:
        raise RuntimeError(f"Failed to fetch {url}: {exc}") from exc


def parse_display_date(value):
    value = " ".join(value.split())
    for fmt in ("%m/%d/%Y", "%m/%d/%Y %I:%M %p"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            pass
    return ""


def clean_label(value):
    return " ".join(value.replace("\xa0", " ").strip().rstrip(":").lower().split())


def clean_value(value):
    return " ".join(value.replace("\xa0", " ").split())


def normalize_action(value):
    return clean_value(value).upper()


def parse_detail_pairs(soup):
    details = {}
    for row in soup.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
        if len(cells) < 2:
            continue
        for index in range(0, len(cells) - 1, 2):
            label = clean_label(cells[index])
            value = clean_value(cells[index + 1])
            if label and value and len(label) <= 40:
                details[label] = value
    return details


def parse_history_rows(soup):
    rows = []
    for row in soup.find_all("tr"):
        cells = [clean_value(cell.get_text(" ", strip=True)) for cell in row.find_all(["th", "td"])]
        if len(cells) < 5 or cells[0] == "Date":
            continue
        if not parse_display_date(cells[0]):
            continue
        action = cells[3] if len(cells) > 3 else ""
        result = cells[4] if len(cells) > 4 else ""
        if action:
            rows.append({
                "date": parse_display_date(cells[0]),
                "action_by": cells[2] if len(cells) > 2 else "",
                "action": action,
                "result": result,
            })
    return rows


def enrich_from_legislation_detail(item):
    soup = BeautifulSoup(fetch_page(item["url"]), "html.parser")
    details = parse_detail_pairs(soup)
    history = parse_history_rows(soup)

    item["file_number"] = re.sub(r"\s+Version:.*$", "", details.get("file #", item["file_number"])).strip()
    item["title"] = details.get("title", item["title"])
    item["type"] = details.get("type", item["type"])
    item["status"] = details.get("status", item["status"])
    item["introduced_date"] = parse_display_date(details.get("introduced", "")) or item["introduced_date"]
    item["final_action_date"] = parse_display_date(details.get("final action", "")) or item["final_action_date"]
    item["in_control"] = details.get("in control", item["in_control"])

    if history:
        latest = max(history, key=lambda row: row["date"])
        item["action_date"] = latest["date"]
        item["action"] = latest["action"]
        if latest["action_by"]:
            item["in_control"] = latest["action_by"]
        item["committees"] = list(dict.fromkeys(
            compact_list(item["committees"]) + [
                row["action_by"] for row in history if row["action_by"]
            ]
        ))

    action_text = " ".join(
        [item["status"], item["action"]]
        + [row["action"] for row in history]
        + [row["result"] for row in history]
    )
    normalized = normalize_action(action_text)
    item["passed"] = any(word in normalized for word in ("PASSED", "ADOPTED", "APPROVED", "FINALLY PASSED"))
    item["failed"] = any(word in normalized for word in ("FAILED", "DISAPPROVED", "VETOED", "KILLED", "WITHDRAWN"))

    for row in history:
        row_text = normalize_action(f"{row['action']} {row['result']}")
        if not item["passed_date"] and any(word in row_text for word in ("PASSED", "ADOPTED", "APPROVED", "FINALLY PASSED")):
            item["passed_date"] = row["date"]
        if not item["failed_date"] and any(word in row_text for word in ("FAILED", "DISAPPROVED", "VETOED", "KILLED", "WITHDRAWN")):
            item["failed_date"] = row["date"]

    if item["final_action_date"] and item["passed"] and not item["passed_date"]:
        item["passed_date"] = item["final_action_date"]
    if item["final_action_date"] and item["failed"] and not item["failed_date"]:
        item["failed_date"] = item["final_action_date"]

    return item


def parse_legislation_id(url):
    raw_id = parse_qs(urlparse(url).query).get("ID", [""])[0]
    return int(raw_id) if raw_id.isdigit() else 0


def match_keywords(text):
    text = text.lower()
    hits = {}
    for group, words in KEYWORD_GROUPS.items():
        matches = [word for word in words if re.search(rf"\b{re.escape(word)}\b", text)]
        if matches:
            hits[group] = matches
    return hits


def match_department_triggers(text):
    text = text.lower()
    return [
        trigger
        for trigger in DEPARTMENT_TRIGGERS
        if re.search(rf"\b{re.escape(trigger)}\b", text)
    ]


def is_policy_relevant(keyword_hits, department_hits):
    if department_hits:
        return True
    if set(keyword_hits) == {"funding_budget"}:
        return False
    return bool(keyword_hits)


def assign_priority(keyword_hits, department_hits):
    if "funding_budget" in keyword_hits and (department_hits or len(keyword_hits) > 1):
        return "HIGH"
    if department_hits and keyword_hits:
        return "HIGH"
    if len(keyword_hits) > 1:
        return "HIGH"
    if keyword_hits or department_hits:
        return "MEDIUM"
    return "LOW"


def truncate_text(value, max_length=180):
    value = " ".join(value.split())
    return value if len(value) <= max_length else f"{value[:max_length - 3]}..."


def fetch_recent_agenda_items(days=DEFAULT_AGENDA_LOOKBACK_DAYS, limit=None):
    html = fetch_page("Calendar.aspx")
    soup = BeautifulSoup(html, "html.parser")
    cutoff_date = (datetime.utcnow().date() - timedelta(days=days)).isoformat()
    meetings = []

    for table in soup.find_all("table"):
        if "gridCalendar" not in (table.get("id") or ""):
            continue
        for row in table.find_all("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
            if len(cells) < 7:
                continue
            meeting_date = parse_display_date(cells[1])
            if not meeting_date or meeting_date < cutoff_date:
                continue

            details_link = None
            for link in row.find_all("a", href=True):
                href = unescape(link["href"])
                label = link.get_text(" ", strip=True).lower()
                if "MeetingDetail.aspx" in href or "meeting" in label:
                    details_link = href
                    break

            if details_link:
                meetings.append({
                    "name": cells[0],
                    "date": meeting_date,
                    "details_url": urljoin(f"{LEGISTAR_SITE}/", details_link),
                })

    items = []
    seen_files = set()
    for meeting in meetings:
        detail_soup = BeautifulSoup(fetch_page(meeting["details_url"]), "html.parser")
        for table in detail_soup.find_all("table"):
            if "gridMain" not in (table.get("id") or ""):
                continue
            for row in table.find_all("tr"):
                cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
                if len(cells) < 7:
                    continue
                file_number = cells[0]
                if not file_number or file_number in seen_files:
                    continue
                legislation_link = row.find("a", href=re.compile("LegislationDetail.aspx"))
                if not legislation_link:
                    continue

                detail_url = urljoin(f"{LEGISTAR_SITE}/", unescape(legislation_link["href"]))
                text = " ".join([cells[3], cells[4], cells[5], cells[6], meeting["name"]])
                keyword_hits = match_keywords(text)
                department_hits = match_department_triggers(text)
                if not is_policy_relevant(keyword_hits, department_hits):
                    continue

                seen_files.add(file_number)
                items.append({
                    "matter_id": parse_legislation_id(detail_url),
                    "file_number": file_number,
                    "title": cells[6],
                    "type": cells[4],
                    "priority": assign_priority(keyword_hits, department_hits),
                    "department": ", ".join(department_hits),
                    "in_control": meeting["name"],
                    "action": f"On {meeting['name']} agenda",
                    "action_date": meeting["date"],
                    "introduced_date": "",
                    "final_action_date": "",
                    "passed": False,
                    "failed": False,
                    "passed_date": "",
                    "failed_date": "",
                    "primary_sponsor": "",
                    "secondary_sponsors": [],
                    "committees": [meeting["name"]],
                    "keyword_groups": list(keyword_hits.keys()),
                    "status": cells[5] or "Pending",
                    "url": detail_url,
                    "date_checked": datetime.utcnow().date().isoformat(),
                })
                enrich_from_legislation_detail(items[-1])
                if limit and len(items) >= limit:
                    return items

    return items


def compact_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    return [str(value).strip()] if str(value).strip() else []


def compact_text(value):
    if isinstance(value, list):
        value = ", ".join(compact_list(value))
    return " ".join(str(value or "").split())


def parse_number(value):
    if isinstance(value, (int, float)):
        return value
    text = compact_text(value)
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }


def notion_property_value(property_type, value):
    text = compact_text(value)
    if property_type == "title":
        return {"title": [{"text": {"content": text[:2000]}}]}
    if property_type == "rich_text":
        return {"rich_text": [{"text": {"content": text[:2000]}}]} if text else {"rich_text": []}
    if property_type == "number":
        number = parse_number(value)
        return {"number": number} if number is not None else None
    if property_type == "select":
        return {"select": {"name": text[:100]}} if text else None
    if property_type == "multi_select":
        return {"multi_select": [{"name": name[:100]} for name in compact_list(value)]}
    if property_type == "status":
        return {"status": {"name": text[:100]}} if text else None
    if property_type == "url":
        return {"url": text} if text else None
    if property_type == "date":
        return {"date": {"start": text}} if text else None
    if property_type == "checkbox":
        return {"checkbox": bool(value)}
    return None


def fetch_notion_schema(parent_id, headers):
    for endpoint in (
        f"{NOTION_DATA_SOURCES_API}/{parent_id}",
        f"{NOTION_DATABASES_API}/{parent_id}",
    ):
        try:
            response = SESSION.get(endpoint, headers=headers, timeout=REQUEST_TIMEOUT)
        except RequestException:
            continue
        if response.ok:
            return response.json().get("properties", {})
    return None


def discover_child_data_source_ids(response):
    try:
        error_body = response.json()
    except ValueError:
        return []
    return error_body.get("additional_data", {}).get("child_data_source_ids", [])


def find_property(schema, aliases, preferred_type=None):
    normalized = {name.strip().lower(): name for name in schema}
    for alias in aliases:
        name = normalized.get(alias.strip().lower())
        if name and (not preferred_type or schema[name].get("type") == preferred_type):
            return name
    return None


def default_property_type(key):
    if key == "title":
        return "title"
    if key == "matter_id":
        return "number"
    if key in {"priority", "status", "type"}:
        return "select"
    if key in {"secondary_sponsors", "committees", "keyword_groups"}:
        return "multi_select"
    if key == "url":
        return "url"
    if key == "date_checked" or key.endswith("_date"):
        return "date"
    if key in {"passed", "failed"}:
        return "checkbox"
    return "rich_text"


def build_notion_properties(item, schema=None):
    properties = {}
    if schema:
        for key, aliases, getter in FIELD_SPECS:
            name = find_property(schema, aliases, preferred_type="title" if key == "title" else None)
            if not name:
                continue
            value = notion_property_value(schema[name].get("type"), getter(item))
            if value is not None:
                properties[name] = value
        if not any("title" in value for value in properties.values()):
            for name, config in schema.items():
                if config.get("type") == "title":
                    properties[name] = notion_property_value("title", item["title"])
                    break
    else:
        for key, aliases, getter in FIELD_SPECS:
            value = notion_property_value(default_property_type(key), getter(item))
            if value is not None:
                properties[aliases[0]] = value

    if not any("title" in value for value in properties.values()):
        raise RuntimeError("Notion target needs a title property, preferably named Title or Name.")
    return properties


def post_to_notion(parent, item, headers, schema=None):
    payload = {"parent": parent, "properties": build_notion_properties(item, schema)}
    try:
        return SESSION.post(NOTION_PAGES_API, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
    except RequestException as exc:
        raise RuntimeError(f"Failed to push to Notion: {exc}") from exc


def patch_notion_page(page_id, item, headers, schema=None):
    payload = {"properties": build_notion_properties(item, schema)}
    try:
        return SESSION.patch(
            f"{NOTION_PAGES_API}/{page_id}",
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
    except RequestException as exc:
        raise RuntimeError(f"Failed to update Notion page {page_id}: {exc}") from exc


def archive_notion_page(page_id, headers):
    try:
        response = SESSION.patch(
            f"{NOTION_PAGES_API}/{page_id}",
            headers=headers,
            json={"archived": True},
            timeout=REQUEST_TIMEOUT,
        )
    except RequestException as exc:
        print(f"Warning: failed to archive duplicate Notion page {page_id}: {exc}")
        return False
    if not response.ok:
        print(f"Warning: failed to archive duplicate Notion page {page_id}: {response.text}")
    return response.ok


def filter_for_property(property_name, property_type, value):
    text = compact_text(value)
    if not text:
        return None
    if property_type == "title":
        return {"property": property_name, "title": {"equals": text}}
    if property_type == "rich_text":
        return {"property": property_name, "rich_text": {"equals": text}}
    if property_type == "number":
        number = parse_number(text)
        return {"property": property_name, "number": {"equals": number}} if number is not None else None
    if property_type == "select":
        return {"property": property_name, "select": {"equals": text}}
    return None


def query_notion_parent(parent, headers, schema, file_number):
    file_property = find_property(
        schema or {},
        ["File Number", "File", "File No.", "File No"],
    )
    if not file_property:
        return []

    query_filter = filter_for_property(
        file_property,
        schema[file_property].get("type"),
        file_number,
    )
    if not query_filter:
        return []

    if "data_source_id" in parent:
        endpoint = f"{NOTION_DATA_SOURCES_API}/{parent['data_source_id']}/query"
    else:
        endpoint = f"{NOTION_DATABASES_API}/{parent['database_id']}/query"

    try:
        response = SESSION.post(
            endpoint,
            headers=headers,
            json={"filter": query_filter, "page_size": 100},
            timeout=REQUEST_TIMEOUT,
        )
    except RequestException as exc:
        print(f"Warning: failed to query Notion for file {file_number}: {exc}")
        return []

    if not response.ok:
        return []
    return response.json().get("results", [])


def sort_existing_pages(pages, schema):
    date_property = find_property(schema or {}, ["Date Checked", "Checked", "Last Checked"], preferred_type="date")

    def checked_date(page):
        if not date_property:
            return ""
        date_value = page.get("properties", {}).get(date_property, {}).get("date") or {}
        return date_value.get("start") or ""

    return sorted(pages, key=checked_date, reverse=True)


def upsert_notion_page(parent, item, headers, schema=None):
    matches = sort_existing_pages(
        query_notion_parent(parent, headers, schema, item["file_number"]),
        schema,
    )
    if matches:
        page_id = matches[0]["id"]
        response = patch_notion_page(page_id, item, headers, schema)
        if response.ok:
            for duplicate in matches[1:]:
                archive_notion_page(duplicate["id"], headers)
        return response
    return post_to_notion(parent, item, headers, schema)


def push_to_notion(item):
    if not NOTION_TOKEN or not (NOTION_DATABASE_ID or NOTION_DATA_SOURCE_ID):
        raise RuntimeError("Missing Notion credentials. Use --dry-run to preview without exporting.")

    headers = notion_headers()
    if NOTION_DATA_SOURCE_ID:
        schema = fetch_notion_schema(NOTION_DATA_SOURCE_ID, headers)
        response = upsert_notion_page({"data_source_id": NOTION_DATA_SOURCE_ID}, item, headers, schema)
    else:
        schema = fetch_notion_schema(NOTION_DATABASE_ID, headers)
        response = upsert_notion_page({"database_id": NOTION_DATABASE_ID}, item, headers, schema)

    if response.ok:
        return

    child_ids = discover_child_data_source_ids(response)
    for data_source_id in child_ids:
        schema = fetch_notion_schema(data_source_id, headers)
        response = upsert_notion_page({"data_source_id": data_source_id}, item, headers, schema)
        if response.ok:
            return

    raise RuntimeError(f"Notion API error {response.status_code}: {response.text}")


def format_report(items):
    if not items:
        return "No matching cultural policy items found in current public agendas."
    lines = [
        "# FIELDWAVE SF Policy Radar Preview",
        "",
        f"Generated: {datetime.utcnow().date().isoformat()} UTC",
        "",
        "| Priority | File | Title | Status | Signals |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in items:
        signals = item["keyword_groups"] + compact_list(item["department"])
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


def run_monitor(dry_run=False, output_json=False, limit=None, agenda_days=DEFAULT_AGENDA_LOOKBACK_DAYS):
    items = fetch_recent_agenda_items(days=agenda_days, limit=limit)
    if dry_run:
        print(json.dumps(items, indent=2) if output_json else format_report(items))
        print(f"\nPreview complete: {len(items)} matching item(s).")
        return

    for item in items:
        push_to_notion(item)
    print(f"Legistar monitor run complete. Exported {len(items)} item(s) to Notion.")


def parse_args():
    parser = argparse.ArgumentParser(description="Monitor current SF agendas for cultural policy signals.")
    parser.add_argument("--dry-run", action="store_true", help="Preview matches without exporting to Notion.")
    parser.add_argument("--json", action="store_true", help="Print dry-run matches as JSON.")
    parser.add_argument("--limit", type=int, default=None, help="Stop after this many matching items.")
    parser.add_argument("--agenda-days", type=int, default=DEFAULT_AGENDA_LOOKBACK_DAYS)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_monitor(
        dry_run=args.dry_run,
        output_json=args.json,
        limit=args.limit,
        agenda_days=args.agenda_days,
    )
