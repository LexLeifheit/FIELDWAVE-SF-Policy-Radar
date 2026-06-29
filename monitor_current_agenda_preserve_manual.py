import monitor_current_agenda as radar


EDITORIAL_FIELD_KEYS = {
    "primary_category",
    "subcategories",
    "impact_level",
    "policy_signal",
    "urgency",
    "why_it_matters",
}


EXTRA_FIELD_SPECS = [
    ("why_it_matters", ["Why It Matters", "Policy Rationale"], lambda item: item["why_it_matters"]),
    ("needs_review", ["Needs Review", "Review Needed"], lambda item: item["needs_review"]),
    ("machine_primary_category", ["Last Machine Primary Category", "Machine Primary Category"], lambda item: item["machine_primary_category"]),
    ("machine_subcategories", ["Last Machine Subcategories", "Machine Subcategories"], lambda item: item["machine_subcategories"]),
    ("machine_impact_level", ["Last Machine Impact Level", "Machine Impact Level"], lambda item: item["machine_impact_level"]),
    ("machine_policy_signal", ["Last Machine Policy Signal", "Machine Policy Signal"], lambda item: item["machine_policy_signal"]),
    ("machine_urgency", ["Last Machine Urgency", "Machine Urgency"], lambda item: item["machine_urgency"]),
]


def install_field_specs():
    existing_keys = {key for key, _aliases, _getter in radar.FIELD_SPECS}
    for spec in EXTRA_FIELD_SPECS:
        if spec[0] not in existing_keys:
            radar.FIELD_SPECS.append(spec)


def build_why_it_matters(item):
    category = item.get("primary_category") or "cultural policy"
    subcategories = radar.compact_list(item.get("subcategories"))
    focus = subcategories[0] if subcategories else category
    action = radar.compact_text(item.get("action")) or "showing current legislative activity"
    impact = radar.compact_text(item.get("impact_level")).lower() or "watch"
    return f"{focus}: {action}. Current monitor read: {impact} impact."


original_classify_policy_taxonomy = radar.classify_policy_taxonomy


def classify_policy_taxonomy(item):
    original_classify_policy_taxonomy(item)
    if item["type"] == "Communication":
        item["impact_level"] = "Low"

    item["why_it_matters"] = build_why_it_matters(item)
    item["machine_primary_category"] = item["primary_category"]
    item["machine_subcategories"] = item["subcategories"]
    item["machine_impact_level"] = item["impact_level"]
    item["machine_policy_signal"] = item["policy_signal"]
    item["machine_urgency"] = item["urgency"]
    item["needs_review"] = False
    return item


original_default_property_type = radar.default_property_type


def default_property_type(key):
    if key in {"machine_subcategories", "machine_policy_signal"}:
        return "multi_select"
    if key in {
        "machine_primary_category",
        "machine_impact_level",
        "machine_urgency",
    }:
        return "select"
    if key == "needs_review":
        return "checkbox"
    return original_default_property_type(key)


def build_notion_properties(item, schema=None, exclude_keys=None):
    properties = {}
    exclude_keys = exclude_keys or set()
    if schema:
        for key, aliases, getter in radar.FIELD_SPECS:
            if key in exclude_keys:
                continue
            name = radar.find_property(schema, aliases, preferred_type="title" if key == "title" else None)
            if not name:
                continue
            value = radar.notion_property_value(schema[name].get("type"), getter(item))
            if value is not None:
                properties[name] = value
        if not any("title" in value for value in properties.values()):
            for name, config in schema.items():
                if config.get("type") == "title":
                    properties[name] = radar.notion_property_value("title", item["title"])
                    break
    else:
        for key, aliases, getter in radar.FIELD_SPECS:
            if key in exclude_keys:
                continue
            value = radar.notion_property_value(default_property_type(key), getter(item))
            if value is not None:
                properties[aliases[0]] = value

    if not any("title" in value for value in properties.values()):
        raise RuntimeError("Notion target needs a title property, preferably named Title or Name.")
    return properties


def patch_notion_page(page_id, item, headers, schema=None):
    payload = {"properties": build_notion_properties(item, schema, exclude_keys=EDITORIAL_FIELD_KEYS)}
    try:
        return radar.SESSION.patch(
            f"{radar.NOTION_PAGES_API}/{page_id}",
            headers=headers,
            json=payload,
            timeout=radar.REQUEST_TIMEOUT,
        )
    except radar.RequestException as exc:
        raise RuntimeError(f"Failed to update Notion page {page_id}: {exc}") from exc


def extract_notion_property_value(property_value):
    property_type = property_value.get("type")
    value = property_value.get(property_type)
    if property_type in {"title", "rich_text"}:
        return radar.compact_text(
            "".join(part.get("plain_text", "") for part in value or [])
        )
    if property_type in {"select", "status"}:
        return (value or {}).get("name", "")
    if property_type == "multi_select":
        return [option.get("name", "") for option in value or [] if option.get("name")]
    if property_type == "checkbox":
        return bool(value)
    if property_type == "date":
        return (value or {}).get("start", "")
    if property_type == "number":
        return value
    if property_type == "url":
        return value or ""
    return ""


def existing_page_value(page, schema, key):
    field = next((field for field in radar.FIELD_SPECS if field[0] == key), None)
    if not field:
        return ""
    name = radar.find_property(schema or {}, field[1])
    if not name:
        return ""
    property_value = page.get("properties", {}).get(name, {})
    return extract_notion_property_value(property_value)


def values_differ(left, right):
    if isinstance(left, list) or isinstance(right, list):
        return sorted(radar.compact_list(left)) != sorted(radar.compact_list(right))
    return radar.compact_text(left) != radar.compact_text(right)


def update_review_flag_from_existing_page(item, existing_page, schema):
    for key in EDITORIAL_FIELD_KEYS:
        existing_value = existing_page_value(existing_page, schema, key)
        if not existing_value:
            continue
        if values_differ(existing_value, item.get(key)):
            item["needs_review"] = True
            return
    item["needs_review"] = False


def upsert_notion_page(parent, item, headers, schema=None):
    matches = radar.sort_existing_pages(
        radar.query_notion_parent(parent, headers, schema, item["file_number"]),
        schema,
    )
    if matches:
        page_id = matches[0]["id"]
        update_review_flag_from_existing_page(item, matches[0], schema)
        response = patch_notion_page(page_id, item, headers, schema)
        if response.ok:
            for duplicate in matches[1:]:
                radar.archive_notion_page(duplicate["id"], headers)
        return response
    return radar.post_to_notion(parent, item, headers, schema)


def install_overrides():
    install_field_specs()
    radar.classify_policy_taxonomy = classify_policy_taxonomy
    radar.default_property_type = default_property_type
    radar.build_notion_properties = build_notion_properties
    radar.patch_notion_page = patch_notion_page
    radar.upsert_notion_page = upsert_notion_page


if __name__ == "__main__":
    install_overrides()
    args = radar.parse_args()
    radar.run_monitor(
        dry_run=args.dry_run,
        output_json=args.json,
        limit=args.limit,
        agenda_days=args.agenda_days,
    )
