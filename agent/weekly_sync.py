"""
weekly_sync.py
--------------
Single-session weekly sync: fetches hours AND performance from Fresha using
one browser context per account, then pushes everything to GHL.

Steps per account:
  1. Hours  — Fresha API calls (no page navigation) → per-day hours per staff
              → upsert GHL payroll records (creates if missing, updates if exists)
  2. Performance — Fresha CSV download (browser navigation) → parse with Claude
              → update GHL payroll records (tips, commissions, service_sales_exc_gst)
              → upsert GHL location_performance records
              → append JSON history for dashboard

Run with:  python agent/weekly_sync.py
Requires:  data/session.json        (NT Fresha session)
           data/session_cairns.json (QLD Fresha session)
"""

import asyncio
import json
import os
import smtplib
from datetime import datetime, timedelta, timezone
from email import encoders as email_encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import anthropic
import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"

GHL_API_KEY     = os.environ.get("GHL_API_KEY", "")
GHL_LOCATION_ID = os.environ.get("GHL_LOCATION_ID", "")
GHL_BASE        = "https://services.leadconnectorhq.com"
GHL_HEADERS     = {
    "Authorization": f"Bearer {GHL_API_KEY}",
    "Version":       "2021-07-28",
    "Accept":        "application/json",
    "Content-Type":  "application/json",
}

ACCOUNTS = [
    {
        "label":        "NT (Darwin)",
        "session":      DATA_DIR / "session.json",
        "email_env":    "FRESHA_EMAIL",
        "pass_env":     "FRESHA_PASSWORD",
        "timezone":     timezone(timedelta(hours=9, minutes=30)),
        "provider_id":  "1371504",
        "default_org":  "Diamond Barbers Darwin",
        "output":       DATA_DIR / "performance_summary.json",
    },
    {
        "label":        "QLD (Cairns)",
        "session":      DATA_DIR / "session_cairns.json",
        "email_env":    "CAIRNS_FRESHA_EMAIL",
        "pass_env":     "CAIRNS_FRESHA_PASSWORD",
        "timezone":     timezone(timedelta(hours=10)),
        "provider_id":  "1390965",
        "default_org":  "Diamond Barbers Cairns",
        "output":       DATA_DIR / "cairns_performance_summary.json",
    },
]

# Static employee → Xero org mapping.
# This is the source of truth — never derived from which location they work at.
# Default if not listed: NT staff → "Diamond Barbers Darwin", QLD staff → "Diamond Barbers Cairns"
EMPLOYEE_XERO_ORG = {
    "Vincenzo Vanzanella": "Diamond Barbers Parap",
    "Krish Manocha":       "Diamond Barbers Parap",
    "Sean Maguire":        "Diamond Barbers Parap",
}

# Fresha location name → GHL location_performance label
LOCATION_TO_ORG = {
    "Diamond Barbers - COOLALINGA":     "Diamond Barbers Darwin",
    "Diamond Barbers - BELLAMACK":      "Diamond Barbers Darwin",
    "Diamond Barbers - YARRAWONGA":     "Diamond Barbers Darwin",
    "Diamond Barbers - CASUARINA":      "Diamond Barbers Darwin",
    "Diamond Barbers - DARWIN CBD":     "Diamond Barbers Darwin",
    "Diamond Barbers - DELUXE":         "Diamond Barbers Darwin",
    "Diamond Barbers - PARAP":          "Diamond Barbers Parap",
    "Diamond Barbers Rising Sun":       "Diamond Barbers Cairns",
    "Diamond Barbers Showgrounds":      "Diamond Barbers Cairns",
    "Diamond Barbers Northern Beaches": "Diamond Barbers Cairns",
    "Diamond Barbers Night Markets":    "Diamond Barbers Cairns",
    "Diamond Barbers Wulguru":          "Diamond Barbers Cairns",
}

# Manager commission overrides — commission = sum of location product sales * 0.9 * 0.10
MANAGER_LOCATIONS = {
    "Anthony Crispo":      ["Diamond Barbers - COOLALINGA"],
    "Airol Basallo":       ["Diamond Barbers - BELLAMACK"],
    "Wilfred Vidal":       ["Diamond Barbers - YARRAWONGA"],
    "Marianne Escobar":    ["Diamond Barbers - COOLALINGA", "Diamond Barbers - BELLAMACK",
                            "Diamond Barbers - YARRAWONGA", "Diamond Barbers - CASUARINA",
                            "Diamond Barbers - PARAP", "Diamond Barbers - DARWIN CBD"],
    "Avinash Borade":      ["Diamond Barbers - CASUARINA"],
    "Vincenzo Vanzanella": ["Diamond Barbers - PARAP"],
    "Jairo Espinosa":      ["Diamond Barbers - DARWIN CBD"],
    "Jerry Guevarra":      ["Diamond Barbers Showgrounds", "Diamond Barbers Night Markets",
                            "Diamond Barbers Northern Beaches"],
    "Alfon Amora":         ["Diamond Barbers Rising Sun"],
    "Brazil Lamsen":       ["Diamond Barbers Wulguru"],
}

DAY_NAMES = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

GQL_QUERY = """
query employeeWorkingDays($dateFrom: Date!, $dateTo: Date!, $locationId: IID!, $employeeIds: [IID!]!) {
  blockedTimeOccurrences(employeeIds: $employeeIds locationId: $locationId fromDate: $dateFrom toDate: $dateTo) {
    id employeeId date startTime endTime __typename
  }
  timesOffOccurrences(employeeIds: $employeeIds fromDate: $dateFrom toDate: $dateTo) {
    employeeId date startTime endTime __typename
  }
  employeeScheduleDays(employeeIds: $employeeIds fromDate: $dateFrom toDate: $dateTo locationId: $locationId) {
    date employeeId locationId
    shifts { startTime endTime __typename }
    __typename
  }
}
"""


# ── Time helpers ───────────────────────────────────────────────────────────────

def _mins(t):
    h, m = t.split(":")[:2]
    return int(h) * 60 + int(m)


def _day_index(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").weekday()


# ── Hours calculation (per day, not just weekday bucket) ───────────────────────

def calc_hours_per_day(schedule_days, blocked_times, times_off, emp_ids, date_from, date_to):
    results = {}
    for emp_id in emp_ids:
        daily = {d: 0 for d in DAY_NAMES}

        for day in schedule_days:
            if day["employeeId"] != emp_id:
                continue
            if not (date_from <= day["date"] <= date_to):
                continue
            idx = _day_index(day["date"])
            for shift in day.get("shifts", []):
                daily[DAY_NAMES[idx]] += _mins(shift["endTime"]) - _mins(shift["startTime"])

        for block in blocked_times:
            if block["employeeId"] != emp_id:
                continue
            if not (date_from <= block["date"] <= date_to):
                continue
            idx = _day_index(block["date"])
            daily[DAY_NAMES[idx]] -= _mins(block["endTime"]) - _mins(block["startTime"])

        for off in times_off:
            if off["employeeId"] != emp_id:
                continue
            if not (date_from <= off.get("date", "") <= date_to):
                continue
            if off.get("startTime") and off.get("endTime"):
                idx = _day_index(off["date"])
                daily[DAY_NAMES[idx]] -= _mins(off["endTime"]) - _mins(off["startTime"])

        results[emp_id] = {d: round(max(0, daily[d]) / 60, 2) for d in DAY_NAMES}
        results[emp_id]["total"] = round(sum(max(0, daily[d]) for d in DAY_NAMES) / 60, 2)

    return results


# ── Fresha hours fetch (API calls only — no page navigation needed) ────────────

async def fetch_hours(account, context, date_from, date_to):
    pid     = account["provider_id"]
    label   = account["label"]
    default = account["default_org"]
    gql_url = f"https://staff-working-hours-api.fresha.com/graphql?__pid={pid}"

    print(f"\n  [HOURS] Fetching via API...")

    loc_resp = await context.request.get(
        f"https://partners-api.fresha.com/locations?__pid={pid}"
    )
    loc_data  = await loc_resp.json()
    locations = [
        {"id": item["id"], "name": item["attributes"].get("name", item["id"])}
        for item in loc_data.get("data", [])
        if not item["attributes"].get("deleted-at")
    ]
    print(f"  Found {len(locations)} locations: {[l['name'] for l in locations]}")

    # combined[emp_name] = {monday: 0, ..., total: 0, xero_org: "..."}
    combined = {}

    for loc in locations:
        loc_id   = loc["id"]
        loc_name = loc["name"]
        xero_org = LOCATION_TO_XERO_ORG.get(loc_name, default)

        emp_resp = await context.request.get(
            f"https://partners-api.fresha.com/v2/employees"
            f"?location-id={loc_id}&with-deleted=false&__pid={pid}"
        )
        emp_data  = await emp_resp.json()
        employees = []
        for item in emp_data.get("data", []):
            attrs = item.get("attributes", {})
            name  = f"{attrs.get('first-name','')} {attrs.get('last-name','')}".strip()
            if name:
                employees.append({"id": item["id"], "name": name})

        if not employees:
            continue

        emp_ids  = [e["id"] for e in employees]
        gql_resp = await context.request.post(
            gql_url,
            data=json.dumps({
                "operationName": "employeeWorkingDays",
                "query": GQL_QUERY,
                "variables": {
                    "dateFrom":    date_from,
                    "dateTo":      date_to,
                    "employeeIds": emp_ids,
                    "locationId":  loc_id,
                },
            }),
            headers={"Content-Type": "application/json"},
        )
        gql_data = await gql_resp.json()
        wh       = gql_data.get("data", {})

        hours = calc_hours_per_day(
            wh.get("employeeScheduleDays", []),
            wh.get("blockedTimeOccurrences", []),
            wh.get("timesOffOccurrences", []),
            emp_ids, date_from, date_to,
        )

        emp_map = {e["id"]: e["name"] for e in employees}
        for emp_id, h in hours.items():
            name = emp_map.get(emp_id, emp_id)
            if h["total"] == 0:
                continue
            if name not in combined:
                combined[name] = {d: 0.0 for d in DAY_NAMES}
                combined[name]["total"]    = 0.0
                combined[name]["xero_org"] = EMPLOYEE_XERO_ORG.get(name, default)
            for d in DAY_NAMES:
                combined[name][d] += h[d]
            combined[name]["total"] += h["total"]

    print(f"  Hours fetched for {len(combined)} staff members.")
    for name, h in sorted(combined.items()):
        days_str = "  ".join(f"{d[:3]}={h[d]:.1f}h" for d in DAY_NAMES if h[d] > 0)
        print(f"    {name:30s}  total={h['total']:.1f}h  [{days_str}]  org={h['xero_org']}")

    return combined


# ── GHL payroll record upsert ──────────────────────────────────────────────────

def ghl_upsert_payroll(employee_name, week_start, week_end, xero_org, hours):
    if not GHL_API_KEY:
        return "no_key"

    r = requests.post(
        f"{GHL_BASE}/objects/custom_objects.payroll/records/search",
        headers=GHL_HEADERS,
        json={
            "locationId": GHL_LOCATION_ID,
            "page":        1,
            "pageLimit":   20,
            "filters": [{"field": "properties.employee_name", "operator": "eq", "value": employee_name}],
        },
    )
    if r.status_code not in (200, 201):
        raise Exception(f"Search failed {r.status_code}: {r.text[:200]}")

    records = [
        rec for rec in r.json().get("records", [])
        if rec.get("properties", {}).get("week_start") == week_start
    ]

    props = {
        "employee_name":        employee_name,
        "week_start":           week_start,
        "week_end":             week_end,
        "xero_org":             xero_org,
        "monday_hours":         hours.get("monday",    0),
        "tuesday_hours":        hours.get("tuesday",   0),
        "wednesday_hours":      hours.get("wednesday", 0),
        "thursday_hours":       hours.get("thursday",  0),
        "friday_hours":         hours.get("friday",    0),
        "saturday_hours":       hours.get("saturday",  0),
        "sunday_hours":         hours.get("sunday",    0),
        "public_holiday_hours": hours.get("public_holiday", 0),
        "total_hours":          hours.get("total",     0),
    }

    if records:
        r = requests.put(
            f"{GHL_BASE}/objects/custom_objects.payroll/records/{records[0]['id']}",
            headers=GHL_HEADERS,
            params={"locationId": GHL_LOCATION_ID},
            json={"properties": props},
        )
        action = "updated"
    else:
        r = requests.post(
            f"{GHL_BASE}/objects/custom_objects.payroll/records",
            headers=GHL_HEADERS,
            json={"locationId": GHL_LOCATION_ID, "properties": props},
        )
        action = "created"

    if r.status_code in (200, 201):
        return action
    raise Exception(f"GHL {r.status_code}: {r.text[:200]}")


def ghl_update_performance(employee_name, week_start, tips, commissions, service_sales_exc_gst, occupancy_rate):
    if not GHL_API_KEY:
        return "no_key"

    r = requests.post(
        f"{GHL_BASE}/objects/custom_objects.payroll/records/search",
        headers=GHL_HEADERS,
        json={
            "locationId": GHL_LOCATION_ID,
            "page":        1,
            "pageLimit":   20,
            "filters": [{"field": "properties.employee_name", "operator": "eq", "value": employee_name}],
        },
    )
    if r.status_code not in (200, 201):
        raise Exception(f"Search failed {r.status_code}: {r.text[:200]}")

    records = [
        rec for rec in r.json().get("records", [])
        if rec.get("properties", {}).get("week_start") == week_start
    ]
    if not records:
        return "no_record"

    r = requests.put(
        f"{GHL_BASE}/objects/custom_objects.payroll/records/{records[0]['id']}",
        headers=GHL_HEADERS,
        params={"locationId": GHL_LOCATION_ID},
        json={"properties": {
            "tips":                  tips,
            "commissions":           commissions,
            "service_sales_exc_gst": service_sales_exc_gst,
            "occupancy_rate":        occupancy_rate,
        }},
    )
    if r.status_code in (200, 201):
        return "updated"
    raise Exception(f"GHL {r.status_code}: {r.text[:200]}")


def ghl_upsert_location(location_name, week_start, week_end, location_label, services_ex_gst, commissions, occupancy_pct):
    if not GHL_API_KEY:
        return "no_key"

    r = requests.post(
        f"{GHL_BASE}/objects/custom_objects.location_performance/records/search",
        headers=GHL_HEADERS,
        json={
            "locationId": GHL_LOCATION_ID,
            "page":        1,
            "pageLimit":   20,
            "filters": [{"field": "properties.location_name", "operator": "eq", "value": location_name}],
        },
    )
    if r.status_code not in (200, 201):
        raise Exception(f"Search failed {r.status_code}: {r.text[:200]}")

    records = [
        rec for rec in r.json().get("records", [])
        if rec.get("properties", {}).get("week_start") == week_start
    ]

    properties = {
        "location_name":   location_name,
        "week_start":      week_start,
        "week_end":        week_end,
        "location":        location_label,
        "services_ex_gst": services_ex_gst,
        "commissions":     commissions,
        "occupancy_rate":  occupancy_pct,
    }

    if records:
        r = requests.put(
            f"{GHL_BASE}/objects/custom_objects.location_performance/records/{records[0]['id']}",
            headers=GHL_HEADERS,
            params={"locationId": GHL_LOCATION_ID},
            json={"properties": properties},
        )
        action = "updated"
    else:
        r = requests.post(
            f"{GHL_BASE}/objects/custom_objects.location_performance/records",
            headers=GHL_HEADERS,
            json={"locationId": GHL_LOCATION_ID, "properties": properties},
        )
        action = "created"

    if r.status_code in (200, 201):
        return action
    raise Exception(f"GHL {r.status_code}: {r.text[:200]}")


# ── Fresha session login ───────────────────────────────────────────────────────

async def ensure_logged_in(account, page, context):
    session_file = account["session"]
    label        = account["label"]

    await page.goto("https://partners.fresha.com/reports", wait_until="networkidle")
    await page.wait_for_timeout(3000)

    if "/users/sign-in" not in page.url:
        return

    print("  Session expired. Logging in...")
    session_file.unlink(missing_ok=True)

    email    = os.environ.get(account["email_env"], "")
    password = os.environ.get(account["pass_env"], "")

    try:
        await page.get_by_role("button", name="Accept all").click(timeout=5000)
    except Exception:
        pass

    email_field = page.locator('input[placeholder="Enter your email address"]')
    await email_field.wait_for(timeout=10000)
    await email_field.fill(email)
    await page.click('[data-qa="continue"]', force=True)
    await page.wait_for_selector('input[type="password"]:not([tabindex="-1"])', timeout=15000)
    await page.locator('input[type="password"]:not([tabindex="-1"])').fill(password)

    try:
        await page.locator('button[type="submit"]').click(force=True, timeout=5000)
    except Exception:
        await page.keyboard.press("Enter")

    print("  Waiting for 2FA (5 minutes)...")
    try:
        await page.wait_for_url(lambda url: "/users/sign-in" not in url, timeout=300000)
    except Exception:
        pass

    if "/users/sign-in" in page.url:
        raise Exception(f"Login failed for {label}.")

    await context.storage_state(path=str(session_file))
    print("  Session saved.")


# ── Fresha performance CSV download ───────────────────────────────────────────

async def download_performance_csvs(account, page, context, date_from_fallback, date_to_fallback):
    label = account["label"]
    print(f"\n  [PERFORMANCE] Navigating to Performance Summary...")

    await page.goto(
        "https://partners.fresha.com/reports/table/performance-summary",
        wait_until="networkidle"
    )
    await page.wait_for_timeout(4000)

    print("  Selecting Last week...")
    await page.get_by_text("Month to date", exact=True).first.click(timeout=10000)
    await page.wait_for_timeout(1000)
    await page.locator('select:has(option[value="last_week"])').select_option(value="last_week")
    await page.wait_for_timeout(1000)

    try:
        await page.get_by_role("button", name="Apply").click(timeout=5000)
    except Exception:
        pass

    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(10000)

    confirmed_url = page.url
    await page.goto(confirmed_url, wait_until="networkidle")
    await page.wait_for_timeout(8000)

    parsed    = urlparse(page.url)
    params    = parse_qs(parsed.query)
    date_from = params.get("dateFrom", [date_from_fallback])[0]
    date_to   = params.get("dateTo",   [date_to_fallback])[0]
    print(f"  Date range: {date_from} to {date_to}")

    # Team member CSV
    print("  Downloading team member CSV...")
    async with page.expect_download(timeout=30000) as dl_info:
        await page.get_by_role("button", name="Options").click(timeout=10000)
        await page.wait_for_timeout(1500)
        await page.get_by_role("menuitem", name="CSV").click(timeout=10000)

    download  = await dl_info.value
    csv_path  = DATA_DIR / f"fresha_report_{label.split()[0].lower()}_{datetime.now().strftime('%Y%m%d')}.csv"
    await download.save_as(str(csv_path))
    print(f"  Team member CSV saved: {csv_path.name}")

    # Location CSV
    loc_csv_path = None
    try:
        print("  Switching to Location grouping...")
        try:
            await page.get_by_role("button", name="Team member").click(timeout=8000)
        except Exception:
            await page.get_by_text("Team member").first.click(timeout=8000)
        await page.wait_for_timeout(1000)
        await page.get_by_text("Location", exact=True).click(timeout=5000)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(6000)

        print("  Downloading location CSV...")
        async with page.expect_download(timeout=30000) as dl_info2:
            await page.get_by_role("button", name="Options").click(timeout=10000)
            await page.wait_for_timeout(1500)
            await page.get_by_role("menuitem", name="CSV").click(timeout=10000)

        dl2 = await dl_info2.value
        loc_csv_path = DATA_DIR / f"fresha_location_{label.split()[0].lower()}_{datetime.now().strftime('%Y%m%d')}.csv"
        await dl2.save_as(str(loc_csv_path))
        print(f"  Location CSV saved: {loc_csv_path.name}")
    except Exception as e:
        print(f"  WARNING: Could not download location CSV: {e}")

    await context.storage_state(path=str(account["session"]))

    return str(csv_path), str(loc_csv_path) if loc_csv_path else None, date_from, date_to


# ── Claude AI parsing ──────────────────────────────────────────────────────────

def parse_staff_csv(csv_path, api_key, date_from, date_to):
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        csv_content = f.read()

    print("  Parsing team member CSV with Claude AI...")
    client  = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": f"""This is a CSV export from Fresha's Performance Summary report (grouped by Team member) for last week.

CRITICAL RULES:
1. Ignore any row labelled "Total" -- extract ONLY individual named staff members.
2. Include every named staff member even if they have zero sales.

Return ONLY valid JSON in this exact structure:
{{
  "period_start": "YYYY-MM-DD",
  "period_end": "YYYY-MM-DD",
  "sales_summary": {{
    "services": 0.00, "products": 0.00, "tips": 0.00, "total_sales": 0.00
  }},
  "staff": [{{
    "name": "Staff Name",
    "services": 0.00,
    "products": 0.00,
    "tips": 0.00,
    "total_sales": 0.00,
    "total_appts": 0,
    "cancelled_appts": 0,
    "no_show_appts": 0,
    "services_sold": 0,
    "occupancy_pct": 0.0
  }}]
}}

Rules: Return ONLY the JSON. All monetary values as plain numbers. occupancy_pct as a percentage (e.g. 72.5 not 0.725).
CSV DATA:
{csv_content}"""}]
    )

    raw   = message.content[0].text
    start = raw.find("{")
    end   = raw.rfind("}") + 1
    data  = json.loads(raw[start:end])

    data["period_start"] = date_from
    data["period_end"]   = date_to

    staff = data.get("staff", [])
    if staff:
        data["sales_summary"]["services"]    = round(sum(s.get("services",    0) for s in staff), 2)
        data["sales_summary"]["products"]    = round(sum(s.get("products",    0) for s in staff), 2)
        data["sales_summary"]["tips"]        = round(sum(s.get("tips",        0) for s in staff), 2)
        data["sales_summary"]["total_sales"] = round(sum(s.get("total_sales", 0) for s in staff), 2)

    for s in staff:
        products = s.get("products", 0) or 0
        services = s.get("services", 0) or 0
        s["commissions"]           = round(products / 1.1 * 0.10, 2)
        s["service_sales_exc_gst"] = round(services / 1.1, 2)

    print(f"  Parsed {len(staff)} staff members. "
          f"Services=${data['sales_summary']['services']:.2f}  "
          f"Products=${data['sales_summary']['products']:.2f}  "
          f"Tips=${data['sales_summary']['tips']:.2f}")

    return data


def parse_location_csv(csv_path, api_key):
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        csv_content = f.read()

    print("  Parsing location CSV with Claude AI...")
    client  = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": f"""This is a CSV export from Fresha's Performance Summary report grouped by Location.

CRITICAL: Ignore any "Total" or summary rows. Extract ONLY individual named locations.

Return ONLY a valid JSON array:
[
  {{
    "name": "Location Name",
    "services": 0.00,
    "products": 0.00,
    "total_sales": 0.00,
    "occupancy_pct": 0.0
  }}
]

Rules: Return ONLY the JSON array. All monetary values as plain numbers. occupancy_pct as a percentage (e.g. 72.5 not 0.725).
CSV DATA:
{csv_content}"""}]
    )

    raw   = message.content[0].text
    start = raw.find("[")
    end   = raw.rfind("]") + 1
    locations = json.loads(raw[start:end])

    for loc in locations:
        services = loc.get("services", 0) or 0
        products = loc.get("products", 0) or 0
        loc["services_ex_gst"]      = round(services / 1.1, 2)
        loc["location_commissions"] = round(products / 1.1 * 0.10, 2)

    print(f"  Parsed {len(locations)} locations.")
    for loc in locations:
        print(f"    {loc['name']:35s}  svc_ex_gst=${loc['services_ex_gst']:.2f}  "
              f"commissions=${loc['location_commissions']:.2f}  occupancy={loc.get('occupancy_pct',0):.1f}%")
    return locations


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    has_ghl = bool(GHL_API_KEY)

    async with async_playwright() as p:
        for account in ACCOUNTS:
            label        = account["label"]
            session_file = account["session"]
            output_file  = account["output"]
            tz           = account["timezone"]

            print(f"\n{'='*60}")
            print(f"ACCOUNT: {label}")
            print(f"{'='*60}")

            if not session_file.exists():
                print(f"  WARNING: {session_file.name} not found -- skipping.")
                continue

            today     = datetime.now(tz)
            last_mon  = today - timedelta(days=today.weekday() + 7)
            last_sun  = last_mon + timedelta(days=6)
            date_from = last_mon.strftime("%Y-%m-%d")
            date_to   = last_sun.strftime("%Y-%m-%d")

            headless = os.environ.get("CI", "false").lower() == "true"
            browser  = await p.chromium.launch(headless=headless)
            context  = await browser.new_context(
                storage_state=str(session_file),
                accept_downloads=True,
                viewport={"width": 1280, "height": 800},
            )
            page = await context.new_page()

            # ── Step 1: Check login, then fetch hours via API ─────────────────
            try:
                await ensure_logged_in(account, page, context)
            except Exception as e:
                print(f"  ERROR logging in: {e}")
                await browser.close()
                continue

            try:
                hours_data = await fetch_hours(account, context, date_from, date_to)
            except Exception as e:
                print(f"  ERROR fetching hours: {e}")
                hours_data = {}

            # Push hours to GHL payroll records
            if has_ghl and hours_data:
                print(f"\n  Pushing hours to GHL ({len(hours_data)} staff)...")
                ok = err = 0
                for name, h in hours_data.items():
                    try:
                        result = ghl_upsert_payroll(
                            employee_name = name,
                            week_start    = date_from,
                            week_end      = date_to,
                            xero_org      = h["xero_org"],
                            hours         = h,
                        )
                        print(f"    {result:7s}  {name}")
                        ok += 1
                    except Exception as e:
                        print(f"    ERROR {name}: {e}")
                        err += 1
                print(f"  Hours: {ok} pushed, {err} errors.")
            elif not has_ghl:
                print("\n  GHL_API_KEY not set -- skipping GHL hours push.")

            # ── Step 2: Download performance CSVs (browser navigation) ────────
            try:
                csv_path, loc_csv_path, date_from, date_to = await download_performance_csvs(
                    account, page, context, date_from, date_to
                )
            except Exception as e:
                print(f"  ERROR downloading CSVs: {e}")
                screenshot = str(DATA_DIR / f"error_{label.split()[0].lower()}.png")
                await page.screenshot(path=screenshot)
                print(f"  Screenshot saved: {screenshot}")
                await browser.close()
                continue

            # Parse staff CSV
            try:
                perf_data = parse_staff_csv(csv_path, api_key, date_from, date_to)
            except Exception as e:
                print(f"  ERROR parsing staff CSV: {e}")
                await browser.close()
                continue

            # Parse location CSV
            locations = []
            if loc_csv_path and Path(loc_csv_path).exists():
                try:
                    locations = parse_location_csv(loc_csv_path, api_key)
                except Exception as e:
                    print(f"  ERROR parsing location CSV: {e}")

            # ── Step 3: Save JSON history for dashboard ───────────────────────
            perf_data["report_date"] = datetime.now().strftime("%Y-%m-%d")
            perf_data["report_type"] = "performance_summary"

            if output_file.exists():
                history = json.loads(output_file.read_text())
                if not isinstance(history, list):
                    history = [history]
            else:
                history = []

            history.append(perf_data)
            output_file.write_text(json.dumps(history, indent=2))
            print(f"\n  Dashboard JSON saved to {output_file.name}")

            # ── Step 4: Update GHL payroll records with tips/commissions ──────
            if has_ghl:
                loc_products = {loc["name"]: loc.get("products", 0) or 0 for loc in locations}

                print(f"\n  Updating GHL payroll records with tips/commissions...")
                ok = skipped = 0
                for s in perf_data.get("staff", []):
                    name                  = s.get("name", "").strip()
                    tips                  = s.get("tips", 0) or 0
                    commissions           = s.get("commissions", 0) or 0
                    service_sales_exc_gst = s.get("service_sales_exc_gst", 0) or 0
                    occupancy_rate        = s.get("occupancy_pct", 0) or 0
                    if not name:
                        continue

                    if name in MANAGER_LOCATIONS:
                        total_products = sum(loc_products.get(loc, 0) for loc in MANAGER_LOCATIONS[name])
                        commissions    = round(total_products * 0.9 * 0.10, 2)
                        print(f"    MANAGER {name:28s}  products=${total_products:.2f}  comm=${commissions:.2f}")

                    try:
                        result = ghl_update_performance(name, date_from, tips, commissions, service_sales_exc_gst, occupancy_rate)
                        if result == "no_record":
                            print(f"    SKIP  {name:30s}  (no payroll record)")
                            skipped += 1
                        else:
                            print(f"    OK    {name:30s}  tips=${tips:.2f}  comm=${commissions:.2f}")
                            ok += 1
                    except Exception as e:
                        print(f"    ERROR {name}: {e}")

                print(f"\n  Performance: {ok} updated, {skipped} skipped.")

                # ── Step 5: Push location_performance to GHL ──────────────────
                if locations:
                    print(f"\n  Pushing {len(locations)} location records to GHL...")
                    lok = 0
                    for loc in locations:
                        loc_name = loc.get("name", "").strip()
                        if not loc_name:
                            continue
                        try:
                            result = ghl_upsert_location(
                                location_name   = loc_name,
                                week_start      = date_from,
                                week_end        = date_to,
                                location_label  = LOCATION_TO_ORG.get(loc_name, loc_name),
                                services_ex_gst = loc.get("services_ex_gst", 0),
                                commissions     = loc.get("location_commissions", 0),
                                occupancy_pct   = loc.get("occupancy_pct", 0),
                            )
                            print(f"    {result:7s}  {loc_name}")
                            lok += 1
                        except Exception as e:
                            print(f"    ERROR {loc_name}: {e}")
                    print(f"  Location records: {lok}/{len(locations)} pushed.")

            await browser.close()

    print("\nAll accounts processed.")


if __name__ == "__main__":
    asyncio.run(run())
