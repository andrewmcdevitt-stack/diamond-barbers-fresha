"""
fetch_hours.py
--------------
Fetches roster hours for all staff from both Fresha accounts (NT + QLD).
Calculates actual hours worked = shifts - blocked time - time off, split
into Mon / Tue / Wed / Thu / Fri / Sat / Sun / Public Holiday buckets.

Pushes one GHL Weekly Payroll record per employee into Go High Level.

Run with:  python agent/fetch_hours.py
Requires:  data/session.json        (NT Fresha session)
           data/session_cairns.json (QLD Fresha session)
"""

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"

GHL_API_KEY     = os.environ["GHL_API_KEY"]
GHL_LOCATION_ID = os.environ["GHL_LOCATION_ID"]
GHL_BASE        = "https://services.leadconnectorhq.com"
GHL_HEADERS     = {
    "Authorization": f"Bearer {GHL_API_KEY}",
    "Version":       "2021-07-28",
    "Accept":        "application/json",
    "Content-Type":  "application/json",
}

# Fresha location name → GHL Xero Org label
LOCATION_TO_ORG = {
    # NT locations → Darwin or Parap
    "Diamond Barbers - Darwin":          "Diamond Barbers Darwin",
    "Diamond Barbers Darwin":            "Diamond Barbers Darwin",
    "Diamond Barbers - Parap":           "Diamond Barbers Parap",
    "Diamond Barbers Parap":             "Diamond Barbers Parap",
    # QLD locations → Cairns
    "Diamond Barbers Rising Sun":        "Diamond Barbers Cairns",
    "Diamond Barbers Showgrounds":       "Diamond Barbers Cairns",
    "Diamond Barbers Northern Beaches":  "Diamond Barbers Cairns",
    "Diamond Barbers Night Markets":     "Diamond Barbers Cairns",
    "Diamond Barbers Wulguru":           "Diamond Barbers Cairns",
}

ACCOUNTS = [
    {
        "label":       "NT (Darwin + Parap)",
        "session":     DATA_DIR / "session.json",
        "provider_id": "1371504",
        "timezone":    timezone(timedelta(hours=9, minutes=30)),
        "holidays":    PUBLIC_HOLIDAYS_NT,
    },
    {
        "label":       "QLD (Cairns)",
        "session":     DATA_DIR / "session_cairns.json",
        "provider_id": "1390965",
        "timezone":    timezone(timedelta(hours=10)),
        "holidays":    PUBLIC_HOLIDAYS_QLD,
    },
]

GQL_QUERY = """
query employeeWorkingDays($dateFrom: Date!, $dateTo: Date!, $locationId: IID!, $employeeIds: [IID!]!) {
  blockedTimeOccurrences(
    employeeIds: $employeeIds
    locationId: $locationId
    fromDate: $dateFrom
    toDate: $dateTo
  ) {
    id employeeId date startTime endTime title __typename
  }
  timesOffOccurrences(
    employeeIds: $employeeIds
    fromDate: $dateFrom
    toDate: $dateTo
  ) {
    employeeId date startTime endTime __typename
  }
  employeeScheduleDays(
    employeeIds: $employeeIds
    fromDate: $dateFrom
    toDate: $dateTo
    locationId: $locationId
  ) {
    date employeeId locationId
    shifts { startTime endTime __typename }
    __typename
  }
}
"""

# Public holidays by state — dates when shifts count as public holiday pay
# Sources: fairwork.gov.au, qld.gov.au
PUBLIC_HOLIDAYS_NT = {
    "2026-01-01",  # New Year's Day
    "2026-01-26",  # Australia Day
    "2026-04-03",  # Good Friday
    "2026-04-04",  # Easter Saturday
    "2026-04-05",  # Easter Sunday
    "2026-04-06",  # Easter Monday
    "2026-04-25",  # ANZAC Day
    "2026-05-04",  # May Day
    "2026-06-08",  # King's Birthday (NT)
    "2026-08-03",  # Picnic Day (NT)
    "2026-12-25",  # Christmas Day
    "2026-12-26",  # Boxing Day
}

PUBLIC_HOLIDAYS_QLD = {
    "2026-01-01",  # New Year's Day
    "2026-01-26",  # Australia Day
    "2026-04-03",  # Good Friday
    "2026-04-04",  # Easter Saturday
    "2026-04-05",  # Easter Sunday
    "2026-04-06",  # Easter Monday
    "2026-04-25",  # ANZAC Day
    "2026-05-04",  # Labour Day
    "2026-10-05",  # King's Birthday (QLD)
    "2026-12-25",  # Christmas Day
    "2026-12-26",  # Boxing Day
    "2026-12-28",  # Boxing Day substitute
}


def parse_time_to_minutes(t):
    parts = t.split(":")
    return int(parts[0]) * 60 + int(parts[1])


def day_bucket(date_str, holidays):
    if date_str in holidays:
        return "public_holiday"
    d = datetime.strptime(date_str, "%Y-%m-%d").weekday()
    return ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][d]


def calc_hours(schedule_days, blocked_times, times_off, employee_ids, date_from, date_to, holidays):
    results = {}
    for emp_id in employee_ids:
        buckets = {d: 0 for d in ["monday","tuesday","wednesday","thursday","friday","saturday","sunday","public_holiday"]}

        for day in schedule_days:
            if day["employeeId"] != emp_id:
                continue
            if not (date_from <= day["date"] <= date_to):
                continue
            bucket = day_bucket(day["date"], holidays)
            for shift in day.get("shifts", []):
                buckets[bucket] += parse_time_to_minutes(shift["endTime"]) - parse_time_to_minutes(shift["startTime"])

        for block in blocked_times:
            if block["employeeId"] != emp_id:
                continue
            if not (date_from <= block["date"] <= date_to):
                continue
            bucket = day_bucket(block["date"], holidays)
            buckets[bucket] -= parse_time_to_minutes(block["endTime"]) - parse_time_to_minutes(block["startTime"])

        for off in times_off:
            if off["employeeId"] != emp_id:
                continue
            if not (date_from <= off.get("date", "") <= date_to):
                continue
            if off.get("startTime") and off.get("endTime"):
                bucket = day_bucket(off["date"], holidays)
                buckets[bucket] -= parse_time_to_minutes(off["endTime"]) - parse_time_to_minutes(off["startTime"])

        results[emp_id] = {k: round(max(0, v) / 60, 2) for k, v in buckets.items()}
        results[emp_id]["total_hours"] = round(sum(results[emp_id].values()), 2)
    return results


# ── GHL helpers ───────────────────────────────────────────────────────────────

def ghl_find_record(employee_name, week_start):
    """Find existing GHL payroll record for this employee + week."""
    r = requests.post(
        f"{GHL_BASE}/objects/custom_objects.payroll/records/search",
        headers=GHL_HEADERS,
        json={
            "locationId": GHL_LOCATION_ID,
            "page":        1,
            "pageLimit":   20,
            "filters": [
                {"field": "properties.employee_name", "operator": "eq", "value": employee_name},
            ],
        },
    )
    if r.status_code not in (200, 201):
        return None
    records = [
        rec for rec in r.json().get("records", [])
        if rec.get("properties", {}).get("week_start") == week_start
    ]
    return records[0]["id"] if records else None


def ghl_upsert_record(employee_name, week_start, week_end, xero_org, hours):
    """Create or update a GHL Weekly Payroll record."""
    properties = {
        "employee_name":       employee_name,
        "week_start":          week_start,
        "week_end":            week_end,
        "xero_org":            xero_org,
        "monday_hours":        hours.get("monday", 0),
        "tuesday_hours":       hours.get("tuesday", 0),
        "wednesday_hours":     hours.get("wednesday", 0),
        "thursday_hours":      hours.get("thursday", 0),
        "friday_hours":        hours.get("friday", 0),
        "saturday_hours":      hours.get("saturday", 0),
        "sunday_hours":        hours.get("sunday", 0),
        "public_holiday_hours": hours.get("public_holiday", 0),
        "total_hours":         hours.get("total_hours", 0),
    }

    existing_id = ghl_find_record(employee_name, week_start)

    if existing_id:
        r = requests.put(
            f"{GHL_BASE}/objects/custom_objects.payroll/records/{existing_id}",
            headers=GHL_HEADERS,
            params={"locationId": GHL_LOCATION_ID},
            json={"properties": properties},
        )
        action = "updated"
    else:
        r = requests.post(
            f"{GHL_BASE}/objects/custom_objects.payroll/records",
            headers=GHL_HEADERS,
            json={"locationId": GHL_LOCATION_ID, "properties": properties},
        )
        action = "created"

    if r.status_code in (200, 201):
        return action
    else:
        raise Exception(f"GHL {r.status_code}: {r.text[:200]}")


# ── Per-account fetch ──────────────────────────────────────────────────────────

async def fetch_account(account, context, date_from, date_to, holidays):
    pid     = account["provider_id"]
    label   = account["label"]
    gql_url = f"https://staff-working-hours-api.fresha.com/graphql?__pid={pid}"

    print(f"\n{'='*60}")
    print(f"ACCOUNT: {label}")
    print(f"{'='*60}")

    loc_resp  = await context.request.get(f"https://partners-api.fresha.com/locations?__pid={pid}")
    loc_data  = await loc_resp.json()
    locations = [
        {"id": item["id"], "name": item["attributes"].get("name", item["id"])}
        for item in loc_data.get("data", [])
        if not item["attributes"].get("deleted-at")
    ]
    print(f"Found {len(locations)} location(s): {[l['name'] for l in locations]}")

    # employee_id → {name, xero_org, hours buckets}
    employees_combined = {}

    for loc in locations:
        loc_id   = loc["id"]
        loc_name = loc["name"]
        xero_org = LOCATION_TO_ORG.get(loc_name)
        if not xero_org:
            # Default fallback based on account
            xero_org = "Diamond Barbers Darwin" if "1371504" in pid else "Diamond Barbers Cairns"
            print(f"  WARNING: No org mapping for '{loc_name}' — defaulting to {xero_org}")

        print(f"\n  -- {loc_name} -> {xero_org} --")

        emp_resp  = await context.request.get(
            f"https://partners-api.fresha.com/v2/employees?location-id={loc_id}&with-deleted=false&__pid={pid}"
        )
        emp_data  = await emp_resp.json()
        employees = []
        for item in emp_data.get("data", []):
            attrs = item.get("attributes", {})
            name  = f"{attrs.get('first-name', '')} {attrs.get('last-name', '')}".strip()
            employees.append({"id": item["id"], "name": name})

        if not employees:
            print("    No employees found.")
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
        gql_data      = await gql_resp.json()
        wh            = gql_data.get("data", {})
        hours_by_emp  = calc_hours(
            wh.get("employeeScheduleDays", []),
            wh.get("blockedTimeOccurrences", []),
            wh.get("timesOffOccurrences", []),
            emp_ids, date_from, date_to, holidays,
        )

        for emp in employees:
            h = hours_by_emp.get(emp["id"], {})
            if h.get("total_hours", 0) == 0:
                continue

            eid = emp["id"]
            if eid not in employees_combined:
                employees_combined[eid] = {"name": emp["name"], "xero_org": xero_org, "hours": {k: 0.0 for k in h}}

            for bucket, val in h.items():
                employees_combined[eid]["hours"][bucket] = round(
                    employees_combined[eid]["hours"].get(bucket, 0) + val, 2
                )

            print(f"    {emp['name']:30s}  total={h['total_hours']}h")

    return employees_combined


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    async with async_playwright() as p:
        for account in ACCOUNTS:
            session_file = account["session"]
            if not session_file.exists():
                print(f"WARNING: {session_file.name} not found — skipping {account['label']}.")
                continue

            tz        = account["timezone"]
            today     = datetime.now(tz)
            last_mon  = today - timedelta(days=today.weekday() + 7)
            last_sun  = last_mon + timedelta(days=6)
            date_from = last_mon.strftime("%Y-%m-%d")
            date_to   = last_sun.strftime("%Y-%m-%d")
            print(f"\nDate range ({account['label']}): {date_from} to {date_to}")

            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(storage_state=str(session_file))

            employees = await fetch_account(account, context, date_from, date_to, account["holidays"])

            print(f"\nPushing {len(employees)} employees to GHL...")
            ok = 0
            for emp_id, emp in employees.items():
                try:
                    action = ghl_upsert_record(
                        employee_name = emp["name"],
                        week_start    = date_from,
                        week_end      = date_to,
                        xero_org      = emp["xero_org"],
                        hours         = emp["hours"],
                    )
                    print(f"  {action:7s}  {emp['name']:30s}  {emp['xero_org']}  total={emp['hours'].get('total_hours',0)}h")
                    ok += 1
                except Exception as e:
                    print(f"  ERROR  {emp['name']}: {e}")

            print(f"\nDone — {ok}/{len(employees)} records pushed to GHL.")
            await browser.close()


if __name__ == "__main__":
    asyncio.run(run())
