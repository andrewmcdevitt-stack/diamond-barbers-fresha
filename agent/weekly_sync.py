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
import subprocess
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
        "label":               "NT (Darwin)",
        "session":             DATA_DIR / "session.json",
        "email_env":           "FRESHA_EMAIL",
        "pass_env":            "FRESHA_PASSWORD",
        "timezone":            timezone(timedelta(hours=9, minutes=30)),
        "provider_id":         "1371504",
        "default_org":         "Diamond Barbers Darwin",
        "output":              DATA_DIR / "performance_summary.json",
        "night_markets_loc":   None,
    },
    {
        "label":               "QLD (Cairns)",
        "session":             DATA_DIR / "session_cairns.json",
        "email_env":           "CAIRNS_FRESHA_EMAIL",
        "pass_env":            "CAIRNS_FRESHA_PASSWORD",
        "timezone":            timezone(timedelta(hours=10)),
        "provider_id":         "1390965",
        "default_org":         "Diamond Barbers Cairns",
        "output":              DATA_DIR / "cairns_performance_summary.json",
        "night_markets_loc":   "Diamond Barbers Night Markets",
        "night_markets_loc_id": "1472834",
    },
]

# Barbers at Night Markets are paid 50% of their Night Markets service revenue
# (ex-GST) as a bonus instead of hourly rates for that location.
NIGHT_MARKETS_COMMISSION_RATE = 0.50

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

# Add public holiday dates here (YYYY-MM-DD). Hours worked on these days go into
# public_holiday_hours instead of the normal day bucket.
PUBLIC_HOLIDAYS = {
    "2026-01-01",  # New Year's Day
    "2026-01-26",  # Australia Day
    "2026-04-03",  # Good Friday
    "2026-04-04",  # Easter Saturday
    "2026-04-05",  # Easter Sunday
    "2026-04-06",  # Easter Monday
    "2026-04-25",  # ANZAC Day
    "2026-05-04",  # May Day (NT)
    "2026-06-08",  # Queen's Birthday (QLD)
    "2026-08-10",  # Picnic Day (NT)
    "2026-10-05",  # Labour Day (QLD)
    "2026-12-25",  # Christmas Day
    "2026-12-26",  # Boxing Day
    "2026-12-28",  # Boxing Day observed (if 26th is weekend)
}

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

def _day_bucket(date_str):
    """Return 'public_holiday' if date is a public holiday, else the weekday name."""
    return "public_holiday" if date_str in PUBLIC_HOLIDAYS else DAY_NAMES[_day_index(date_str)]


def calc_hours_per_day(schedule_days, blocked_times, times_off, emp_ids, date_from, date_to):
    results = {}
    for emp_id in emp_ids:
        daily = {d: 0 for d in DAY_NAMES}
        daily["public_holiday"] = 0

        for day in schedule_days:
            if day["employeeId"] != emp_id:
                continue
            if not (date_from <= day["date"] <= date_to):
                continue
            bucket = _day_bucket(day["date"])
            for shift in day.get("shifts", []):
                daily[bucket] += _mins(shift["endTime"]) - _mins(shift["startTime"])

        for block in blocked_times:
            if block["employeeId"] != emp_id:
                continue
            if not (date_from <= block["date"] <= date_to):
                continue
            bucket = _day_bucket(block["date"])
            daily[bucket] -= _mins(block["endTime"]) - _mins(block["startTime"])

        for off in times_off:
            if off["employeeId"] != emp_id:
                continue
            if not (date_from <= off.get("date", "") <= date_to):
                continue
            if off.get("startTime") and off.get("endTime"):
                bucket = _day_bucket(off["date"])
                daily[bucket] -= _mins(off["endTime"]) - _mins(off["startTime"])

        all_buckets = DAY_NAMES + ["public_holiday"]
        results[emp_id] = {b: round(max(0, daily[b]) / 60, 2) for b in all_buckets}
        results[emp_id]["total"] = round(sum(max(0, daily[b]) for b in all_buckets) / 60, 2)

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
                combined[name]["public_holiday"] = 0.0
                combined[name]["total"]    = 0.0
                combined[name]["xero_org"] = EMPLOYEE_XERO_ORG.get(name, default)
            for d in DAY_NAMES:
                combined[name][d] += h[d]
            combined[name]["public_holiday"] += h.get("public_holiday", 0)
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


def ghl_update_bonus(employee_name, week_start, bonus):
    """Push Night Markets 50-50 service bonus to the GHL payroll record's bonus field."""
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
        json={"properties": {"bonus": bonus}},
    )
    if r.status_code in (200, 201):
        return "updated"
    raise Exception(f"GHL {r.status_code}: {r.text[:200]}")


# ── Issue-detection checklist ───────────────────────────────────────────────────
# Every notable click/login/download/push step gets recorded here as
# OK / FAIL / SKIP / FLAG so the end-of-run report is a copy-pasteable
# checklist of exactly what happened, rather than a wall of print() output.

async def _try_click(checklist, name, click_coro, required=False):
    """Await a click action and record the outcome on the checklist.

    required=True means this step is essential (e.g. the actual sign-in
    button) — failure raises so the caller's error handling kicks in.
    required=False means it's an optional dismissal (cookie banners, modals
    that may or may not appear) — failure is recorded as SKIP, not an error.
    """
    try:
        await click_coro
        checklist.append({"check": name, "status": "OK"})
        return True
    except Exception as e:
        detail = str(e).splitlines()[0][:160]
        checklist.append({"check": name, "status": "FAIL" if required else "SKIP", "detail": detail})
        if required:
            raise Exception(f"{name} failed: {detail}")
        return False


def flag_zero_value_issues(perf_data, locations, checklist):
    """Flag suspicious zero totals so a silently-broken scrape doesn't get pushed unnoticed."""
    summary = perf_data.get("sales_summary", {})
    if summary.get("total_sales", 0) == 0:
        checklist.append({
            "check": "Weekly total_sales",
            "status": "FLAG",
            "detail": "Total sales = $0 for the week — verify the CSV covered the correct date range",
        })
    for s in perf_data.get("staff", []):
        if (s.get("total_sales", 0) or 0) == 0 and (s.get("total_appts", 0) or 0) == 0:
            checklist.append({
                "check": f"Staff '{s.get('name')}' totals",
                "status": "FLAG",
                "detail": "Zero sales AND zero appointments — confirm this is expected, not a parse miss",
            })
    for loc in locations:
        if (loc.get("total_sales", 0) or 0) == 0:
            checklist.append({
                "check": f"Location '{loc.get('name')}' totals",
                "status": "FLAG",
                "detail": "Zero total sales for the week",
            })


def git_commit_and_push(files, commit_message):
    """Commit + push the given files from a local (non-CI) run. Returns a checklist item."""
    repo_root = Path(__file__).parent.parent
    try:
        subprocess.run(["git", "add", *files], cwd=repo_root, check=True, capture_output=True, text=True)
        staged = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=repo_root)
        if staged.returncode == 0:
            return {"check": "Git commit + push to GitHub", "status": "SKIP", "detail": "No changes to commit"}
        subprocess.run(["git", "commit", "-m", commit_message], cwd=repo_root, check=True, capture_output=True, text=True)
        subprocess.run(["git", "push"], cwd=repo_root, check=True, capture_output=True, text=True)
        return {"check": "Git commit + push to GitHub", "status": "OK"}
    except subprocess.CalledProcessError as e:
        detail = (e.stderr or str(e)).strip().splitlines()[-1][:200] if (e.stderr or str(e)) else str(e)
        return {"check": "Git commit + push to GitHub", "status": "FAIL", "detail": detail}


def print_final_checklist(sync_results, push_status):
    print("\n" + "=" * 60)
    print("SYNC CHECKLIST")
    print("=" * 60)
    for r in sync_results:
        print(f"\n[{r['label']}]  overall status: {r['status'].upper()}")
        for item in r.get("checklist", []):
            line = f"  [{item['status']}] {item['check']}"
            if item.get("detail"):
                line += f" -- {item['detail']}"
            print(line)
        if not r.get("checklist"):
            print("  (no checklist items recorded)")
    if push_status:
        print(f"\n[GLOBAL]")
        line = f"  [{push_status['status']}] {push_status['check']}"
        if push_status.get("detail"):
            line += f" -- {push_status['detail']}"
        print(line)
    print("=" * 60)


# ── Fresha session login ───────────────────────────────────────────────────────

async def ensure_logged_in(account, page, context, checklist):
    session_file = account["session"]
    label        = account["label"]

    for attempt in range(1, 4):
        try:
            await page.goto("https://partners.fresha.com/reports", wait_until="networkidle", timeout=60000)
            break
        except Exception as e:
            if attempt == 3:
                checklist.append({"check": "Fresha page load", "status": "FAIL", "detail": f"Timed out after 3 attempts: {str(e).splitlines()[0][:120]}"})
                raise Exception(f"Page load failed for {label} after 3 attempts: {e}")
            print(f"  Page load attempt {attempt} timed out, retrying...")
            await page.wait_for_timeout(5000)
    await page.wait_for_timeout(3000)

    if "/users/sign-in" not in page.url:
        checklist.append({"check": "Fresha page load", "status": "OK"})
        checklist.append({"check": "Fresha session valid (no login required)", "status": "OK"})
        return

    print("  Session expired. Logging in...")
    session_file.unlink(missing_ok=True)
    checklist.append({"check": "Fresha session valid (no login required)", "status": "SKIP", "detail": "Session expired -- logging in"})

    email    = os.environ.get(account["email_env"], "")
    password = os.environ.get(account["pass_env"], "")

    await _try_click(checklist, "Cookie banner 'Accept all' dismissed",
                      page.get_by_role("button", name="Accept all").click(timeout=5000))

    email_field = page.locator('input[placeholder="Enter your email address"]')
    try:
        await email_field.wait_for(timeout=10000)
        await email_field.fill(email)
        checklist.append({"check": "Email field found and filled", "status": "OK"})
    except Exception as e:
        checklist.append({"check": "Email field found and filled", "status": "FAIL", "detail": str(e).splitlines()[0][:160]})
        raise Exception(f"Login failed for {label}: email field not found ({e})")

    await _try_click(checklist, "'Continue' button clicked",
                      page.click('[data-qa="continue"]', force=True), required=True)

    try:
        await page.wait_for_selector('input[type="password"]:not([tabindex="-1"])', timeout=15000)
        await page.locator('input[type="password"]:not([tabindex="-1"])').fill(password)
        checklist.append({"check": "Password field found and filled", "status": "OK"})
    except Exception as e:
        checklist.append({"check": "Password field found and filled", "status": "FAIL", "detail": str(e).splitlines()[0][:160]})
        raise Exception(f"Login failed for {label}: password field not found ({e})")

    submitted = await _try_click(checklist, "'Sign in' submit button clicked",
                                  page.locator('button[type="submit"]').click(force=True, timeout=5000))
    if not submitted:
        await page.keyboard.press("Enter")
        checklist.append({"check": "Fallback Enter keypress used to submit sign-in", "status": "INFO"})

    print("  Waiting for 2FA (5 minutes)...")
    try:
        await page.wait_for_url(lambda url: "/users/sign-in" not in url, timeout=300000)
    except Exception:
        pass

    if "/users/sign-in" in page.url:
        checklist.append({"check": f"Login completed for {label}", "status": "FAIL", "detail": "Still on sign-in page after 2FA wait (5 min)"})
        raise Exception(f"Login failed for {label}.")

    checklist.append({"check": f"Login completed for {label}", "status": "OK"})
    await context.storage_state(path=str(session_file))
    print("  Session saved.")


# ── Fresha performance CSV download ───────────────────────────────────────────

async def download_performance_csvs(account, page, context, checklist, date_from_fallback, date_to_fallback):
    label = account["label"]
    print(f"\n  [PERFORMANCE] Navigating to Performance Summary...")

    await page.goto(
        "https://partners.fresha.com/reports/table/performance-summary",
        wait_until="networkidle"
    )
    await page.wait_for_timeout(4000)

    # Dismiss any popups/modals that may be blocking interaction
    print("  Dismissing any popups...")
    await page.keyboard.press("Escape")
    await page.wait_for_timeout(500)
    dismissed_any = False
    for close_label in ("Close", "Dismiss", "Got it", "OK", "Done"):
        clicked = await _try_click(checklist, f"Popup dismiss button '{close_label}' clicked",
                                    page.get_by_role("button", name=close_label).click(timeout=1500))
        if clicked:
            dismissed_any = True
            print(f"  Dismissed popup: '{close_label}'")
            await page.wait_for_timeout(500)
    if not dismissed_any:
        checklist.append({"check": "Popups present to dismiss", "status": "INFO", "detail": "None found -- page loaded clean"})

    print("  Selecting Last week...")
    date_chip_clicked = False
    for chip_label in ("Month to date", "Last week", "This week", "Last month"):
        try:
            await page.get_by_text(chip_label, exact=True).first.click(timeout=5000)
            print(f"  Clicked date chip: '{chip_label}'")
            checklist.append({"check": f"Date range chip '{chip_label}' clicked", "status": "OK"})
            date_chip_clicked = True
            break
        except Exception:
            continue
    if not date_chip_clicked:
        checklist.append({"check": "Date range chip clicked", "status": "FAIL", "detail": "None of the expected chip labels were found"})
    await page.wait_for_timeout(1000)
    try:
        await page.locator('select:has(option[value="last_week"])').select_option(value="last_week")
        checklist.append({"check": "'Last week' dropdown option selected", "status": "OK"})
    except Exception as e:
        checklist.append({"check": "'Last week' dropdown option selected", "status": "FAIL", "detail": str(e).splitlines()[0][:160]})
    await page.wait_for_timeout(1000)

    await _try_click(checklist, "'Apply' button clicked",
                      page.get_by_role("button", name="Apply").click(timeout=5000))

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
    try:
        async with page.expect_download(timeout=30000) as dl_info:
            await page.get_by_role("button", name="Options").click(timeout=10000)
            await page.wait_for_timeout(1500)
            await page.get_by_role("menuitem", name="CSV").click(timeout=10000)
        checklist.append({"check": "Team member CSV: Options -> CSV menu clicked", "status": "OK"})
    except Exception as e:
        checklist.append({"check": "Team member CSV: Options -> CSV menu clicked", "status": "FAIL", "detail": str(e).splitlines()[0][:160]})
        raise

    download  = await dl_info.value
    csv_path  = DATA_DIR / f"fresha_report_{label.split()[0].lower()}_{datetime.now().strftime('%Y%m%d')}.csv"
    await download.save_as(str(csv_path))
    checklist.append({"check": "Team member CSV downloaded", "status": "OK", "detail": csv_path.name})
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
        checklist.append({"check": "Switched grouping to 'Location'", "status": "OK"})

        print("  Downloading location CSV...")
        async with page.expect_download(timeout=30000) as dl_info2:
            await page.get_by_role("button", name="Options").click(timeout=10000)
            await page.wait_for_timeout(1500)
            await page.get_by_role("menuitem", name="CSV").click(timeout=10000)

        dl2 = await dl_info2.value
        loc_csv_path = DATA_DIR / f"fresha_location_{label.split()[0].lower()}_{datetime.now().strftime('%Y%m%d')}.csv"
        await dl2.save_as(str(loc_csv_path))
        checklist.append({"check": "Location CSV downloaded", "status": "OK", "detail": loc_csv_path.name})
        print(f"  Location CSV saved: {loc_csv_path.name}")
    except Exception as e:
        detail = str(e).splitlines()[0][:160]
        checklist.append({"check": "Location CSV downloaded", "status": "FAIL", "detail": detail})
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


# ── Night Markets 50-50 bonus ──────────────────────────────────────────────────

async def download_night_markets_csv(account, page, context, checklist, date_from, date_to):
    """Download a team-member CSV filtered to Night Markets only.

    Called after the main CSVs are already downloaded so the page is still
    open.  Returns the local CSV path, or None on failure.
    """
    nm_loc = account.get("night_markets_loc")
    if not nm_loc:
        return None

    print(f"\n  [NIGHT MARKETS] Downloading filtered team-member CSV ({nm_loc})...")

    loc_id = account.get("night_markets_loc_id", "")
    url = (
        f"https://partners.fresha.com/reports/table/performance-summary"
        f"?groupBy=employee_name&location_id={loc_id}&employee_id=all"
        f"&dateFrom={date_from}&dateTo={date_to}"
    )
    try:
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(3000)
        checklist.append({"check": "Night Markets: performance page loaded", "status": "OK"})
    except Exception as e:
        checklist.append({"check": "Night Markets: performance page loaded", "status": "FAIL",
                          "detail": str(e).splitlines()[0][:160]})
        return None

    # Download team-member CSV
    try:
        async with page.expect_download(timeout=30000) as dl_info:
            await page.get_by_role("button", name="Options").click(timeout=10000)
            await page.wait_for_timeout(1500)
            await page.get_by_role("menuitem", name="CSV").click(timeout=10000)
        download = await dl_info.value
        csv_path = DATA_DIR / f"fresha_night_markets_{datetime.now().strftime('%Y%m%d')}.csv"
        await download.save_as(str(csv_path))
        checklist.append({"check": "Night Markets: CSV downloaded", "status": "OK",
                          "detail": csv_path.name})
        print(f"  Night Markets CSV saved: {csv_path.name}")
        return str(csv_path)
    except Exception as e:
        checklist.append({"check": "Night Markets: CSV downloaded", "status": "FAIL",
                          "detail": str(e).splitlines()[0][:160]})
        return None


def parse_night_markets_csv(csv_path, api_key):
    """Parse a Night Markets-filtered team-member CSV.

    Returns a dict of {staff_name: {"services_ex_gst": float, "bonus": float}}
    where bonus = services_ex_gst * NIGHT_MARKETS_COMMISSION_RATE.
    """
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        csv_content = f.read()

    print("  Parsing Night Markets CSV with Claude AI...")
    client  = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": f"""This is a Fresha Performance Summary CSV filtered to ONE location (Night Markets).
Ignore any Total/summary rows. Extract ONLY individual named staff members.

Return ONLY a valid JSON array:
[{{"name": "Staff Name", "services": 0.00}}]

Rules: monetary values as plain numbers (no currency symbols).
CSV DATA:
{csv_content}"""}]
    )

    raw   = message.content[0].text
    start = raw.find("[")
    end   = raw.rfind("]") + 1
    staff_list = json.loads(raw[start:end])

    result = {}
    for s in staff_list:
        name     = (s.get("name") or "").strip()
        services = s.get("services", 0) or 0
        if not name or services == 0:
            continue
        svc_ex_gst = round(services / 1.1, 2)
        bonus      = round(svc_ex_gst * NIGHT_MARKETS_COMMISSION_RATE, 2)
        result[name] = {"services_ex_gst": svc_ex_gst, "bonus": bonus}

    print(f"  Night Markets 50-50 bonus ({len(result)} staff):")
    for name, d in sorted(result.items()):
        print(f"    {name:30s}  svc_ex_gst=${d['services_ex_gst']:.2f}  bonus=${d['bonus']:.2f}")
    return result


# ── Email report ──────────────────────────────────────────────────────────────

def build_sync_email(week_start, week_end, sync_results):
    has_issues = any(r.get("issues") for r in sync_results)

    alert_banner = ""
    if has_issues:
        alert_banner = (
            '<div style="background:#ffebee;border:1px solid #ef9a9a;padding:10px 16px;'
            'margin-bottom:16px;border-radius:4px;color:#c62828;font-size:12px">'
            '<strong>&#9888; Issues detected during this sync — see details below.</strong>'
            '</div>'
        )

    sections = ""
    for r in sync_results:
        label  = r["label"]
        status = r["status"]
        issues = r.get("issues", [])
        staff  = r.get("staff", [])

        status_color = {"ok": "#2e7d32", "partial": "#f57c00", "error": "#c62828"}.get(status, "#666")
        status_text  = {"ok": "OK", "partial": "PARTIAL — some steps failed", "error": "FAILED"}.get(status, status.upper())

        stats_parts = []
        if r.get("hours_pushed") or r.get("hours_errors"):
            stats_parts.append(f"Hours pushed: {r.get('hours_pushed', 0)}")
            if r.get("hours_errors"):
                stats_parts.append(f"{r['hours_errors']} errors")
        if r.get("perf_updated") is not None:
            stats_parts.append(f"Performance updated: {r['perf_updated']}")
            if r.get("perf_skipped"):
                stats_parts.append(f"{r['perf_skipped']} skipped (no GHL record)")
        stats_line = " &nbsp;|&nbsp; ".join(stats_parts) if stats_parts else "No data pushed"

        # Staff tips/commissions table
        if staff:
            has_bonus = any(s.get("bonus") for s in staff)
            staff_rows = ""
            for idx, s in enumerate(sorted(staff, key=lambda x: x.get("name", ""))):
                name  = s.get("name", "")
                tips  = s.get("tips", 0) or 0
                comm  = s.get("commissions", 0) or 0
                svc   = s.get("service_sales_exc_gst", 0) or 0
                bonus = s.get("bonus", 0) or 0
                bg    = "#f9f9f9" if idx % 2 == 0 else "#ffffff"
                bonus_cell = (f'<td style="padding:3px 8px;border-bottom:1px solid #eee;text-align:right">{"$%.2f" % bonus if bonus else "-"}</td>'
                              if has_bonus else "")
                staff_rows += (
                    f'<tr style="background:{bg}">'
                    f'<td style="padding:3px 8px;border-bottom:1px solid #eee">{name}</td>'
                    f'<td style="padding:3px 8px;border-bottom:1px solid #eee;text-align:right">{"$%.2f" % tips if tips else "-"}</td>'
                    f'<td style="padding:3px 8px;border-bottom:1px solid #eee;text-align:right">{"$%.2f" % comm if comm else "-"}</td>'
                    f'<td style="padding:3px 8px;border-bottom:1px solid #eee;text-align:right">{"$%.2f" % svc if svc else "-"}</td>'
                    f'{bonus_cell}'
                    f'</tr>'
                )
            total_tips  = sum(s.get("tips", 0) or 0 for s in staff)
            total_comm  = sum(s.get("commissions", 0) or 0 for s in staff)
            total_svc   = sum(s.get("service_sales_exc_gst", 0) or 0 for s in staff)
            total_bonus = sum(s.get("bonus", 0) or 0 for s in staff)
            bonus_total_cell = (f'<td style="padding:4px 8px;text-align:right">${total_bonus:.2f}</td>'
                                if has_bonus else "")
            staff_rows += (
                '<tr style="background:#1a1a2e;color:#fff;font-weight:600">'
                '<td style="padding:4px 8px">TOTAL</td>'
                f'<td style="padding:4px 8px;text-align:right">${total_tips:.2f}</td>'
                f'<td style="padding:4px 8px;text-align:right">${total_comm:.2f}</td>'
                f'<td style="padding:4px 8px;text-align:right">${total_svc:.2f}</td>'
                f'{bonus_total_cell}'
                '</tr>'
            )
            bonus_header = ('<th style="padding:4px 8px;text-align:right">Night Mkts Bonus</th>'
                            if has_bonus else "")
            staff_table = (
                '<table style="border-collapse:collapse;width:100%;font-size:11px;margin-top:8px">'
                '<thead><tr style="background:#1a1a2e;color:#fff">'
                '<th style="padding:4px 8px;text-align:left">Name</th>'
                '<th style="padding:4px 8px;text-align:right">Tips</th>'
                '<th style="padding:4px 8px;text-align:right">Commission</th>'
                '<th style="padding:4px 8px;text-align:right">Services (ex GST)</th>'
                f'{bonus_header}'
                f'</tr></thead><tbody>{staff_rows}</tbody></table>'
            )
        else:
            staff_table = '<p style="color:#888;font-size:11px;margin:8px 0">No performance data — CSV download may have failed.</p>'

        issues_html = ""
        if issues:
            items = "".join(f"<li style='margin:3px 0'>{i}</li>" for i in issues)
            issues_html = (
                '<div style="margin-top:10px;background:#fff8e1;border-left:3px solid #f9a825;'
                'padding:8px 12px;font-size:11px">'
                f'<strong>Issues ({len(issues)}):</strong>'
                f'<ul style="margin:4px 0;padding-left:18px">{items}</ul>'
                '</div>'
            )

        sections += (
            '<div style="margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid #e0e0e0">'
            f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">'
            f'<h3 style="margin:0;font-size:13px;color:#1a1a2e">{label}</h3>'
            f'<span style="background:{status_color};color:#fff;padding:2px 10px;border-radius:10px;font-size:10px;font-weight:600">{status_text}</span>'
            '</div>'
            f'<p style="margin:2px 0 6px;color:#888;font-size:10px">{stats_line}</p>'
            f'{staff_table}{issues_html}'
            '</div>'
        )

    return (
        '<html><head><meta charset="utf-8"></head>'
        '<body style="font-family:Arial,sans-serif;color:#333;margin:0;padding:20px;max-width:900px">'
        '<h2 style="color:#1a1a2e;margin-bottom:2px;font-size:16px">Diamond Barbers — Weekly Sync Report</h2>'
        f'<p style="color:#888;margin-top:0;margin-bottom:16px;font-size:11px">Week: {week_start} to {week_end}</p>'
        f'{alert_banner}{sections}'
        '</body></html>'
    )


def send_sync_email(html, week_start, week_end, has_issues, csv_files=None):
    email_from = "claude@diamondbarbers.com.au"
    email_to   = "admin@diamondbarbers.com.au"
    password   = os.environ.get("EMAIL_PASSWORD", "")
    host       = os.environ.get("EMAIL_HOST", "mail.diamondbarbers.com.au")

    flag    = " ⚠ ISSUES" if has_issues else ""
    msg     = MIMEMultipart("mixed")
    msg["Subject"] = f"Diamond Barbers Weekly Sync{flag} — {week_start} to {week_end}"
    msg["From"]    = email_from
    msg["To"]      = email_to
    msg.attach(MIMEText(html, "html"))

    for csv_file in csv_files or []:
        csv_path = Path(csv_file)
        if not csv_path.exists():
            continue
        with open(csv_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        email_encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{csv_path.name}"')
        msg.attach(part)

    try:
        with smtplib.SMTP(host, 587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(email_from, password)
            smtp.sendmail(email_from, email_to, msg.as_string())
        print(f"  Sync report emailed to {email_to}")
    except Exception as e:
        print(f"  WARNING: Email failed: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    has_ghl = bool(GHL_API_KEY)
    sync_results = []

    async with async_playwright() as p:
        for account in ACCOUNTS:
            label        = account["label"]
            session_file = account["session"]
            output_file  = account["output"]
            tz           = account["timezone"]

            print(f"\n{'='*60}")
            print(f"ACCOUNT: {label}")
            print(f"{'='*60}")

            acct = {
                "label":        label,
                "status":       "ok",
                "issues":       [],
                "hours_pushed": 0,
                "hours_errors": 0,
                "perf_updated": None,
                "perf_skipped": 0,
                "staff":        [],
                "csv_files":    [],
                "checklist":    [],
            }
            checklist = acct["checklist"]

            if not session_file.exists():
                msg = f"{session_file.name} not found — account skipped"
                print(f"  WARNING: {msg}")
                acct["status"] = "error"
                acct["issues"].append(msg)
                sync_results.append(acct)
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
                await ensure_logged_in(account, page, context, checklist)
            except Exception as e:
                msg = f"Login failed: {e}"
                print(f"  ERROR {msg}")
                acct["status"] = "error"
                acct["issues"].append(msg)
                await browser.close()
                sync_results.append(acct)
                continue

            try:
                hours_data = await fetch_hours(account, context, date_from, date_to)
            except Exception as e:
                msg = f"Hours fetch failed: {e}"
                print(f"  ERROR {msg}")
                acct["issues"].append(msg)
                acct["status"] = "partial"
                hours_data = {}

            # Push hours to GHL payroll records
            if has_ghl and hours_data:
                print(f"\n  Pushing hours to GHL ({len(hours_data)} staff)...")
                ok = err = 0
                for name, h in hours_data.items():
                    try:
                        action = ghl_upsert_payroll(
                            employee_name = name,
                            week_start    = date_from,
                            week_end      = date_to,
                            xero_org      = h["xero_org"],
                            hours         = h,
                        )
                        print(f"    {action:7s}  {name}")
                        ok += 1
                    except Exception as e:
                        print(f"    ERROR {name}: {e}")
                        acct["issues"].append(f"GHL hours push failed for {name}: {e}")
                        err += 1
                acct["hours_pushed"] = ok
                acct["hours_errors"] = err
                if err:
                    acct["status"] = "partial"
                print(f"  Hours: {ok} pushed, {err} errors.")
                checklist.append({
                    "check": "GHL hours push",
                    "status": "OK" if err == 0 else "FAIL",
                    "detail": f"{ok} pushed, {err} errors" if err else None,
                })
            elif not has_ghl:
                print("\n  GHL_API_KEY not set -- skipping GHL hours push.")
                checklist.append({"check": "GHL hours push", "status": "SKIP", "detail": "GHL_API_KEY not set"})

            # Save hours to JSON for xero_create_payrun.py
            if hours_data:
                suffix = "nt" if "NT" in account["label"] else "qld"
                hours_json_path = DATA_DIR / f"fresha_hours_{suffix}.json"
                hours_summary = {}
                for _nm, _h in hours_data.items():
                    wk = sum(_h.get(d, 0) for d in ["monday", "tuesday", "wednesday", "thursday", "friday"])
                    hours_summary[_nm] = {
                        "weekday_hrs": round(wk, 2),
                        "saturday_hrs": round(_h.get("saturday", 0), 2),
                        "sunday_hrs":   round(_h.get("sunday", 0), 2),
                        "total_hrs":    round(_h.get("total", 0), 2),
                    }
                hours_json_path.write_text(json.dumps({
                    "date_from":  date_from,
                    "date_to":    date_to,
                    "generated":  datetime.now(timezone.utc).isoformat(),
                    "summary":    hours_summary,
                }, indent=2))
                print(f"  Saved {hours_json_path.name} ({len(hours_summary)} staff)")

            # ── Step 2: Download performance CSVs (browser navigation) ────────
            try:
                csv_path, loc_csv_path, date_from, date_to = await download_performance_csvs(
                    account, page, context, checklist, date_from, date_to
                )
                acct["csv_files"].append(csv_path)
                if loc_csv_path:
                    acct["csv_files"].append(loc_csv_path)
            except Exception as e:
                msg = f"Performance CSV download failed: {e}"
                print(f"  ERROR {msg}")
                acct["issues"].append(msg)
                acct["status"] = "partial" if acct["hours_pushed"] > 0 else "error"
                screenshot = str(DATA_DIR / f"error_{label.split()[0].lower()}.png")
                await page.screenshot(path=screenshot)
                print(f"  Screenshot saved: {screenshot}")
                await browser.close()
                sync_results.append(acct)
                continue

            # Parse staff CSV
            try:
                perf_data = parse_staff_csv(csv_path, api_key, date_from, date_to)
            except Exception as e:
                msg = f"Staff CSV parsing failed: {e}"
                print(f"  ERROR {msg}")
                acct["issues"].append(msg)
                acct["status"] = "partial" if acct["hours_pushed"] > 0 else "error"
                await browser.close()
                sync_results.append(acct)
                continue

            # Parse location CSV
            locations = []
            if loc_csv_path and Path(loc_csv_path).exists():
                try:
                    locations = parse_location_csv(loc_csv_path, api_key)
                except Exception as e:
                    msg = f"Location CSV parsing failed: {e}"
                    print(f"  ERROR {msg}")
                    acct["issues"].append(msg)
                    if acct["status"] == "ok":
                        acct["status"] = "partial"

            flag_zero_value_issues(perf_data, locations, checklist)

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
                staff_for_email = []
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
                        action = ghl_update_performance(name, date_from, tips, commissions, service_sales_exc_gst, occupancy_rate)
                        if action == "no_record":
                            print(f"    SKIP  {name:30s}  (no payroll record)")
                            acct["issues"].append(f"No GHL record for {name} — tips/commissions not pushed (hours may not have synced)")
                            skipped += 1
                            if acct["status"] == "ok":
                                acct["status"] = "partial"
                        else:
                            print(f"    OK    {name:30s}  tips=${tips:.2f}  comm=${commissions:.2f}")
                            ok += 1
                    except Exception as e:
                        print(f"    ERROR {name}: {e}")
                        acct["issues"].append(f"GHL performance update failed for {name}: {e}")

                    staff_for_email.append({
                        "name":                  name,
                        "tips":                  tips,
                        "commissions":           commissions,
                        "service_sales_exc_gst": service_sales_exc_gst,
                    })

                acct["perf_updated"] = ok
                acct["perf_skipped"] = skipped
                acct["staff"]        = staff_for_email
                print(f"\n  Performance: {ok} updated, {skipped} skipped.")
                checklist.append({
                    "check": "GHL performance (tips/commissions) push",
                    "status": "OK" if skipped == 0 else "FLAG",
                    "detail": f"{ok} updated, {skipped} skipped (no matching payroll record)" if skipped else f"{ok} updated",
                })

                # ── Step 5: Push location_performance to GHL ──────────────────
                if locations:
                    print(f"\n  Pushing {len(locations)} location records to GHL...")
                    lok = 0
                    for loc in locations:
                        loc_name = loc.get("name", "").strip()
                        if not loc_name:
                            continue
                        try:
                            action = ghl_upsert_location(
                                location_name   = loc_name,
                                week_start      = date_from,
                                week_end        = date_to,
                                location_label  = LOCATION_TO_ORG.get(loc_name, loc_name),
                                services_ex_gst = loc.get("services_ex_gst", 0),
                                commissions     = loc.get("location_commissions", 0),
                                occupancy_pct   = loc.get("occupancy_pct", 0),
                            )
                            print(f"    {action:7s}  {loc_name}")
                            lok += 1
                        except Exception as e:
                            print(f"    ERROR {loc_name}: {e}")
                            acct["issues"].append(f"Location GHL push failed for {loc_name}: {e}")
                    print(f"  Location records: {lok}/{len(locations)} pushed.")
                    checklist.append({
                        "check": "GHL location_performance push",
                        "status": "OK" if lok == len(locations) else "FAIL",
                        "detail": f"{lok}/{len(locations)} pushed",
                    })

            # ── Step 6: Night Markets 50-50 bonus (QLD only) ─────────────────
            if has_ghl and account.get("night_markets_loc"):
                print(f"\n  [NIGHT MARKETS] Processing 50-50 service bonus...")
                nm_csv = None
                try:
                    nm_csv = await download_night_markets_csv(
                        account, page, context, checklist, date_from, date_to
                    )
                except Exception as e:
                    checklist.append({
                        "check": "Night Markets: CSV download",
                        "status": "FAIL",
                        "detail": str(e).splitlines()[0][:160],
                    })

                if nm_csv:
                    try:
                        nm_bonuses = parse_night_markets_csv(nm_csv, api_key)
                        # Save for xero_payrun.py to pick up
                        (DATA_DIR / "night_markets_bonus.json").write_text(
                            json.dumps(nm_bonuses, indent=2)
                        )
                        nm_ok = nm_skip = 0
                        for nm_name, nm_data in nm_bonuses.items():
                            try:
                                action = ghl_update_bonus(nm_name, date_from, nm_data["bonus"])
                                if action == "no_record":
                                    print(f"    SKIP  {nm_name:30s}  (no payroll record)")
                                    nm_skip += 1
                                elif action == "no_key":
                                    nm_skip += 1
                                else:
                                    print(f"    OK    {nm_name:30s}  bonus=${nm_data['bonus']:.2f}")
                                    nm_ok += 1
                                    # Attach bonus to staff_for_email if present
                                    for s in acct.get("staff", []):
                                        if s["name"] == nm_name:
                                            s["bonus"] = nm_data["bonus"]
                            except Exception as e:
                                print(f"    ERROR {nm_name}: {e}")
                                acct["issues"].append(f"Night Markets bonus push failed for {nm_name}: {e}")
                        checklist.append({
                            "check": "Night Markets: bonus GHL push",
                            "status": "OK" if nm_skip == 0 else "FLAG",
                            "detail": f"{nm_ok} updated, {nm_skip} skipped",
                        })
                    except Exception as e:
                        checklist.append({
                            "check": "Night Markets: bonus CSV parse",
                            "status": "FAIL",
                            "detail": str(e).splitlines()[0][:160],
                        })

            if acct["status"] == "ok" and any(item["status"] in ("FAIL", "FLAG") for item in checklist):
                acct["status"] = "partial"

            await browser.close()
            sync_results.append(acct)

    print("\nAll accounts processed.")

    # ── Send sync summary email ────────────────────────────────────────────────
    tz       = ACCOUNTS[0]["timezone"]
    today    = datetime.now(tz)
    last_mon = today - timedelta(days=today.weekday() + 7)
    last_sun = last_mon + timedelta(days=6)
    w_start  = last_mon.strftime("%Y-%m-%d")
    w_end    = last_sun.strftime("%Y-%m-%d")

    if sync_results:
        has_issues = any(r.get("issues") for r in sync_results)
        email_html = build_sync_email(w_start, w_end, sync_results)
        csv_files  = [f for r in sync_results for f in r.get("csv_files", [])]
        send_sync_email(email_html, w_start, w_end, has_issues, csv_files)

    # ── Local manual run: commit + push results to GitHub ourselves ───────────
    # (GitHub Actions runs already do this as a separate workflow step under
    # CI=true, so this only fires for runs you trigger by hand on your machine.)
    push_status = None
    if os.environ.get("CI", "false").lower() != "true":
        push_status = git_commit_and_push(
            ["data/performance_summary.json", "data/cairns_performance_summary.json"],
            f"chore: manual weekly sync {datetime.now().strftime('%Y-%m-%d')}",
        )

    print_final_checklist(sync_results, push_status)


if __name__ == "__main__":
    asyncio.run(run())
