"""
fetch_performance.py
--------------------
Downloads the Fresha Performance Summary CSV from both accounts (NT + QLD),
parses tips and commissions per staff member using Claude AI, and updates
the existing GHL Weekly Payroll records for the same week.

Run AFTER fetch_hours.py so the payroll records already exist.

Run with:  python agent/fetch_performance.py
Requires:  data/session.json        (NT Fresha session)
           data/session_cairns.json (QLD Fresha session)
"""

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import anthropic
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

ACCOUNTS = [
    {
        "label":     "NT (Darwin + Parap)",
        "session":   DATA_DIR / "session.json",
        "email_env": "FRESHA_EMAIL",
        "pass_env":  "FRESHA_PASSWORD",
        "timezone":  timezone(timedelta(hours=9, minutes=30)),
    },
    {
        "label":     "QLD (Cairns)",
        "session":   DATA_DIR / "session_cairns.json",
        "email_env": "CAIRNS_FRESHA_EMAIL",
        "pass_env":  "CAIRNS_FRESHA_PASSWORD",
        "timezone":  timezone(timedelta(hours=10)),
    },
]


# ── Fresha CSV download ────────────────────────────────────────────────────────

async def download_csv(account, page, context):
    session_file = account["session"]
    label        = account["label"]

    print(f"  Navigating to Performance Summary ({label})...")
    await page.goto("https://partners.fresha.com/reports", wait_until="networkidle")
    await page.wait_for_timeout(3000)

    if "/users/sign-in" in page.url:
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
            raise Exception("Login failed after 5 minutes.")

        await context.storage_state(path=str(session_file))
        print("  Session saved.")
        await page.goto("https://partners.fresha.com/reports", wait_until="networkidle")
        await page.wait_for_timeout(3000)

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

    parsed   = urlparse(page.url)
    params   = parse_qs(parsed.query)
    tz       = account["timezone"]
    today    = datetime.now(tz)
    last_mon = today - timedelta(days=today.weekday() + 7)
    last_sun = last_mon + timedelta(days=6)
    date_from = params.get("dateFrom", [last_mon.strftime("%Y-%m-%d")])[0]
    date_to   = params.get("dateTo",   [last_sun.strftime("%Y-%m-%d")])[0]
    print(f"  Date range: {date_from} to {date_to}")

    # Download team member CSV
    print("  Downloading team member CSV...")
    async with page.expect_download(timeout=30000) as dl_info:
        await page.get_by_role("button", name="Options").click(timeout=10000)
        await page.wait_for_timeout(1500)
        await page.get_by_role("menuitem", name="CSV").click(timeout=10000)

    download  = await dl_info.value
    csv_path  = DATA_DIR / f"perf_{label.split()[0].lower()}_{datetime.now().strftime('%Y%m%d')}.csv"
    await download.save_as(str(csv_path))
    print(f"  Team member CSV saved: {csv_path.name}")

    # Switch to Location grouping and download location CSV
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
        loc_csv_path = DATA_DIR / f"perf_loc_{label.split()[0].lower()}_{datetime.now().strftime('%Y%m%d')}.csv"
        await dl2.save_as(str(loc_csv_path))
        print(f"  Location CSV saved: {loc_csv_path.name}")
    except Exception as e:
        print(f"  WARNING: Could not download location CSV: {e}")

    await context.storage_state(path=str(session_file))

    return str(csv_path), str(loc_csv_path) if loc_csv_path else None, date_from, date_to


# ── Claude AI parsing ──────────────────────────────────────────────────────────

def parse_csv(csv_path, api_key, date_from, date_to):
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        csv_content = f.read()

    print("  Parsing CSV with Claude AI...")
    client  = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": f"""This is a CSV export from Fresha's Performance Summary report grouped by Team member.

CRITICAL RULES:
1. Ignore any row labelled "Total" — extract ONLY individual named staff members.
2. Include every named staff member even if they have zero values.
3. "commissions" = the value in any column labelled "Commission" or "Commissions" (use 0 if not present).

Return ONLY valid JSON in this exact structure:
{{
  "staff": [{{
    "name": "Staff Name",
    "services": 0.00,
    "products": 0.00,
    "tips": 0.00,
    "occupancy_pct": 0.0
  }}]
}}

Rules: Return ONLY the JSON. All monetary values as plain numbers (no $ sign).
CSV DATA:
{csv_content}"""}]
    )

    raw   = message.content[0].text
    start = raw.find("{")
    end   = raw.rfind("}") + 1
    data  = json.loads(raw[start:end])
    staff = data.get("staff", [])

    # Calculate derived fields
    for s in staff:
        products    = s.get("products", 0) or 0
        services    = s.get("services", 0) or 0
        s["commissions"]    = round(products * 0.10, 2)
        s["service_sales_exc_gst"] = round(services / 1.1, 2)

    print(f"  Parsed {len(staff)} staff members.")
    for s in staff:
        if s.get("tips", 0) or s.get("commissions", 0) or s.get("service_sales_exc_gst", 0):
            print(f"    {s['name']:30s}  tips=${s.get('tips',0):.2f}  "
                  f"commissions=${s.get('commissions',0):.2f}  "
                  f"svc_ex_gst=${s.get('service_sales_exc_gst',0):.2f}  "
                  f"occupancy={s.get('occupancy_pct',0):.1f}%")
    return staff


# ── GHL update ────────────────────────────────────────────────────────────────

def ghl_update_performance(employee_name, week_start, tips, commissions, service_sales_exc_gst, occupancy_rate):
    """Find the existing GHL payroll record and add tips + commissions."""
    # Filter by name only (GHL DATE fields don't support eq operator)
    # Then match week_start in Python
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
        raise Exception(f"Search failed {r.status_code}: {r.text[:200]}")

    records = [
        rec for rec in r.json().get("records", [])
        if rec.get("properties", {}).get("week_start") == week_start
    ]
    if not records:
        return "no_record"

    record_id = records[0]["id"]

    r = requests.put(
        f"{GHL_BASE}/objects/custom_objects.payroll/records/{record_id}",
        headers=GHL_HEADERS,
        params={"locationId": GHL_LOCATION_ID},
        json={"properties": {"tips": tips, "commissions": commissions, "service_sales_exc_gst": service_sales_exc_gst, "occupancy_rate": occupancy_rate}},
    )
    if r.status_code in (200, 201):
        return "updated"
    raise Exception(f"GHL {r.status_code}: {r.text[:200]}")


# ── Location CSV parsing ──────────────────────────────────────────────────────

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

    # Calculate derived fields
    for loc in locations:
        services = loc.get("services", 0) or 0
        products = loc.get("products", 0) or 0
        loc["services_ex_gst"]      = round(services / 1.1, 2)
        loc["location_commissions"] = round(products * 0.10, 2)

    print(f"  Parsed {len(locations)} locations.")
    for loc in locations:
        print(f"    {loc['name']:35s}  svc_ex_gst=${loc['services_ex_gst']:.2f}  "
              f"commissions=${loc['location_commissions']:.2f}  occupancy={loc.get('occupancy_pct',0):.1f}%")
    return locations


def ghl_upsert_location(location_name, week_start, week_end, location_label, services_ex_gst, commissions, occupancy_pct):
    """Create or update a GHL Location Performance record."""
    # Search for existing record by name
    r = requests.post(
        f"{GHL_BASE}/objects/custom_objects.location_performance/records/search",
        headers=GHL_HEADERS,
        json={
            "locationId": GHL_LOCATION_ID,
            "page":        1,
            "pageLimit":   20,
            "filters": [
                {"field": "properties.location_name", "operator": "eq", "value": location_name},
            ],
        },
    )
    if r.status_code not in (200, 201):
        raise Exception(f"Search failed {r.status_code}: {r.text[:200]}")

    records = [
        rec for rec in r.json().get("records", [])
        if rec.get("properties", {}).get("week_start") == week_start
    ]

    properties = {
        "location_name":       location_name,
        "week_start":          week_start,
        "week_end":            week_end,
        "location":            location_label,
        "services_ex_gst":     services_ex_gst,
        "commissions":         commissions,
        "occupancy_rate":      occupancy_pct,
    }

    if records:
        record_id = records[0]["id"]
        r = requests.put(
            f"{GHL_BASE}/objects/custom_objects.location_performance/records/{record_id}",
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


# ── Location → Xero org label ──────────────────────────────────────────────────

LOCATION_TO_ORG = {
    "Diamond Barbers - Darwin":          "Diamond Barbers Darwin",
    "Diamond Barbers Darwin":            "Diamond Barbers Darwin",
    "Diamond Barbers - Parap":           "Diamond Barbers Parap",
    "Diamond Barbers Parap":             "Diamond Barbers Parap",
    "Diamond Barbers Rising Sun":        "Diamond Barbers Cairns",
    "Diamond Barbers Showgrounds":       "Diamond Barbers Cairns",
    "Diamond Barbers Northern Beaches":  "Diamond Barbers Cairns",
    "Diamond Barbers Night Markets":     "Diamond Barbers Cairns",
    "Diamond Barbers Wulguru":           "Diamond Barbers Cairns",
}


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    async with async_playwright() as p:
        for account in ACCOUNTS:
            label        = account["label"]
            session_file = account["session"]

            print(f"\n{'='*60}")
            print(f"ACCOUNT: {label}")
            print(f"{'='*60}")

            if not session_file.exists():
                print(f"  WARNING: {session_file.name} not found — skipping.")
                continue

            headless = os.environ.get("CI", "false").lower() == "true"
            browser  = await p.chromium.launch(headless=headless)
            context  = await browser.new_context(
                storage_state=str(session_file),
                accept_downloads=True,
                viewport={"width": 1280, "height": 800},
            )
            page = await context.new_page()

            try:
                csv_path, loc_csv_path, date_from, date_to = await download_csv(account, page, context)
            except Exception as e:
                print(f"  ERROR downloading CSV: {e}")
                await browser.close()
                continue

            try:
                staff = parse_csv(csv_path, api_key, date_from, date_to)
            except Exception as e:
                print(f"  ERROR parsing staff CSV: {e}")
                await browser.close()
                continue

            print(f"\n  Pushing performance data to GHL (week {date_from})...")
            ok = skipped = 0
            for s in staff:
                name                  = s.get("name", "").strip()
                tips                  = s.get("tips", 0) or 0
                commissions           = s.get("commissions", 0) or 0
                service_sales_exc_gst = s.get("service_sales_exc_gst", 0) or 0
                occupancy_rate        = s.get("occupancy_pct", 0) or 0
                if not name:
                    continue
                try:
                    result = ghl_update_performance(name, date_from, tips, commissions, service_sales_exc_gst, occupancy_rate)
                    if result == "no_record":
                        print(f"    SKIP  {name:30s}  (no payroll record for this week)")
                        skipped += 1
                    else:
                        print(f"    OK    {name:30s}  tips=${tips:.2f}  comm=${commissions:.2f}  "
                              f"svc_ex_gst=${service_sales_exc_gst:.2f}  occupancy={occupancy_rate:.1f}%")
                        ok += 1
                except Exception as e:
                    print(f"    ERROR {name}: {e}")

            print(f"\n  Done — {ok} updated, {skipped} skipped (no hours record).")

            # ── Location Performance ───────────────────────────────────────────
            if loc_csv_path and Path(loc_csv_path).exists():
                try:
                    locations = parse_location_csv(loc_csv_path, api_key)
                except Exception as e:
                    print(f"  ERROR parsing location CSV: {e}")
                    locations = []

                print(f"\n  Pushing {len(locations)} location records to GHL...")
                lok = 0
                for loc in locations:
                    loc_name = loc.get("name", "").strip()
                    if not loc_name:
                        continue
                    loc_label = LOCATION_TO_ORG.get(loc_name, loc_name)
                    try:
                        result = ghl_upsert_location(
                            location_name  = loc_name,
                            week_start     = date_from,
                            week_end       = date_to,
                            location_label = loc_label,
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
