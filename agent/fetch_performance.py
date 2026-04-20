"""
fetch_performance.py
--------------------
Downloads the Fresha Performance Summary CSV from both accounts (NT + QLD),
parses services / products / tips per staff member using Claude AI, saves
JSON history for the dashboard, AND pushes data to GHL custom objects.

Saves:
  data/performance_summary.json         -- NT (Darwin) -- dashboard + xero_payrun.py
  data/cairns_performance_summary.json  -- QLD (Cairns) -- dashboard + xero_payrun.py

Pushes to GHL:
  custom_objects.payroll                -- tips, commissions, service_sales_exc_gst, occupancy_rate
  custom_objects.location_performance   -- services_ex_gst, commissions, occupancy_rate

Run with:  python agent/fetch_performance.py
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
        "label":       "NT (Darwin)",
        "session":     DATA_DIR / "session.json",
        "email_env":   "FRESHA_EMAIL",
        "pass_env":    "FRESHA_PASSWORD",
        "timezone":    timezone(timedelta(hours=9, minutes=30)),
        "output":      DATA_DIR / "performance_summary.json",
    },
    {
        "label":       "QLD (Cairns)",
        "session":     DATA_DIR / "session_cairns.json",
        "email_env":   "CAIRNS_FRESHA_EMAIL",
        "pass_env":    "CAIRNS_FRESHA_PASSWORD",
        "timezone":    timezone(timedelta(hours=10)),
        "output":      DATA_DIR / "cairns_performance_summary.json",
    },
]


# ── Manager commission overrides ──────────────────────────────────────────────
# Each manager's commission = sum of their location(s) product sales * 0.9 * 0.10

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


LOCATION_TO_ORG = {
    "Diamond Barbers - Darwin":         "Diamond Barbers Darwin",
    "Diamond Barbers Darwin":           "Diamond Barbers Darwin",
    "Diamond Barbers - Parap":          "Diamond Barbers Parap",
    "Diamond Barbers Parap":            "Diamond Barbers Parap",
    "Diamond Barbers Rising Sun":       "Diamond Barbers Cairns",
    "Diamond Barbers Showgrounds":      "Diamond Barbers Cairns",
    "Diamond Barbers Northern Beaches": "Diamond Barbers Cairns",
    "Diamond Barbers Night Markets":    "Diamond Barbers Cairns",
    "Diamond Barbers Wulguru":          "Diamond Barbers Cairns",
}


# ── Fresha navigation + CSV download ──────────────────────────────────────────

async def download_csv(account, page, context):
    session_file = account["session"]
    label        = account["label"]

    print(f"\n  Navigating to Performance Summary ({label})...")
    await page.goto("https://partners.fresha.com/reports", wait_until="networkidle")
    await page.wait_for_timeout(3000)

    # Session expired -- do full login
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

    # Navigate to Performance Summary
    await page.goto(
        "https://partners.fresha.com/reports/table/performance-summary",
        wait_until="networkidle"
    )
    await page.wait_for_timeout(4000)

    # Select "Last week"
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

    # Reload confirmed URL so CSV export uses the correct dates
    confirmed_url = page.url
    await page.goto(confirmed_url, wait_until="networkidle")
    await page.wait_for_timeout(8000)

    # Grab date range from URL
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

    download = await dl_info.value
    csv_path = DATA_DIR / f"fresha_report_{label.split()[0].lower()}_{datetime.now().strftime('%Y%m%d')}.csv"
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
        loc_csv_path = DATA_DIR / f"fresha_location_{label.split()[0].lower()}_{datetime.now().strftime('%Y%m%d')}.csv"
        await dl2.save_as(str(loc_csv_path))
        print(f"  Location CSV saved: {loc_csv_path.name}")
    except Exception as e:
        print(f"  WARNING: Could not download location CSV: {e}")

    # Refresh session
    await context.storage_state(path=str(session_file))

    return str(csv_path), str(loc_csv_path) if loc_csv_path else None, date_from, date_to


# ── Claude AI parsing (staff) ──────────────────────────────────────────────────

def parse_csv_with_claude(csv_path, api_key, date_from, date_to):
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

    # Always override dates from URL (more reliable than Claude's guess)
    data["period_start"] = date_from
    data["period_end"]   = date_to

    # Recalculate totals from individual staff rows -- never trust the Total row
    staff = data.get("staff", [])
    if staff:
        data["sales_summary"]["services"]    = round(sum(s.get("services",    0) for s in staff), 2)
        data["sales_summary"]["products"]    = round(sum(s.get("products",    0) for s in staff), 2)
        data["sales_summary"]["tips"]        = round(sum(s.get("tips",        0) for s in staff), 2)
        data["sales_summary"]["total_sales"] = round(sum(s.get("total_sales", 0) for s in staff), 2)

    # Add derived fields for GHL payroll push
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


# ── Claude AI parsing (locations) ─────────────────────────────────────────────

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

    # Add derived fields
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


# ── GHL helpers ───────────────────────────────────────────────────────────────

def ghl_update_performance(employee_name, week_start, tips, commissions, service_sales_exc_gst, occupancy_rate):
    """Find the existing GHL payroll record and update tips + commissions."""
    if not GHL_API_KEY:
        return "no_key"

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
    """Create or update a GHL Location Performance record."""
    if not GHL_API_KEY:
        return "no_key"

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
        "location_name":   location_name,
        "week_start":      week_start,
        "week_end":        week_end,
        "location":        location_label,
        "services_ex_gst": services_ex_gst,
        "commissions":     commissions,
        "occupancy_rate":  occupancy_pct,
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


# ── Weekly report ─────────────────────────────────────────────────────────────

def fetch_report_data(week_start):
    if not GHL_API_KEY:
        return []
    r = requests.post(
        f"{GHL_BASE}/objects/custom_objects.payroll/records/search",
        headers=GHL_HEADERS,
        json={"locationId": GHL_LOCATION_ID, "page": 1, "pageLimit": 100},
    )
    records = r.json().get("records", [])
    return [
        rec for rec in records
        if rec.get("properties", {}).get("week_start") == week_start
    ]


def build_report_html(week_start, week_end, records):
    rows   = ""
    totals = {k: 0.0 for k in ["monday","tuesday","wednesday","thursday","friday","saturday","sunday","public_holiday","tips","commissions","total_hours"]}

    for i, rec in enumerate(sorted(records, key=lambda r: r.get("properties", {}).get("employee_name", ""))):
        p   = rec.get("properties", {})
        bg  = "#f5f5f5" if i % 2 == 0 else "#ffffff"
        mon  = float(p.get("monday_hours",         0) or 0)
        tue  = float(p.get("tuesday_hours",        0) or 0)
        wed  = float(p.get("wednesday_hours",      0) or 0)
        thu  = float(p.get("thursday_hours",       0) or 0)
        fri  = float(p.get("friday_hours",         0) or 0)
        sat  = float(p.get("saturday_hours",       0) or 0)
        sun  = float(p.get("sunday_hours",         0) or 0)
        ph   = float(p.get("public_holiday_hours", 0) or 0)
        tips = float(p.get("tips",                 0) or 0)
        comm = float(p.get("commissions",          0) or 0)
        total = float(p.get("total_hours",         0) or 0)

        for k, v in [("monday",mon),("tuesday",tue),("wednesday",wed),("thursday",thu),
                     ("friday",fri),("saturday",sat),("sunday",sun),("public_holiday",ph),
                     ("tips",tips),("commissions",comm),("total_hours",total)]:
            totals[k] += v

        def h(v): return f"{v:.2f}h" if v > 0 else "-"
        def d(v): return f"${v:.2f}" if v > 0 else "-"

        rows += f"""<tr style="background:{bg}">
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0">{p.get("employee_name","")}</td>
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{h(mon)}</td>
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{h(tue)}</td>
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{h(wed)}</td>
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{h(thu)}</td>
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{h(fri)}</td>
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{h(sat)}</td>
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{h(sun)}</td>
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{h(ph)}</td>
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{h(total)}</td>
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{d(tips)}</td>
          <td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{d(comm)}</td>
        </tr>"""

    def h(v): return f"{v:.2f}h" if v > 0 else "-"
    def d(v): return f"${v:.2f}" if v > 0 else "-"

    totals_row = f"""<tr style="background:#1a1a2e;color:#ffffff;font-weight:600">
      <td style="padding:5px 8px">TOTAL</td>
      <td style="padding:5px 8px;text-align:right">{h(totals['monday'])}</td>
      <td style="padding:5px 8px;text-align:right">{h(totals['tuesday'])}</td>
      <td style="padding:5px 8px;text-align:right">{h(totals['wednesday'])}</td>
      <td style="padding:5px 8px;text-align:right">{h(totals['thursday'])}</td>
      <td style="padding:5px 8px;text-align:right">{h(totals['friday'])}</td>
      <td style="padding:5px 8px;text-align:right">{h(totals['saturday'])}</td>
      <td style="padding:5px 8px;text-align:right">{h(totals['sunday'])}</td>
      <td style="padding:5px 8px;text-align:right">{h(totals['public_holiday'])}</td>
      <td style="padding:5px 8px;text-align:right">{h(totals['total_hours'])}</td>
      <td style="padding:5px 8px;text-align:right">{d(totals['tips'])}</td>
      <td style="padding:5px 8px;text-align:right">{d(totals['commissions'])}</td>
    </tr>"""

    return f"""<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;color:#333;margin:0;padding:16px">
<h2 style="color:#1a1a2e;margin-bottom:2px;font-size:15px">Diamond Barbers -- Weekly Payroll Summary</h2>
<p style="color:#888;margin-top:0;margin-bottom:12px;font-size:11px">{week_start} to {week_end}</p>
<table style="border-collapse:collapse;width:100%;font-size:9px">
  <thead>
    <tr style="background:#1a1a2e;color:#ffffff">
      <th style="padding:5px 8px;text-align:left">Name</th>
      <th style="padding:5px 8px;text-align:right">Mon</th>
      <th style="padding:5px 8px;text-align:right">Tue</th>
      <th style="padding:5px 8px;text-align:right">Wed</th>
      <th style="padding:5px 8px;text-align:right">Thu</th>
      <th style="padding:5px 8px;text-align:right">Fri</th>
      <th style="padding:5px 8px;text-align:right">Sat</th>
      <th style="padding:5px 8px;text-align:right">Sun</th>
      <th style="padding:5px 8px;text-align:right">PH</th>
      <th style="padding:5px 8px;text-align:right">Total</th>
      <th style="padding:5px 8px;text-align:right">Tips</th>
      <th style="padding:5px 8px;text-align:right">Commission</th>
    </tr>
  </thead>
  <tbody>{rows}{totals_row}</tbody>
</table>
</body></html>"""


async def send_weekly_report(week_start, week_end, playwright_instance):
    """Generate PDF from GHL data and email it."""
    if not GHL_API_KEY:
        print("  Skipping report -- GHL_API_KEY not set.")
        return

    print("\nGenerating weekly payroll report...")
    records = fetch_report_data(week_start)
    if not records:
        print("  No records found for report.")
        return

    html = build_report_html(week_start, week_end, records)

    browser   = await playwright_instance.chromium.launch(headless=True)
    page      = await browser.new_page()
    await page.set_content(html, wait_until="load")
    pdf_bytes = await page.pdf(
        format="A4",
        landscape=True,
        margin={"top": "10mm", "right": "10mm", "bottom": "10mm", "left": "10mm"},
    )
    await browser.close()

    pdf_path = DATA_DIR / f"payroll_report_{week_start}.pdf"
    pdf_path.write_bytes(pdf_bytes)
    print(f"  PDF saved: {pdf_path.name}")

    email_from = "claude@diamondbarbers.com.au"
    email_to   = "admin@diamondbarbers.com.au"
    password   = os.environ.get("EMAIL_PASSWORD", "")
    host       = os.environ.get("EMAIL_HOST", "mail.diamondbarbers.com.au")

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"Diamond Barbers Weekly Payroll -- {week_start} to {week_end}"
    msg["From"]    = email_from
    msg["To"]      = email_to
    msg.attach(MIMEText(html, "html"))

    with open(pdf_path, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
    email_encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{pdf_path.name}"')
    msg.attach(part)

    try:
        with smtplib.SMTP(host, 587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(email_from, password)
            smtp.sendmail(email_from, email_to, msg.as_string())
        print(f"  Report emailed to {email_to}")
    except Exception as e:
        print(f"  WARNING: Email failed: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    api_key    = os.environ.get("ANTHROPIC_API_KEY", "")
    has_ghl    = bool(GHL_API_KEY)

    async with async_playwright() as p:
        for account in ACCOUNTS:
            label        = account["label"]
            session_file = account["session"]
            output_file  = account["output"]

            print(f"\n{'='*60}")
            print(f"ACCOUNT: {label}")
            print(f"{'='*60}")

            if not session_file.exists():
                print(f"  WARNING: {session_file.name} not found -- skipping.")
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
                screenshot = str(DATA_DIR / f"error_{label.split()[0].lower()}.png")
                await page.screenshot(path=screenshot)
                print(f"  Screenshot: {screenshot}")
                await browser.close()
                continue

            # Parse staff CSV
            try:
                data = parse_csv_with_claude(csv_path, api_key, date_from, date_to)
            except Exception as e:
                print(f"  ERROR parsing CSV: {e}")
                await browser.close()
                continue

            # Parse location CSV
            locations = []
            if loc_csv_path and Path(loc_csv_path).exists():
                try:
                    locations = parse_location_csv(loc_csv_path, api_key)
                except Exception as e:
                    print(f"  ERROR parsing location CSV: {e}")

            # ── Save JSON history for dashboard ───────────────────────────────
            data["report_date"] = datetime.now().strftime("%Y-%m-%d")
            data["report_type"] = "performance_summary"

            if output_file.exists():
                history = json.loads(output_file.read_text())
                if not isinstance(history, list):
                    history = [history]
            else:
                history = []

            history.append(data)
            output_file.write_text(json.dumps(history, indent=2))
            print(f"  Saved to {output_file.name}")

            # ── Push staff data to GHL payroll records ─────────────────────────
            if has_ghl:
                loc_products = {loc["name"]: loc.get("products", 0) or 0 for loc in locations}

                print(f"\n  Pushing performance data to GHL (week {date_from})...")
                ok = skipped = 0
                for s in data.get("staff", []):
                    name                  = s.get("name", "").strip()
                    tips                  = s.get("tips", 0) or 0
                    commissions           = s.get("commissions", 0) or 0
                    service_sales_exc_gst = s.get("service_sales_exc_gst", 0) or 0
                    occupancy_rate        = s.get("occupancy_pct", 0) or 0
                    if not name:
                        continue

                    # Override commission for managers based on location product sales
                    if name in MANAGER_LOCATIONS:
                        total_products = sum(loc_products.get(loc, 0) for loc in MANAGER_LOCATIONS[name])
                        commissions    = round(total_products * 0.9 * 0.10, 2)
                        print(f"    MANAGER {name:28s}  location_products=${total_products:.2f}  comm=${commissions:.2f}")

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

                print(f"\n  Done -- {ok} updated, {skipped} skipped.")

                # ── Push location data to GHL location_performance records ────
                if locations:
                    print(f"\n  Pushing {len(locations)} location records to GHL...")
                    lok = 0
                    for loc in locations:
                        loc_name = loc.get("name", "").strip()
                        if not loc_name:
                            continue
                        loc_label = LOCATION_TO_ORG.get(loc_name, loc_name)
                        try:
                            result = ghl_upsert_location(
                                location_name   = loc_name,
                                week_start      = date_from,
                                week_end        = date_to,
                                location_label  = loc_label,
                                services_ex_gst = loc.get("services_ex_gst", 0),
                                commissions     = loc.get("location_commissions", 0),
                                occupancy_pct   = loc.get("occupancy_pct", 0),
                            )
                            print(f"    {result:7s}  {loc_name}")
                            lok += 1
                        except Exception as e:
                            print(f"    ERROR {loc_name}: {e}")
                    print(f"  Location records: {lok}/{len(locations)} pushed.")
            else:
                print("\n  GHL_API_KEY not set -- skipping GHL push.")

            await browser.close()

        print("\nAll accounts processed.")

        # ── Send weekly payroll report ─────────────────────────────────────────
        try:
            tz       = ACCOUNTS[-1]["timezone"]
            today    = datetime.now(tz)
            last_mon = today - timedelta(days=today.weekday() + 7)
            last_sun = last_mon + timedelta(days=6)
            await send_weekly_report(
                last_mon.strftime("%Y-%m-%d"),
                last_sun.strftime("%Y-%m-%d"),
                p,
            )
        except Exception as e:
            print(f"WARNING: Report failed: {e}")


if __name__ == "__main__":
    asyncio.run(run())
