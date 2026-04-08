"""
fetch_performance.py
--------------------
Downloads the Fresha Performance Summary CSV from both accounts (NT + QLD),
parses services / products / tips per staff member using Claude AI, and saves
the results for the GHL dashboard and Xero payrun script.

Saves:
  data/performance_summary.json         — NT (Darwin) — read by GHL dashboard + xero_payrun.py
  data/cairns_performance_summary.json  — QLD (Cairns) — read by GHL dashboard + xero_payrun.py

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
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"

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


# ── Fresha navigation + CSV download ──────────────────────────────────────────

async def download_csv(account, page, context):
    session_file = account["session"]
    label        = account["label"]

    print(f"\n  Navigating to Performance Summary ({label})...")
    await page.goto("https://partners.fresha.com/reports", wait_until="networkidle")
    await page.wait_for_timeout(3000)

    # Session expired — do full login
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
    parsed  = urlparse(page.url)
    params  = parse_qs(parsed.query)
    tz      = account["timezone"]
    today   = datetime.now(tz)
    last_mon = today - timedelta(days=today.weekday() + 7)
    last_sun = last_mon + timedelta(days=6)
    date_from = params.get("dateFrom", [last_mon.strftime("%Y-%m-%d")])[0]
    date_to   = params.get("dateTo",   [last_sun.strftime("%Y-%m-%d")])[0]
    print(f"  Date range: {date_from} → {date_to}")

    # Download CSV
    print("  Downloading CSV...")
    async with page.expect_download(timeout=30000) as dl_info:
        await page.get_by_role("button", name="Options").click(timeout=10000)
        await page.wait_for_timeout(1500)
        await page.get_by_role("menuitem", name="CSV").click(timeout=10000)

    download  = await dl_info.value
    csv_path  = DATA_DIR / f"fresha_report_{label.split()[0].lower()}_{datetime.now().strftime('%Y%m%d')}.csv"
    await download.save_as(str(csv_path))
    print(f"  CSV saved: {csv_path.name}")

    # Refresh session
    await context.storage_state(path=str(session_file))

    return str(csv_path), date_from, date_to


# ── Claude AI parsing ──────────────────────────────────────────────────────────

def parse_csv_with_claude(csv_path, api_key, date_from, date_to):
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        csv_content = f.read()

    print("  Parsing CSV with Claude AI...")
    client  = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": f"""This is a CSV export from Fresha's Performance Summary report (grouped by Team member) for last week.

CRITICAL RULES:
1. Ignore any row labelled "Total" — extract ONLY individual named staff members.
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

    # Recalculate totals from individual staff rows — never trust the Total row
    staff = data.get("staff", [])
    if staff:
        data["sales_summary"]["services"]    = round(sum(s.get("services",    0) for s in staff), 2)
        data["sales_summary"]["products"]    = round(sum(s.get("products",    0) for s in staff), 2)
        data["sales_summary"]["tips"]        = round(sum(s.get("tips",        0) for s in staff), 2)
        data["sales_summary"]["total_sales"] = round(sum(s.get("total_sales", 0) for s in staff), 2)
        print(f"  Parsed {len(staff)} staff members. "
              f"Services=${data['sales_summary']['services']:.2f}  "
              f"Products=${data['sales_summary']['products']:.2f}  "
              f"Tips=${data['sales_summary']['tips']:.2f}")

    return data


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    async with async_playwright() as p:
        for account in ACCOUNTS:
            label        = account["label"]
            session_file = account["session"]
            output_file  = account["output"]

            print(f"\n{'='*60}")
            print(f"ACCOUNT: {label}")
            print(f"{'='*60}")

            if not session_file.exists():
                print(f"  WARNING: {session_file.name} not found — skipping.")
                continue

            headless = os.environ.get("CI", "false").lower() == "true"
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(
                storage_state=str(session_file),
                accept_downloads=True,
                viewport={"width": 1280, "height": 800},
            )
            page = await context.new_page()

            try:
                csv_path, date_from, date_to = await download_csv(account, page, context)
            except Exception as e:
                print(f"  ERROR downloading CSV: {e}")
                screenshot = str(DATA_DIR / f"error_{label.split()[0].lower()}.png")
                await page.screenshot(path=screenshot)
                print(f"  Screenshot: {screenshot}")
                await browser.close()
                continue

            try:
                data = parse_csv_with_claude(csv_path, api_key, date_from, date_to)
            except Exception as e:
                print(f"  ERROR parsing CSV: {e}")
                await browser.close()
                continue

            data["report_date"]  = datetime.now().strftime("%Y-%m-%d")
            data["report_type"]  = "performance_summary"

            # Append to history list
            if output_file.exists():
                history = json.loads(output_file.read_text())
                if not isinstance(history, list):
                    history = [history]
            else:
                history = []

            history.append(data)
            output_file.write_text(json.dumps(history, indent=2))
            print(f"  Saved to {output_file.name}")

            await browser.close()

    print("\nDone. Both accounts processed.")


if __name__ == "__main__":
    asyncio.run(run())
