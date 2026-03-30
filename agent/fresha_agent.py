import asyncio
import os
import json
from datetime import datetime
from pathlib import Path

import anthropic
from playwright.async_api import async_playwright
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

SESSION_FILE = DATA_DIR / "session.json"


async def download_csvs(email, password):
    csv_path = None
    location_csv_path = None
    date_from = None
    date_to = None
    async with async_playwright() as p:
        headless = os.environ.get("CI", "false").lower() == "true"
        browser = await p.chromium.launch(headless=headless)

        # Load saved session if it exists (skips login + 2FA)
        if SESSION_FILE.exists():
            print("Loading saved session...")
            context = await browser.new_context(
                storage_state=str(SESSION_FILE),
                accept_downloads=True,
                viewport={"width": 1280, "height": 800}
            )
        else:
            print("No saved session. Will do full login.")
            context = await browser.new_context(
                accept_downloads=True,
                viewport={"width": 1280, "height": 800}
            )

        page = await context.new_page()
        try:
            # Try going directly to reports first (works if session is valid)
            print("Going to reports page...")
            await page.goto("https://partners.fresha.com/reports", wait_until="networkidle")
            await page.wait_for_timeout(3000)

            # If redirected to sign-in, session expired — do full login
            if "/users/sign-in" in page.url:
                print("Session expired or not found. Logging in...")
                SESSION_FILE.unlink(missing_ok=True)

                # Dismiss cookie banner if present
                try:
                    await page.get_by_role("button", name="Accept all").click(timeout=5000)
                    print("Dismissed cookie banner.")
                    await page.wait_for_timeout(1000)
                except Exception:
                    pass

                # Enter email
                print("Entering email...")
                email_field = page.locator('input[placeholder="Enter your email address"]')
                await email_field.wait_for(timeout=10000)
                await email_field.click()
                await email_field.type(email, delay=50)
                await page.wait_for_timeout(1000)

                # Click Continue
                print("Clicking Continue...")
                await page.click('[data-qa="continue"]', force=True)
                await page.wait_for_selector('input[type="password"]:not([tabindex="-1"])', timeout=15000)
                await page.wait_for_timeout(1000)

                # Enter password
                print("Entering password...")
                pwd_field = page.locator('input[type="password"]:not([tabindex="-1"])')
                await pwd_field.fill(password)
                await page.wait_for_timeout(1000)

                # Submit
                print("Submitting login...")
                try:
                    await page.locator('button[type="submit"]').click(force=True, timeout=5000)
                except Exception:
                    try:
                        await page.get_by_role("button", name="Log in").click(force=True, timeout=5000)
                    except Exception:
                        await page.keyboard.press("Enter")

                # Wait up to 90 seconds — enter 2FA code in the browser if prompted
                print("==============================================")
                print("CHECK THE BROWSER WINDOW NOW.")
                print("Enter the 2FA code sent to your phone.")
                print("You have 5 minutes.")
                print("==============================================")
                try:
                    await page.wait_for_url(
                        lambda url: "/users/sign-in" not in url,
                        timeout=300000
                    )
                except Exception:
                    pass

                if "/users/sign-in" in page.url:
                    raise Exception("Login failed after 5 minutes.")

                # Save session so next run skips login
                await context.storage_state(path=str(SESSION_FILE))
                print("Session saved. Future runs will skip login and 2FA.")

                # Now go to reports
                await page.goto("https://partners.fresha.com/reports", wait_until="networkidle")
                await page.wait_for_timeout(3000)

            # Navigate directly to Performance Summary report
            print("Navigating to Performance Summary...")
            await page.goto("https://partners.fresha.com/reports/table/performance-summary", wait_until="networkidle")
            await page.wait_for_timeout(4000)
            print(f"Performance Summary URL: {page.url}")

            # Step 1: Click the "Month to date" filter chip to open the date popup
            print("Opening date range popup...")
            await page.get_by_text("Month to date", exact=True).first.click(timeout=10000)
            await page.wait_for_timeout(1000)

            # Step 2: Select "Last week" from the native <select> inside the popup
            print("Selecting Last week...")
            await page.locator('select:has(option[value="last_week"])').select_option(value="last_week")
            await page.wait_for_timeout(1000)

            # Step 3: Click Apply if present
            print("Clicking Apply...")
            try:
                await page.get_by_role("button", name="Apply").click(timeout=5000)
                print("Clicked Apply.")
            except Exception:
                print("No Apply button found — continuing.")

            # Wait for Fresha to reload data for the new date range
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(10000)
            print(f"Final URL: {page.url}")

            # Reload the page using the confirmed URL so the CSV export uses the correct dates
            confirmed_url = page.url
            print(f"Reloading confirmed URL to refresh CSV export: {confirmed_url}")
            await page.goto(confirmed_url, wait_until="networkidle")
            await page.wait_for_timeout(8000)

            # Take a screenshot to verify correct data is on screen before downloading
            pre_download_screenshot = str(DATA_DIR / "pre_download.png")
            await page.screenshot(path=pre_download_screenshot)
            print(f"Pre-download screenshot saved to {pre_download_screenshot}")

            # Grab dates from the URL for the JSON record
            from urllib.parse import urlparse, parse_qs
            from datetime import timedelta, timezone
            parsed = urlparse(page.url)
            params = parse_qs(parsed.query)
            date_from = params.get("dateFrom", [None])[0]
            date_to = params.get("dateTo", [None])[0]
            print(f"Date range from URL: {date_from} → {date_to}")

            # Fallback to Darwin timezone calculation if URL doesn't include dates
            if not date_from or not date_to:
                print("URL has no dates — using Darwin timezone fallback.")
                DARWIN_TZ = timezone(timedelta(hours=9, minutes=30))
                today = datetime.now(DARWIN_TZ)
                days_since_monday = today.weekday()
                last_monday = today - timedelta(days=days_since_monday + 7)
                last_sunday = last_monday + timedelta(days=6)
                date_from = last_monday.strftime("%Y-%m-%d")
                date_to = last_sunday.strftime("%Y-%m-%d")
                print(f"Fallback date range: {date_from} → {date_to}")

            # ── Download Team Member CSV ─────────────────────────────────────
            print("Downloading Team Member CSV...")
            async with page.expect_download(timeout=30000) as download_info:
                await page.get_by_role("button", name="Options").click(timeout=10000)
                await page.wait_for_timeout(1500)
                await page.get_by_role("menuitem", name="CSV").click(timeout=10000)
                print("Clicked CSV menuitem.")
            download = await download_info.value
            csv_path = str(DATA_DIR / f"fresha_report_{datetime.now().strftime('%Y%m%d')}.csv")
            await download.save_as(csv_path)
            print(f"Team Member CSV saved to: {csv_path}")

            # ── Switch to Location grouping and download Location CSV ────────
            print("Switching group by to Location...")
            try:
                # Click the "Team member" button to open the group-by dropdown
                try:
                    await page.get_by_role("button", name="Team member").click(timeout=8000)
                except Exception:
                    await page.get_by_text("Team member").first.click(timeout=8000)
                await page.wait_for_timeout(1000)

                # Click the "Location" option in the dropdown
                await page.get_by_text("Location", exact=True).click(timeout=5000)
                await page.wait_for_timeout(1000)

                # Wait for data to reload
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(6000)
                print(f"Location URL: {page.url}")

                # Take screenshot to verify
                location_screenshot = str(DATA_DIR / "location_pre_download.png")
                await page.screenshot(path=location_screenshot)
                print(f"Location screenshot saved to {location_screenshot}")

                # Download location CSV
                print("Downloading Location CSV...")
                async with page.expect_download(timeout=30000) as dl_info:
                    await page.get_by_role("button", name="Options").click(timeout=10000)
                    await page.wait_for_timeout(1500)
                    await page.get_by_role("menuitem", name="CSV").click(timeout=10000)
                    print("Clicked CSV menuitem (location).")
                dl = await dl_info.value
                location_csv_path = str(DATA_DIR / f"fresha_location_{datetime.now().strftime('%Y%m%d')}.csv")
                await dl.save_as(location_csv_path)
                print(f"Location CSV saved to: {location_csv_path}")
            except Exception as e:
                print(f"WARNING: Could not download Location CSV: {e}")
                loc_screenshot = str(DATA_DIR / "location_error.png")
                try:
                    await page.screenshot(path=loc_screenshot)
                    print(f"Location error screenshot: {loc_screenshot}")
                except Exception:
                    pass

            # Always save the refreshed session after a successful run
            await context.storage_state(path=str(SESSION_FILE))
            print("Session refreshed and saved.")

        except Exception as e:
            print(f"ERROR during navigation: {e}")
            screenshot_path = str(DATA_DIR / "error_screenshot.png")
            await page.screenshot(path=screenshot_path)
            print(f"Screenshot saved to {screenshot_path} for debugging")
        finally:
            await browser.close()
    return csv_path, location_csv_path, date_from, date_to


def extract_data_from_csv(csv_path, api_key, date_from=None, date_to=None):
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        csv_content = f.read()

    print("=== FIRST 2000 CHARS OF CSV ===")
    print(csv_content[:2000])
    print("=== END CSV PREVIEW ===")

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": f"""This is a CSV export from Fresha's Performance Summary report (grouped by Team member) for last week.

CRITICAL RULES:
1. The CSV contains a "Total" or summary row — IGNORE IT COMPLETELY. Do not use any row labelled "Total".
2. Extract ONLY individual named staff members (expect ~20 or more people).
3. For all sales_summary fields (services, products, tips, etc.) — SUM the values from all individual staff rows. Do NOT copy from the Total row.
4. Include EVERY named staff member in the staff array, even if they have zero sales.

Return ONLY a valid JSON object in this exact structure:
{{
  "period_start": "YYYY-MM-DD",
  "period_end": "YYYY-MM-DD",
  "sales_summary": {{
    "services": 0.00, "service_addons": 0.00, "products": 0.00,
    "memberships": 0.00, "late_cancellation_fees": 0.00, "no_show_fees": 0.00,
    "total_sales": 0.00, "service_charges": 0.00, "tips": 0.00, "total_sales_and_other": 0.00
  }},
  "appointments": {{
    "total": 0, "online": 0, "offline": 0, "cancelled": 0, "no_shows": 0,
    "pct_online": 0.0, "pct_cancelled": 0.0, "pct_no_show": 0.0
  }},
  "sales_performance": {{
    "services_sold": 0, "avg_service_value": 0.00, "products_sold": 0, "avg_product_value": 0.00
  }},
  "upsell": {{"total": 0.00, "pct": 0.0}},
  "staff": [{{
    "name": "Staff Name", "services": 0.00, "products": 0.00,
    "total_sales": 0.00, "tips": 0.00, "total_appts": 0,
    "cancelled_appts": 0, "no_show_appts": 0, "services_sold": 0,
    "occupancy_pct": 0.0
  }}]
}}
Rules: Return ONLY the JSON. All monetary values as plain numbers. occupancy_pct is the "% Occupancy" column as a percentage number (e.g. 72.5 not 0.725).
CSV DATA:
{csv_content}"""}]
    )
    raw = message.content[0].text
    start = raw.find("{")
    end = raw.rfind("}") + 1
    result = json.loads(raw[start:end])
    if date_from:
        result["period_start"] = date_from
    if date_to:
        result["period_end"] = date_to

    # Override sales_summary totals by summing individual staff rows in Python.
    # Claude consistently reads the cached Total row — this ensures correct values.
    staff = result.get("staff", [])
    if staff:
        result["sales_summary"]["services"]   = round(sum(s.get("services",   0) for s in staff), 2)
        result["sales_summary"]["products"]   = round(sum(s.get("products",   0) for s in staff), 2)
        result["sales_summary"]["tips"]       = round(sum(s.get("tips",       0) for s in staff), 2)
        result["sales_summary"]["total_sales"]= round(sum(s.get("total_sales",0) for s in staff), 2)
        print(f"Recalculated from staff rows: services={result['sales_summary']['services']}, products={result['sales_summary']['products']}, tips={result['sales_summary']['tips']}, total_sales={result['sales_summary']['total_sales']}")

    return result


def extract_location_data_from_csv(csv_path, api_key):
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        csv_content = f.read()

    print("=== FIRST 1000 CHARS OF LOCATION CSV ===")
    print(csv_content[:1000])
    print("=== END LOCATION CSV PREVIEW ===")

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": f"""This is a CSV export from Fresha's Performance Summary report grouped by Location.

CRITICAL: Ignore any "Total" or summary rows. Extract ONLY individual named locations.

Return ONLY a valid JSON array like this:
[
  {{
    "name": "Location Name",
    "services": 0.00,
    "products": 0.00,
    "total_sales": 0.00,
    "tips": 0.00,
    "total_appts": 0,
    "occupancy_pct": 0.0
  }}
]

Rules: Return ONLY the JSON array. All monetary values as plain numbers. occupancy_pct as a percentage (e.g. 72.5 not 0.725).
CSV DATA:
{csv_content}"""}]
    )
    raw = message.content[0].text
    start = raw.find("[")
    end = raw.rfind("]") + 1
    return json.loads(raw[start:end])


async def run():
    email = os.environ["FRESHA_EMAIL"]
    password = os.environ["FRESHA_PASSWORD"]
    api_key = os.environ["ANTHROPIC_API_KEY"]

    print(f"[{datetime.now()}] Starting Fresha data extraction...")
    csv_path, location_csv_path, date_from, date_to = await download_csvs(email, password)

    if not csv_path or not Path(csv_path).exists():
        print("ERROR: CSV was not downloaded.")
        data = {"error": "CSV download failed"}
    else:
        print(f"[{datetime.now()}] Extracting staff data from CSV using Claude...")
        try:
            data = extract_data_from_csv(csv_path, api_key, date_from, date_to)
            data["report_type"] = "performance_summary"
            print("Staff data extracted successfully.")
        except Exception as e:
            import traceback
            print(f"ERROR extracting data: {e}")
            traceback.print_exc()
            data = {"error": str(e)}

    # Extract location data if available
    if location_csv_path and Path(location_csv_path).exists():
        print(f"[{datetime.now()}] Extracting location data from CSV using Claude...")
        try:
            locations = extract_location_data_from_csv(location_csv_path, api_key)
            data["locations"] = locations
            print(f"Location data extracted: {len(locations)} locations.")
        except Exception as e:
            import traceback
            print(f"WARNING: Could not extract location data: {e}")
            traceback.print_exc()

    data["report_date"] = datetime.now().strftime("%Y-%m-%d")

    output_file = DATA_DIR / "performance_summary.json"
    if output_file.exists():
        with open(output_file, "r") as f:
            history = json.load(f)
        if not isinstance(history, list):
            history = [history]
    else:
        history = []

    history.append(data)

    with open(output_file, "w") as f:
        json.dump(history, f, indent=2)

    print(f"[{datetime.now()}] Saved to {output_file}")
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    asyncio.run(run())
