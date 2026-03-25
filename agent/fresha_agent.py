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


async def download_csv(email, password):
    csv_path = None
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

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

            # Calculate last week's Monday and Sunday
            from datetime import timedelta
            today = datetime.now()
            days_since_monday = today.weekday()
            last_monday = today - timedelta(days=days_since_monday + 7)
            last_sunday = last_monday + timedelta(days=6)
            date_from = last_monday.strftime("%Y-%m-%d")
            date_to = last_sunday.strftime("%Y-%m-%d")

            # Navigate directly to Performance Summary with last week filter pre-applied
            report_url = f"https://partners.fresha.com/reports/table/performance-summary?shortcut=last_week&dateFrom={date_from}&dateTo={date_to}"
            print(f"Navigating to: {report_url}")
            await page.goto(report_url, wait_until="networkidle")
            await page.wait_for_timeout(3000)
            print(f"Performance Summary URL: {page.url}")

            print("Downloading CSV...")
            print("Downloading CSV...")
            async with page.expect_download(timeout=30000) as download_info:
                await page.get_by_role("button", name="Options").click(timeout=10000)
                await page.wait_for_timeout(1500)
                # Click the CSV menu item specifically (not the "Export" section header)
                await page.get_by_role("menuitem", name="CSV").click(timeout=10000)
                print("Clicked CSV menuitem.")
            download = await download_info.value
            csv_path = str(DATA_DIR / f"fresha_report_{datetime.now().strftime('%Y%m%d')}.csv")
            await download.save_as(csv_path)
            print(f"CSV saved to: {csv_path}")

        except Exception as e:
            print(f"ERROR during navigation: {e}")
            screenshot_path = str(DATA_DIR / "error_screenshot.png")
            await page.screenshot(path=screenshot_path)
            print(f"Screenshot saved to {screenshot_path} for debugging")
        finally:
            await browser.close()
    return csv_path


def extract_data_from_csv(csv_path, api_key):
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        csv_content = f.read()

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": f"""This is a CSV export from Fresha's Performance Summary report for last week.
Extract the data and return ONLY a valid JSON object in this exact structure:
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
    "cancelled_appts": 0, "no_show_appts": 0, "services_sold": 0
  }}]
}}
Rules: Return ONLY the JSON. All monetary values as plain numbers. Include ALL staff members.
CSV DATA:
{csv_content[:10000]}"""}]
    )
    raw = message.content[0].text
    start = raw.find("{")
    end = raw.rfind("}") + 1
    return json.loads(raw[start:end])


async def run():
    email = os.environ["FRESHA_EMAIL"]
    password = os.environ["FRESHA_PASSWORD"]
    api_key = os.environ["ANTHROPIC_API_KEY"]

    print(f"[{datetime.now()}] Starting Fresha data extraction...")
    csv_path = await download_csv(email, password)

    if not csv_path or not Path(csv_path).exists():
        print("ERROR: CSV was not downloaded.")
        data = {"error": "CSV download failed"}
    else:
        print(f"[{datetime.now()}] Extracting data from CSV using Claude...")
        try:
            data = extract_data_from_csv(csv_path, api_key)
            data["report_type"] = "performance_summary"
            print("Data extracted successfully.")
            except Exception as e:
            import traceback
            print(f"ERROR extracting data: {e}")
            traceback.print_exc()
            data = {"error": str(e)}


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
