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


async def download_csv(email, password):
    csv_path = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            accept_downloads=True,
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()

        try:
            # Step 1: Go to login page
            print("Going to Fresha login page...")
            await page.goto("https://partners.fresha.com/users/sign-in", wait_until="networkidle")
            await page.wait_for_timeout(3000)

            # Step 2: Enter email
            print("Entering email...")
            await page.fill('input[type="email"]', email)
            await page.wait_for_timeout(1000)

            # Step 3: Click Continue
            print("Clicking Continue...")
            await page.get_by_role("button", name="Continue").click()
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)

            # Step 4: Enter password
            print("Entering password...")
            await page.fill('input[type="password"]', password)
            await page.wait_for_timeout(1000)

            # Step 5: Click Log in
            print("Clicking Log in...")
            await page.get_by_role("button", name="Log in").click()
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(5000)
            print(f"Logged in. Current URL: {page.url}")

            # Step 6: Click Reports in left sidebar
            print("Navigating to Reports...")
            try:
                await page.click('a[href*="report"]', timeout=5000)
            except Exception:
                try:
                    await page.get_by_text("Reports").first.click()
                except Exception:
                    await page.goto(page.url.split("/")[0] + "//" + page.url.split("/")[2] + "/reports")
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)

            # Step 7: Click Performance Summary
            print("Clicking Performance Summary...")
            await page.get_by_text("Performance summary", exact=False).first.click()
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(5000)

            # Step 8: Set date filter to Last week if not already set
            print("Checking date filter...")
            page_content = await page.content()
            if "Last week" not in page_content:
                print("Setting filter to Last week...")
                try:
                    await page.get_by_text("Month to date").click()
                    await page.wait_for_timeout(1000)
                    await page.get_by_text("Last week").click()
                    await page.wait_for_timeout(1000)
                    try:
                        await page.get_by_role("button", name="Apply").click()
                    except Exception:
                        pass
                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_timeout(5000)
            else:
                print("Filter already set to Last week.")

            # Step 9: Click Options then CSV to download
            print("Downloading CSV...")
            async with page.expect_download(timeout=30000) as download_info:
                await page.get_by_role("button", name="Options").click()
                await page.wait_for_timeout(1500)
                await page.get_by_text("CSV").click()

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
        messages=[
            {
                "role": "user",
                "content": f"""This is a CSV export from Fresha's Performance Summary report for last week.

Extract the data and return ONLY a valid JSON object in this exact structure:

{{
  "period_start": "YYYY-MM-DD",
  "period_end": "YYYY-MM-DD",
  "sales_summary": {{
    "services": 0.00,
    "service_addons": 0.00,
    "products": 0.00,
    "memberships": 0.00,
    "late_cancellation_fees": 0.00,
    "no_show_fees": 0.00,
    "total_sales": 0.00,
    "service_charges": 0.00,
    "tips": 0.00,
    "total_sales_and_other": 0.00
  }},
  "appointments": {{
    "total": 0,
    "online": 0,
    "offline": 0,
    "cancelled": 0,
    "no_shows": 0,
    "pct_online": 0.0,
    "pct_cancelled": 0.0,
    "pct_no_show": 0.0
  }},
  "sales_performance": {{
    "services_sold": 0,
    "avg_service_value": 0.00,
    "products_sold": 0,
    "avg_product_value": 0.00
  }},
  "upsell": {{
    "total": 0.00,
    "pct": 0.0
  }},
  "staff": [
    {{
      "name": "Staff Name",
      "services": 0.00,
      "products": 0.00,
      "total_sales": 0.00,
      "tips": 0.00,
      "total_appts": 0,
      "cancelled_appts": 0,
      "no_show_appts": 0,
      "services_sold": 0
    }}
  ]
}}

Rules:
- Return ONLY the JSON object. No other text.
- All monetary values as plain numbers with no currency symbols or commas.
- Include every single staff member from the CSV.
- Use 0 or 0.00 for any missing values.

CSV DATA:
{csv_content[:10000]}"""
            }
        ]
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
            print(f"ERROR extracting data: {e}")
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
