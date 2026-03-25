import asyncio
import os
import json
import csv
import glob
from datetime import datetime
from pathlib import Path

from browser_use import Agent
from browser_use.browser.browser import Browser, BrowserConfig
from langchain_anthropic import ChatAnthropic
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

DOWNLOAD_DIR = Path("/tmp/fresha_downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

async def run():
    email = os.environ["FRESHA_EMAIL"]
    password = os.environ["FRESHA_PASSWORD"]
    api_key = os.environ["ANTHROPIC_API_KEY"]

    llm = ChatAnthropic(
        model="claude-sonnet-4-6",
        api_key=api_key,
        timeout=180,
        stop=None,
    )

    browser = Browser(
        config=BrowserConfig(
            headless=False,
        )
    )

    task = f"""
You are controlling a web browser. Follow these steps EXACTLY in order.
Do not skip any step. Wait for each page to fully load before moving to the next step.

STEP 1:
Go to this exact URL: https://partners.fresha.com/users/sign-in
Wait for the page to fully load.

STEP 2:
Find the email input field on the page.
Click on it and type this email address: {email}

STEP 3:
Find and click the "Continue" button.
Wait for the next page to fully load.

STEP 4:
Find the password input field on the page.
Click on it and type this password: {password}

STEP 5:
Find and click the "Log in" button.
Wait for the dashboard to fully load. This may take a few seconds.

STEP 6:
Look for "Reports" in the left sidebar navigation menu.
Click on "Reports".
Wait for the reports page to fully load.

STEP 7:
Find "Performance Summary" in the list of reports.
Click on it.
Wait for the report to fully load.

STEP 8:
Find the date range filter button. It likely says "Month to date" or shows a date range at the top of the report.
Click on it.
Select "Last week" from the options that appear.
Click "Apply" to confirm.
Wait for the report to reload with the new date range.

STEP 9:
Find the "Options" button or export button on the report page (it may be a button with three dots, a gear icon, or say "Export" or "Options").
Click it.
Look for a "CSV" or "Download CSV" option in the menu that appears.
Click on CSV to download the file.
Wait for the download to complete.

STEP 10:
Now read ALL the data visible on the report page and return it as a JSON object in this exact format:

{{
  "period_start": "YYYY-MM-DD",
  "period_end": "YYYY-MM-DD",
  "gross_sales": 0.00,
  "net_sales": 0.00,
  "taxes": 0.00,
  "tips": 0.00,
  "discounts": 0.00,
  "total_appointments": 0,
  "completed_appointments": 0,
  "cancelled_appointments": 0,
  "no_shows": 0,
  "new_clients": 0,
  "returning_clients": 0,
  "staff": [
    {{"name": "Staff Name", "revenue": 0.00, "appointments": 0}}
  ],
  "services": [
    {{"name": "Service Name", "revenue": 0.00, "count": 0}}
  ]
}}

Rules:
- Return ONLY the JSON object. No other text.
- All money values must be plain numbers e.g. 1250.50 not "$1,250.50"
- Use 0 or empty list [] for anything not visible on the page.
- Use the actual dates shown in the report for period_start and period_end.
"""

    agent = Agent(
        task=task,
        llm=llm,
        browser=browser,
        max_failures=5,
    )

    print(f"[{datetime.now()}] Starting Fresha agent...")
    result = await agent.run()

    raw = result.final_result() or ""
    print(f"[{datetime.now()}] Agent finished. Raw output length: {len(raw)}")

    data = {}
    csv_files = glob.glob(str(DOWNLOAD_DIR / "*.csv"))
    if csv_files:
        latest_csv = max(csv_files, key=os.path.getctime)
        print(f"Found CSV: {latest_csv}")
        try:
            with open(latest_csv, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                data["csv_rows"] = rows
                print(f"CSV parsed: {len(rows)} rows")
        except Exception as e:
            print(f"CSV parse error: {e}")

    if not data:
        try:
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start != -1 and end > start:
                data = json.loads(raw[start:end])
                print("JSON parsed from agent output.")
            else:
                print("WARNING: No JSON found in agent output.")
                data = {"raw_output": raw}
        except json.JSONDecodeError as e:
            print(f"WARNING: JSON parse error: {e}")
            data = {"raw_output": raw}

    data["report_date"] = datetime.now().strftime("%Y-%m-%d")
    data["report_type"] = "performance_summary"

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

    print(f"[{datetime.now()}] Data saved to {output_file}")
    print(json.dumps(data, indent=2))

    await browser.close()
