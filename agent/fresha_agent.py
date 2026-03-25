import asyncio
import os
import json
from datetime import datetime
from pathlib import Path

from browser_use import Agent
from langchain_anthropic import ChatAnthropic
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

SCHEMA = """
{
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
    {"name": "Staff Name", "revenue": 0.00, "appointments": 0}
  ],
  "services": [
    {"name": "Service Name", "revenue": 0.00, "count": 0}
  ]
}
"""


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

    task = f"""
You are extracting business data from Fresha, a salon/barbershop management platform.

STEPS:
1. Go to https://partners.fresha.com
2. Log in with:
   - Email: {email}
   - Password: {password}
3. After logging in, navigate to the Reports section (look in the left sidebar or top navigation).
4. Find the "Performance Summary" report and open it.
5. Set the date range / period filter to "Last Week".
6. Wait for the report to fully load.
7. Extract ALL visible data from the report.

OUTPUT FORMAT:
Return ONLY a valid JSON object matching this exact schema (fill in real values, use 0 if not shown):
{SCHEMA}

Important:
- Return ONLY the JSON object, no other text before or after.
- All monetary values should be numbers (not strings), e.g. 1250.50 not "$1,250.50".
- If a field is not visible in the report, use 0 or an empty list [].
- For period_start and period_end, use the actual dates shown in the report.
"""

    agent = Agent(
        task=task,
        llm=llm,
    )

    print(f"[{datetime.now()}] Starting Fresha agent...")
    result = await agent.run()

    raw = result.final_result() or ""
    print(f"[{datetime.now()}] Agent finished. Raw output length: {len(raw)}")

    data = {}
    try:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start != -1 and end > start:
            data = json.loads(raw[start:end])
            print("JSON parsed successfully.")
        else:
            print("WARNING: No JSON found in output. Saving raw text.")
            data = {"raw_output": raw}
    except json.JSONDecodeError as e:
        print(f"WARNING: JSON parse error: {e}. Saving raw text.")
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


if __name__ == "__main__":
    asyncio.run(run())
