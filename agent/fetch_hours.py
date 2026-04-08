"""
fetch_hours.py
--------------
Fetches roster hours for all staff from both Fresha accounts (NT + QLD).
Calculates actual hours worked = shifts - blocked time - time off, split
into weekday / Saturday / Sunday buckets.

Saves:
  data/fresha_hours_nt.json   — Darwin + Parap staff (feeds xero_payrun.py)
  data/fresha_hours_qld.json  — Cairns staff (feeds xero_payrun.py)

Run with:  python agent/fetch_hours.py
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

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"

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

ACCOUNTS = [
    {
        "label":       "NT (Darwin)",
        "session":     DATA_DIR / "session.json",
        "provider_id": "1371504",
        "timezone":    timezone(timedelta(hours=9, minutes=30)),
        "output":      DATA_DIR / "fresha_hours_nt.json",
        "pdf":         "nt_hours",
        "email_subj":  "NT (Darwin) Weekly Hours Report",
    },
    {
        "label":       "QLD (Cairns)",
        "session":     DATA_DIR / "session_cairns.json",
        "provider_id": "1390965",
        "timezone":    timezone(timedelta(hours=10)),
        "output":      DATA_DIR / "fresha_hours_qld.json",
        "pdf":         "qld_hours",
        "email_subj":  "QLD (Cairns & Townsville) Weekly Hours Report",
    },
]


# ── Shared helpers ─────────────────────────────────────────────────────────────

def parse_time_to_minutes(t):
    parts = t.split(":")
    return int(parts[0]) * 60 + int(parts[1])


def day_type(date_str):
    d = datetime.strptime(date_str, "%Y-%m-%d").weekday()
    if d == 5:
        return "saturday"
    if d == 6:
        return "sunday"
    return "weekday"


def calc_hours(schedule_days, blocked_times, times_off, employee_ids, date_from, date_to):
    results = {}
    for emp_id in employee_ids:
        buckets = {"weekday": 0, "saturday": 0, "sunday": 0}

        for day in schedule_days:
            if day["employeeId"] != emp_id:
                continue
            if not (date_from <= day["date"] <= date_to):
                continue
            bucket = day_type(day["date"])
            for shift in day.get("shifts", []):
                start = parse_time_to_minutes(shift["startTime"])
                end   = parse_time_to_minutes(shift["endTime"])
                buckets[bucket] += (end - start)

        for block in blocked_times:
            if block["employeeId"] != emp_id:
                continue
            if not (date_from <= block["date"] <= date_to):
                continue
            bucket = day_type(block["date"])
            start = parse_time_to_minutes(block["startTime"])
            end   = parse_time_to_minutes(block["endTime"])
            buckets[bucket] -= (end - start)

        for off in times_off:
            if off["employeeId"] != emp_id:
                continue
            if not (date_from <= off.get("date", "") <= date_to):
                continue
            if off.get("startTime") and off.get("endTime"):
                bucket = day_type(off["date"])
                start = parse_time_to_minutes(off["startTime"])
                end   = parse_time_to_minutes(off["endTime"])
                buckets[bucket] -= (end - start)

        results[emp_id] = {
            "weekday_hrs":  round(max(0, buckets["weekday"])  / 60, 2),
            "saturday_hrs": round(max(0, buckets["saturday"]) / 60, 2),
            "sunday_hrs":   round(max(0, buckets["sunday"])   / 60, 2),
            "total_hrs":    round(max(0, sum(buckets.values())) / 60, 2),
        }
    return results


def send_report_email(subject, date_from, date_to, html, pdf_path):
    email_from = "claude@diamondbarbers.com.au"
    email_to   = "admin@diamondbarbers.com.au"
    password   = os.environ.get("EMAIL_PASSWORD", "")

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"{subject} \u2014 {date_from} to {date_to}"
    msg["From"]    = email_from
    msg["To"]      = email_to
    msg.attach(MIMEText(html, "html"))

    with open(pdf_path, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
    email_encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{pdf_path.name}"')
    msg.attach(part)

    with smtplib.SMTP(os.environ.get("EMAIL_HOST", "mail.diamondbarbers.com.au"), 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(email_from, password)
        smtp.sendmail(email_from, email_to, msg.as_string())

    print(f"Report emailed to {email_to}")


def build_report_html(label, date_from, date_to, rows):
    staff_rows = ""
    for i, r in enumerate(rows):
        bg = "#f5f5f5" if i % 2 == 0 else "#ffffff"
        staff_rows += (
            f'<tr style="background:{bg}">'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0">{r["name"]}</td>'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{r["total_hrs"]}h</td>'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{r["weekday_hrs"]}h</td>'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{r["saturday_hrs"]}h</td>'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{r["sunday_hrs"]}h</td>'
            f'</tr>'
        )

    return f"""<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;color:#333;margin:0;padding:16px">
<h2 style="color:#1a1a2e;margin-bottom:2px;font-size:15px">Diamond Barbers {label} &mdash; Weekly Hours</h2>
<p style="color:#888;margin-top:0;margin-bottom:12px;font-size:11px">{date_from} &ndash; {date_to}</p>
<table style="border-collapse:collapse;width:100%;font-size:10px">
  <thead>
    <tr style="background:#1a1a2e;color:#ffffff">
      <th style="padding:5px 8px;text-align:left">Name</th>
      <th style="padding:5px 8px;text-align:right">Total</th>
      <th style="padding:5px 8px;text-align:right">Mon&ndash;Fri</th>
      <th style="padding:5px 8px;text-align:right">Sat</th>
      <th style="padding:5px 8px;text-align:right">Sun</th>
    </tr>
  </thead>
  <tbody>{staff_rows}</tbody>
</table>
</body></html>"""


# ── Per-account fetch ──────────────────────────────────────────────────────────

async def fetch_account(account, context, date_from, date_to):
    pid     = account["provider_id"]
    label   = account["label"]
    gql_url = f"https://staff-working-hours-api.fresha.com/graphql?__pid={pid}"

    print(f"\n{'='*60}")
    print(f"ACCOUNT: {label}")
    print(f"{'='*60}")

    # Fetch all locations
    loc_resp = await context.request.get(
        f"https://partners-api.fresha.com/locations?__pid={pid}"
    )
    loc_data = await loc_resp.json()
    locations = [
        {"id": item["id"], "name": item["attributes"].get("name", item["id"])}
        for item in loc_data.get("data", [])
        if not item["attributes"].get("deleted-at")
    ]
    print(f"Found {len(locations)} location(s): {[l['name'] for l in locations]}")

    all_results = {}

    for loc in locations:
        loc_id   = loc["id"]
        loc_name = loc["name"]
        print(f"\n  -- {loc_name} --")

        emp_resp = await context.request.get(
            f"https://partners-api.fresha.com/v2/employees"
            f"?location-id={loc_id}&with-deleted=false&__pid={pid}"
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
        schedule_days = wh.get("employeeScheduleDays", [])
        blocked_times = wh.get("blockedTimeOccurrences", [])
        times_off     = wh.get("timesOffOccurrences", [])

        hours = calc_hours(schedule_days, blocked_times, times_off, emp_ids, date_from, date_to)

        for emp in employees:
            h = hours.get(emp["id"], {})
            if h.get("total_hrs", 0) == 0:
                continue
            print(f"    {emp['name']:30s}  total={h['total_hrs']}h  "
                  f"wk={h['weekday_hrs']}h  sat={h['saturday_hrs']}h  sun={h['sunday_hrs']}h")

        all_results[loc_name] = {"employees": employees, "hours": hours}

    # Combine hours across locations per employee (sum unique shifts)
    combined = {}
    for loc_name, loc_res in all_results.items():
        emp_map = {e["id"]: e["name"] for e in loc_res["employees"]}
        for emp_id, h in loc_res["hours"].items():
            name = emp_map.get(emp_id, emp_id)
            if name not in combined:
                combined[name] = {"weekday_hrs": 0, "saturday_hrs": 0, "sunday_hrs": 0, "total_hrs": 0}
            combined[name]["weekday_hrs"]  += h.get("weekday_hrs", 0)
            combined[name]["saturday_hrs"] += h.get("saturday_hrs", 0)
            combined[name]["sunday_hrs"]   += h.get("sunday_hrs", 0)
            combined[name]["total_hrs"]    += h.get("total_hrs", 0)

    combined = {
        k: v for k, v in sorted(combined.items(), key=lambda x: x[0].lower())
        if v["total_hrs"] > 0
    }

    return combined, all_results


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
            print(f"\nDate range ({account['label']}): {date_from} → {date_to}")

            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(storage_state=str(session_file))

            combined, all_results = await fetch_account(account, context, date_from, date_to)

            # Save JSON
            out = account["output"]
            out.write_text(json.dumps({
                "date_from":   date_from,
                "date_to":     date_to,
                "generated":   datetime.now().isoformat(),
                "summary":     combined,
                "by_location": all_results,
            }, indent=2))
            print(f"\nSaved to {out}")

            # Generate PDF + email
            rows = [{"name": k, **v} for k, v in combined.items()]
            html = build_report_html(account["label"], date_from, date_to, rows)
            pdf_path = DATA_DIR / f"{account['pdf']}_{date_from}.pdf"
            pdf_page = await context.new_page()
            await pdf_page.set_content(html, wait_until="load")
            pdf_bytes = await pdf_page.pdf(
                format="A4",
                margin={"top": "15mm", "right": "15mm", "bottom": "15mm", "left": "15mm"},
            )
            await pdf_page.close()
            pdf_path.write_bytes(pdf_bytes)
            print(f"PDF saved to {pdf_path}")

            try:
                send_report_email(account["email_subj"], date_from, date_to, html, pdf_path)
            except Exception as e:
                print(f"WARNING: Email failed: {e}")

            await browser.close()

    print("\nDone. Both accounts fetched.")


if __name__ == "__main__":
    asyncio.run(run())
