"""
test_working_hours.py
---------------------
Tests fetching roster hours for 2 barbers from the Fresha working hours API.
Uses the saved session — no login needed.

Run with:  python test_working_hours.py
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

DATA_DIR     = Path(__file__).parent.parent / "data"
SESSION_FILE = DATA_DIR / "session.json"

PROVIDER_ID = "1371504"

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


def parse_time_to_minutes(t):
    """Convert HH:MM:SS to total minutes."""
    parts = t.split(":")
    return int(parts[0]) * 60 + int(parts[1])


def day_type(date_str):
    """Return 'weekday', 'saturday', or 'sunday'."""
    d = datetime.strptime(date_str, "%Y-%m-%d").weekday()  # 0=Mon … 6=Sun
    if d == 5:
        return "saturday"
    if d == 6:
        return "sunday"
    return "weekday"


def calc_hours(schedule_days, blocked_times, times_off, employee_ids, date_from, date_to):
    """Calculate rostered hours per employee broken down by weekday / Saturday / Sunday."""
    results = {}

    for emp_id in employee_ids:
        buckets = {"weekday": 0, "saturday": 0, "sunday": 0}

        # Sum all shifts for this employee within the date range
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

        # Subtract blocked time (breaks, off-site, etc.)
        for block in blocked_times:
            if block["employeeId"] != emp_id:
                continue
            if not (date_from <= block["date"] <= date_to):
                continue
            bucket = day_type(block["date"])
            start = parse_time_to_minutes(block["startTime"])
            end   = parse_time_to_minutes(block["endTime"])
            buckets[bucket] -= (end - start)

        # Subtract time off
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


def build_report_html(date_from, date_to, rows, location_commissions):
    """Build the full HTML report used for both the PDF and the email body."""
    staff_rows = ""
    for i, r in enumerate(rows):
        bg = "#f5f5f5" if i % 2 == 0 else "#ffffff"
        staff_rows += (
            f'<tr style="background:{bg}">'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0">{r["name"]}</td>'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{r["total"]}h</td>'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{r["weekday"]}h</td>'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{r["saturday"]}h</td>'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">{r["sunday"]}h</td>'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">${r["tips"]:.2f}</td>'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">${r["commission"]:.2f}</td>'
            f'</tr>'
        )

    total_commission = round(sum(location_commissions.values()), 2)
    loc_rows = ""
    for i, (loc, commission) in enumerate(location_commissions.items()):
        bg = "#f5f5f5" if i % 2 == 0 else "#ffffff"
        loc_rows += (
            f'<tr style="background:{bg}">'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0">{loc}</td>'
            f'<td style="padding:4px 8px;border-bottom:1px solid #e0e0e0;text-align:right">${commission:.2f}</td>'
            f'</tr>'
        )
    loc_rows += (
        f'<tr style="background:#1a1a2e;color:#ffffff;font-weight:600">'
        f'<td style="padding:5px 8px">TOTAL</td>'
        f'<td style="padding:5px 8px;text-align:right">${total_commission:.2f}</td>'
        f'</tr>'
    )

    return f"""<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;color:#333;margin:0;padding:16px">
<h2 style="color:#1a1a2e;margin-bottom:2px;font-size:15px">Diamond Barbers NT (Darwin) &mdash; Weekly Hours Report</h2>
<p style="color:#888;margin-top:0;margin-bottom:12px;font-size:11px">{date_from} &ndash; {date_to}</p>

<table style="border-collapse:collapse;width:100%;font-size:10px;margin-bottom:16px">
  <thead>
    <tr style="background:#1a1a2e;color:#ffffff">
      <th style="padding:5px 8px;text-align:left;font-weight:600">Name</th>
      <th style="padding:5px 8px;text-align:right;font-weight:600">Total</th>
      <th style="padding:5px 8px;text-align:right;font-weight:600">Mon&ndash;Fri</th>
      <th style="padding:5px 8px;text-align:right;font-weight:600">Sat</th>
      <th style="padding:5px 8px;text-align:right;font-weight:600">Sun</th>
      <th style="padding:5px 8px;text-align:right;font-weight:600">Tips</th>
      <th style="padding:5px 8px;text-align:right;font-weight:600">Commission</th>
    </tr>
  </thead>
  <tbody>{staff_rows}</tbody>
</table>

<h3 style="color:#1a1a2e;margin-bottom:6px;font-size:12px">Commission by Location (ex-GST)</h3>
<table style="border-collapse:collapse;width:100%;max-width:320px;font-size:10px">
  <thead>
    <tr style="background:#1a1a2e;color:#ffffff">
      <th style="padding:5px 8px;text-align:left;font-weight:600">Location</th>
      <th style="padding:5px 8px;text-align:right;font-weight:600">Commission</th>
    </tr>
  </thead>
  <tbody>{loc_rows}</tbody>
</table>

<p style="color:#aaa;font-size:9px;margin-top:12px">
  Commission = 10% of product sales ex-GST &nbsp;&nbsp;|&nbsp;&nbsp; products &divide; 1.1 &times; 10%
</p>
</body></html>"""


def send_report_email(date_from, date_to, html, pdf_path):
    """Email the report with a PDF attachment."""
    email_from = "claude@diamondbarbers.com.au"
    email_to   = "admin@diamondbarbers.com.au"
    password   = os.environ.get("EMAIL_PASSWORD", "")

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"NT (Darwin) Weekly Hours Report \u2014 {date_from} to {date_to}"
    msg["From"]    = email_from
    msg["To"]      = email_to
    msg.attach(MIMEText(html, "html"))

    with open(pdf_path, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
    email_encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="nt_hours_{date_from}.pdf"')
    msg.attach(part)

    with smtplib.SMTP(os.environ.get("EMAIL_HOST", "mail.diamondbarbers.com.au"), 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(email_from, password)
        smtp.sendmail(email_from, email_to, msg.as_string())

    print(f"Report emailed to {email_to}")


async def test():
    if not SESSION_FILE.exists():
        print("ERROR: No session.json found. Run the main agent first to log in.")
        return

    # Last week date range (Darwin timezone, UTC+9:30)
    DARWIN_TZ = timezone(timedelta(hours=9, minutes=30))
    today = datetime.now(DARWIN_TZ)
    days_since_monday = today.weekday()
    last_monday = today - timedelta(days=days_since_monday + 7)
    last_sunday = last_monday + timedelta(days=6)
    date_from = last_monday.strftime("%Y-%m-%d")
    date_to   = last_sunday.strftime("%Y-%m-%d")
    print(f"Date range: {date_from} → {date_to}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(storage_state=str(SESSION_FILE))

        # ── Step 1: Fetch all locations ───────────────────────────────────────
        print("\nFetching all locations...")
        loc_resp = await context.request.get(
            f"https://partners-api.fresha.com/locations?__pid={PROVIDER_ID}"
        )
        loc_data = await loc_resp.json()
        locations = [
            {"id": item["id"], "name": item["attributes"].get("name", item["id"])}
            for item in loc_data.get("data", [])
            if not item["attributes"].get("deleted-at")
        ]
        print(f"Found {len(locations)} location(s): {[l['name'] for l in locations]}")

        all_results = {}
        gql_url = f"https://staff-working-hours-api.fresha.com/graphql?__pid={PROVIDER_ID}"

        for loc in locations:
            loc_id   = loc["id"]
            loc_name = loc["name"]
            print(f"\n── {loc_name} (id={loc_id}) ──")

            # ── Step 2: Get employees for this location ───────────────────────
            emp_resp = await context.request.get(
                f"https://partners-api.fresha.com/v2/employees"
                f"?location-id={loc_id}&with-deleted=false&__pid={PROVIDER_ID}"
            )
            emp_data = await emp_resp.json()
            employees = []
            for item in emp_data.get("data", []):
                attrs = item.get("attributes", {})
                name  = f"{attrs.get('first-name', '')} {attrs.get('last-name', '')}".strip()
                employees.append({"id": item["id"], "name": name})

            if not employees:
                print("  No employees found.")
                continue
            print(f"  {len(employees)} employee(s): {[e['name'] for e in employees]}")

            emp_ids = [e["id"] for e in employees]

            # ── Step 3: Fetch working hours ───────────────────────────────────
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
            wh = gql_data.get("data", {})
            schedule_days = wh.get("employeeScheduleDays", [])
            blocked_times = wh.get("blockedTimeOccurrences", [])
            times_off     = wh.get("timesOffOccurrences", [])

            # ── Step 4: Calculate hours ───────────────────────────────────────
            hours = calc_hours(schedule_days, blocked_times, times_off, emp_ids, date_from, date_to)

            print(f"  {'Name':30s}  {'Total':>6}  {'Mon-Fri':>8}  {'Sat':>5}  {'Sun':>5}")
            print(f"  {'-'*30}  {'-'*6}  {'-'*8}  {'-'*5}  {'-'*5}")
            for emp in employees:
                h = hours.get(emp["id"], {})
                if h.get("total_hrs", 0) == 0:
                    continue   # skip unrostered staff
                print(f"  {emp['name']:30s}  {h.get('total_hrs',0):>5}h  "
                      f"{h.get('weekday_hrs',0):>7}h  "
                      f"{h.get('saturday_hrs',0):>4}h  "
                      f"{h.get('sunday_hrs',0):>4}h")

            all_results[loc_name] = {
                "employees": employees,
                "hours": hours,
            }

        # ── Load tips/sales from GitHub (always fresh) ───────────────────────
        GITHUB_URL = "https://api.github.com/repos/andrewmcdevitt-stack/diamond-barbers-dashboard/contents/data/fresha_performance_nt.json"
        staff_lookup = {}
        try:
            import base64, urllib.request
            req = urllib.request.Request(GITHUB_URL, headers={"Accept": "application/vnd.github.v3+json"})
            with urllib.request.urlopen(req) as resp:
                meta = json.loads(resp.read())
                decoded = base64.b64decode(meta["content"].replace("\n", ""))
                history = json.loads(decoded)
            if not isinstance(history, list):
                history = [history]
            # Find the record matching last week's date range
            matched = next((r for r in reversed(history) if r.get("staff") and r.get("period_start") == date_from), None)
            # Fall back to most recent record with staff if no exact match
            if not matched:
                matched = next((r for r in reversed(history) if r.get("staff")), None)
            if matched:
                print(f"\nLoaded staff data for period: {matched.get('period_start')} to {matched.get('period_end')}")
                for s in matched["staff"]:
                    key = s["name"].lower().strip()
                    staff_lookup[key] = {
                        "tips":        s.get("tips", 0),
                        "services":    s.get("services", 0),
                        "products":    s.get("products", 0),
                        "total_sales": s.get("total_sales", 0),
                    }
        except Exception as e:
            print(f"WARNING: Could not load staff data from GitHub: {e}")

        # ── Combined summary across all locations ─────────────────────────────
        # An employee may work across multiple locations — sum their hours together
        combined = {}  # name -> {weekday, saturday, sunday, total}
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

        # Sort by total hours descending, drop anyone with 0
        combined = {k: v for k, v in sorted(combined.items(), key=lambda x: x[0].lower()) if v["total_hrs"] > 0}

        print(f"\n{'='*95}")
        print(f"ALL STAFF — {date_from} to {date_to}")
        print(f"{'='*95}")
        print(f"  {'Name':30s}  {'Total':>6}  {'Mon-Fri':>8}  {'Sat':>5}  {'Sun':>5}  {'Tips':>8}  {'Commission':>12}")
        print(f"  {'-'*30}  {'-'*6}  {'-'*8}  {'-'*5}  {'-'*5}  {'-'*8}  {'-'*12}")
        report_rows = []
        for name, h in combined.items():
            perf = staff_lookup.get(name.lower().strip())
            # Try exact match first, then first-name-only fallback
            if perf is None:
                first = name.split()[0].lower()
                perf = next((v for k, v in staff_lookup.items() if k.startswith(first + " ") or k == first), {})
            tips       = perf.get("tips", 0)
            products   = perf.get("products", 0)
            commission = round((products / 1.1) * 0.1, 2)   # 10% of product sales ex-GST
            report_rows.append({
                "name": name,
                "total": h["total_hrs"], "weekday": h["weekday_hrs"],
                "saturday": h["saturday_hrs"], "sunday": h["sunday_hrs"],
                "tips": tips, "commission": commission,
            })
            print(f"  {name:30s}  {h['total_hrs']:>5}h  "
                  f"{h['weekday_hrs']:>7}h  "
                  f"{h['saturday_hrs']:>4}h  "
                  f"{h['sunday_hrs']:>4}h  "
                  f"${tips:>7.2f}  "
                  f"${commission:>10.2f}")
        print(f"{'='*95}")
        print(f"  Commission = 10% of product sales ex-GST  (products / 1.1 × 10%)")

        # ── Assign each employee to their primary location (most hours) ──────────
        emp_primary_loc = {}
        for loc_name, loc_res in all_results.items():
            emp_map = {e["id"]: e["name"] for e in loc_res["employees"]}
            for emp_id, h in loc_res["hours"].items():
                if h["total_hrs"] == 0:
                    continue
                name = emp_map.get(emp_id, emp_id)
                if name not in emp_primary_loc or h["total_hrs"] > emp_primary_loc[name]["hrs"]:
                    emp_primary_loc[name] = {
                        "location": loc_name.replace("Diamond Barbers - ", ""),
                        "hrs": h["total_hrs"],
                    }

        location_commissions = {}
        for row in report_rows:
            loc = emp_primary_loc.get(row["name"], {}).get("location", "Other")
            location_commissions[loc] = round(location_commissions.get(loc, 0) + row["commission"], 2)
        location_commissions = dict(sorted(location_commissions.items()))

        # ── Generate PDF using playwright ─────────────────────────────────────
        html_content = build_report_html(date_from, date_to, report_rows, location_commissions)
        pdf_path = DATA_DIR / f"nt_hours_{date_from}.pdf"
        pdf_page = await context.new_page()
        await pdf_page.set_content(html_content, wait_until="load")
        pdf_bytes = await pdf_page.pdf(
            format="A4",
            margin={"top": "15mm", "right": "15mm", "bottom": "15mm", "left": "15mm"},
        )
        await pdf_page.close()
        pdf_path.write_bytes(pdf_bytes)
        print(f"PDF saved to {pdf_path}")

        # ── Send email report ─────────────────────────────────────────────────
        try:
            send_report_email(date_from, date_to, html_content, pdf_path)
        except Exception as e:
            print(f"WARNING: Could not send email: {e}")

        # Save results
        out = DATA_DIR / "fresha_hours_nt.json"
        out.write_text(json.dumps({"summary": combined, "by_location": all_results}, indent=2))
        print(f"\nFull results saved to {out}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(test())
