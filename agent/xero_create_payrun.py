"""
xero_create_payrun.py
---------------------
Creates a DRAFT pay run in Xero for last week and fills in each
employee's earnings lines from Fresha roster data:
  - Monday-Friday hours
  - Saturday hours
  - Sunday hours
  - Tips
  - Commission (10% of product sales ex-GST)

The pay run is left as DRAFT — log in to Xero to review and post it.

Run with:  python agent/xero_create_payrun.py
Requires:  data/xero_token.json          (from xero_auth.py)
           data/fresha_hours_nt.json      (from fresha_hours_nt.py)
           data/fresha_hours_qld.json     (from fresha_hours_qld.py)
           data/fresha_performance_nt.json  (from fresha_performance_nt.py)
           data/fresha_performance_qld.json (from fresha_performance_qld.py)
"""

import base64
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DATA_DIR      = Path(__file__).parent.parent / "data"
TOKEN_FILE    = DATA_DIR / "xero_token.json"
CLIENT_ID     = os.environ["XERO_CLIENT_ID"]
CLIENT_SECRET = os.environ["XERO_CLIENT_SECRET"]


# ── Active earnings rate IDs per Xero org ─────────────────────────────────────
# Confirmed via xero_setup_check.py — only ACTIVE rates used

ORG_RATES = {
    "Diamond Barbers Pty Ltd": {
        "weekday":    "ba08c024-1289-420e-8e46-4a00d989815b",  # MONDAY-FRIDAY
        "saturday":   "b7771727-60d0-4e37-8f60-fca50b0f4423",  # SATURDAY
        "sunday":     "c7d4a9e4-e735-485f-8700-c7d29a17dff4",  # SUNDAY
        "tips":       "759bbf1f-a20a-4123-bb17-80842dc688ec",  # Tips
        "commission": "fb04b066-99fa-4b56-815b-94092a009e38",  # Commission
    },
    "DIAMOND BARBERS CAIRNS PTY LTD": {
        "weekday":    "0ac27a0f-b798-4f26-b53a-7e1c1c300f03",  # MONDAY-FRIDAY
        "saturday":   "18b97f5d-455b-42ee-846b-63550acb6b5d",  # SATURDAY
        "sunday":     "3c88813b-e892-4b9b-9e27-22c5fa734379",  # SUNDAY
        "tips":       "d6aef20e-4ed4-4d92-88c8-3dd3afa6eb23",  # Tips
        "commission": "42714ec9-fb41-4498-9cea-b0a2c8b6f4f3",  # Commission
    },
    "D.B. Parap Pty Ltd": {
        "weekday":    "2c266681-811c-4c02-9ea0-f133885b214c",  # MONDAY-FRIDAY
        "saturday":   "3d92631e-7e25-4ba6-9c4c-d0e3a822e674",  # SATURDAY
        "sunday":     "ca82390d-dacf-4dfc-8bad-29647fc118fa",  # SUNDAY
        "tips":       "f9261b3a-0659-48e4-990c-40d770cef73c",  # TIPS
        "commission": "9b40d911-89b7-401b-82c1-662fa9e2c782",  # COMMISSIONS
    },
}

# Orgs with no employees or no suitable rates — skip entirely
SKIP_ORGS = {"DB WULGURU PTY LTD", "DIAMOND BARBERS CAIRNS PTY LTD", "D.B. Parap Pty Ltd"}

# Employees to exclude from all pay runs (owners, managers)
EXCLUDED_EMPLOYEES = {
    "andrew mcdevitt",
    "andrew  mcdevitt",
    "nicole diamantis",
    "nicole  diamantis",
}

# Xero employee name (normalised lowercase) → Fresha name in hours JSON
# Only needed where the names differ between systems
XERO_TO_FRESHA = {
    "anthony  crispo":      "anthony crispo",
    "jairo espinosa mejia": "jairo espinosa",
    "nikolaos diamantis":   "nico diamantis",
    "vincenzo vanzanella":  "vince vincenzo",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def norm(name):
    """Lowercase and collapse whitespace."""
    return " ".join(name.lower().split())


def resolve_fresha_name(xero_name):
    """Map a Xero employee name to the matching name in the Fresha hours data."""
    n = norm(xero_name)
    return XERO_TO_FRESHA.get(n, n)


def parse_xero_date(date_str):
    """Parse Xero /Date(ms)/ or ISO date string → 'YYYY-MM-DD'."""
    if not date_str:
        return None
    m = re.match(r"/Date\((\d+)", str(date_str))
    if m:
        ms = int(m.group(1))
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    return str(date_str)[:10]


def last_week_dates():
    """Return (monday, sunday) date objects for last Mon–Sun (Darwin time)."""
    DARWIN_TZ = timezone(timedelta(hours=9, minutes=30))
    today          = datetime.now(DARWIN_TZ).date()
    last_monday    = today - timedelta(days=today.weekday())
    last_sunday    = last_monday + timedelta(days=6)
    return last_monday, last_sunday


# ── Token management ──────────────────────────────────────────────────────────

def refresh_token(token_data):
    credentials = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    data = urllib.parse.urlencode({
        "grant_type":    "refresh_token",
        "refresh_token": token_data["refresh_token"],
    }).encode()
    req = urllib.request.Request(
        "https://identity.xero.com/connect/token",
        data=data,
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type":  "application/x-www-form-urlencoded",
        },
    )
    with urllib.request.urlopen(req) as resp:
        new_token = json.loads(resp.read())
    new_token["tenants"] = token_data.get("tenants", [])
    TOKEN_FILE.write_text(json.dumps(new_token, indent=2))
    return new_token


# ── Xero API ──────────────────────────────────────────────────────────────────

def xero_get(path, tenant_id, access_token):
    req = urllib.request.Request(
        f"https://api.xero.com{path}",
        headers={
            "Authorization":  f"Bearer {access_token}",
            "Xero-Tenant-Id": tenant_id,
            "Accept":         "application/json",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def xero_put(path, tenant_id, access_token, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"https://api.xero.com{path}",
        data=data,
        method="PUT",
        headers={
            "Authorization":  f"Bearer {access_token}",
            "Xero-Tenant-Id": tenant_id,
            "Accept":         "application/json",
            "Content-Type":   "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            import gzip
            raw = gzip.decompress(raw)
        except Exception:
            pass
        raise Exception(f"HTTP {e.code}: {raw.decode('utf-8', errors='replace')}") from None


def xero_post(path, tenant_id, access_token, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"https://api.xero.com{path}",
        data=data,
        headers={
            "Authorization":  f"Bearer {access_token}",
            "Xero-Tenant-Id": tenant_id,
            "Accept":         "application/json",
            "Content-Type":   "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            import gzip
            raw = gzip.decompress(raw)
        except Exception:
            pass
        body_text = raw.decode("utf-8", errors="replace")
        raise Exception(f"HTTP {e.code}: {body_text}") from None


# ── Fresha data loading ───────────────────────────────────────────────────────

def load_hours():
    """Merge NT + QLD hours into one dict keyed by normalised Fresha name."""
    combined = {}
    for fname in ("fresha_hours_nt.json", "fresha_hours_qld.json"):
        path = DATA_DIR / fname
        if not path.exists():
            print(f"  WARNING: {fname} not found.")
            continue
        data = json.loads(path.read_text())
        for name, hrs in data.get("summary", {}).items():
            combined[norm(name)] = hrs
    return combined


def load_performance():
    """Load tips and commissions per employee from performance JSONs."""
    perf = {}
    for fname in ("fresha_performance_nt.json", "fresha_performance_qld.json"):
        path = DATA_DIR / fname
        if not path.exists():
            continue
        history = json.loads(path.read_text())
        if not isinstance(history, list):
            history = [history]
        record = next((r for r in reversed(history) if r.get("staff")), None)
        if not record:
            continue
        for s in record["staff"]:
            key = norm(s["name"])
            products = s.get("products", 0) or 0
            perf[key] = {
                "tips":       s.get("tips", 0) or 0,
                "commission": round((products / 1.1) * 0.1, 2),
            }
    return perf


# ── Pay run processing per org ────────────────────────────────────────────────

def build_payslip_list(emp_id_map, hours, perf, rates):
    """Build payslip entries for all employees with hours data."""
    payslip_list = []
    skipped      = []

    for xero_nm_norm, emp_id in emp_id_map.items():
        if xero_nm_norm in EXCLUDED_EMPLOYEES:
            skipped.append(xero_nm_norm)
            continue

        fname = resolve_fresha_name(xero_nm_norm)
        h     = hours.get(fname)
        p     = perf.get(fname, {})

        if not h:
            first = fname.split()[0]
            h = next((v for k, v in hours.items() if k.split()[0] == first), None)

        if not h or h.get("total_hrs", 0) == 0:
            skipped.append(f"{xero_nm_norm} (no hours)")
            continue

        lines = []
        if h.get("weekday_hrs", 0) > 0:
            lines.append({"EarningsRateID": rates["weekday"], "NumberOfUnits": round(h["weekday_hrs"], 2)})
        if h.get("saturday_hrs", 0) > 0:
            lines.append({"EarningsRateID": rates["saturday"], "NumberOfUnits": round(h["saturday_hrs"], 2)})
        if h.get("sunday_hrs", 0) > 0:
            lines.append({"EarningsRateID": rates["sunday"], "NumberOfUnits": round(h["sunday_hrs"], 2)})
        tips = p.get("tips", 0) or 0
        if tips > 0:
            lines.append({"EarningsRateID": rates["tips"], "NumberOfUnits": 1, "RatePerUnit": round(tips, 2)})
        commission = p.get("commission", 0) or 0
        if commission > 0:
            lines.append({"EarningsRateID": rates["commission"], "NumberOfUnits": 1, "RatePerUnit": round(commission, 2)})

        if not lines:
            skipped.append(f"{xero_nm_norm} (all values zero)")
            continue

        payslip_list.append({
            "EmployeeID":    emp_id,
            "EarningsLines": lines,
            "_name":         xero_nm_norm,
            "_h":            h,
            "_tips":         tips,
            "_commission":   commission,
        })

    return payslip_list, skipped


def process_org(tenant_id, tenant_name, access_token, hours, perf):
    print(f"\n{'='*65}")
    print(f"ORG: {tenant_name}")
    print(f"{'='*65}")

    if tenant_name in SKIP_ORGS:
        print("  Skipping (no employees / no rates configured).")
        return

    rates = ORG_RATES.get(tenant_name)
    if not rates:
        print("  No rate configuration — skipping.")
        return

    date_from, date_to = last_week_dates()
    date_from_str = date_from.strftime("%Y-%m-%d")
    date_to_str   = date_to.strftime("%Y-%m-%d")
    print(f"  Pay period: {date_from_str} to {date_to_str}")

    # ── Fetch employees ───────────────────────────────────────────────────────
    try:
        emp_data  = xero_get("/payroll.xro/1.0/Employees", tenant_id, access_token)
        employees = emp_data.get("Employees", [])
    except Exception as e:
        print(f"  ERROR fetching employees: {e}")
        return

    emp_id_map = {}
    for e in employees:
        full = f"{e.get('FirstName','')} {e.get('LastName','')}".strip()
        emp_id_map[norm(full)] = e["EmployeeID"]

    # ── Build payslip data from Fresha ────────────────────────────────────────
    payslip_list, skipped = build_payslip_list(emp_id_map, hours, perf, rates)

    # Strip internal keys before sending to Xero
    def clean(ps):
        return {k: v for k, v in ps.items() if not k.startswith("_")}

    # ── Get PayrollCalendarID from most recent pay run ────────────────────────
    try:
        runs_data = xero_get("/payroll.xro/1.0/PayRuns", tenant_id, access_token)
        all_runs  = runs_data.get("PayRuns", [])
    except Exception as e:
        print(f"  ERROR fetching pay runs: {e}")
        return

    if not all_runs:
        print("  No existing pay runs found.")
        return

    all_runs.sort(
        key=lambda r: parse_xero_date(r.get("PaymentDate") or r.get("PayRunPeriodEndDate", "")) or "",
        reverse=True,
    )
    calendar_id = all_runs[0].get("PayrollCalendarID")
    if not calendar_id:
        print("  Could not determine PayrollCalendarID.")
        return

    # ── Check for existing draft/posted pay run for this period ───────────────
    # First check if there's any existing DRAFT (created via web UI or previously)
    existing = next((r for r in all_runs if r.get("PayRunStatus") == "DRAFT"), None)
    # If no draft, check if this period is already posted
    if not existing:
        for r in all_runs:
            p_start = parse_xero_date(r.get("PayRunPeriodStartDate", ""))
            p_end   = parse_xero_date(r.get("PayRunPeriodEndDate", ""))
            if p_start == date_from_str and p_end == date_to_str:
                existing = r
                break

    if existing:
        status = existing.get("PayRunStatus")
        run_id = existing.get("PayRunID")
        if status == "POSTED":
            print(f"  Pay run for this period already POSTED ({run_id}) — skipping.")
            return
        print(f"  Found existing DRAFT pay run: {run_id}")
    else:
        # ── Create a new pay run WITH payslip data embedded ───────────────────
        print("  Creating new DRAFT pay run with earnings...")
        try:
            body   = [{
                "PayrollCalendarID":     calendar_id,
                "PayRunPeriodStartDate": f"{date_from_str}T00:00:00",
                "PayRunPeriodEndDate":   f"{date_to_str}T00:00:00",
                "Payslips":              [clean(ps) for ps in payslip_list],
            }]
            result = xero_post("/payroll.xro/1.0/PayRuns", tenant_id, access_token, body)
            runs   = result if isinstance(result, list) else result.get("PayRuns", [{}])
            run_id = runs[0].get("PayRunID")
            if not run_id:
                print(f"  ERROR: No PayRunID in response: {result}")
                return
            print(f"  Created: {run_id}")
            # Print first payslip from creation response to see if IDs are there
            resp_slips = runs[0].get("Payslips", [])
            if resp_slips:
                print(f"  DEBUG creation response payslip keys: {list(resp_slips[0].keys())}")
        except Exception as e:
            print(f"  ERROR creating pay run: {e}")
            return

    # ── Activate each payslip via PUT ─────────────────────────────────────────
    print("  Activating payslips...")
    try:
        run_detail = xero_get(f"/payroll.xro/1.0/PayRuns/{run_id}", tenant_id, access_token)
        run_data   = run_detail.get("PayRuns", [{}])[0]
        slips      = run_data.get("Payslips", [])
        emp_to_slip = {s["EmployeeID"]: s["PayslipID"] for s in slips if "PayslipID" in s}
        print(f"  Found {len(emp_to_slip)} PayslipIDs")
        activated  = 0
        for ps in payslip_list:
            slip_id = emp_to_slip.get(ps["EmployeeID"])
            if not slip_id:
                print(f"  No PayslipID for {ps['_name']}")
                continue
            print(f"  Writing to {ps['_name']} ({slip_id})...")
            xero_post(f"/payroll.xro/1.0/Payslip/{slip_id}", tenant_id, access_token, [{
                "PayslipID":    slip_id,
                "EarningsLines": clean(ps)["EarningsLines"],
            }])
            activated += 1
        print(f"  Activated {activated} payslips.")
    except Exception as e:
        print(f"  ERROR in activation: {e}")
        import traceback; traceback.print_exc()

    # ── Print summary ─────────────────────────────────────────────────────────
    print(f"\n  Filled {len(payslip_list)} payslips:\n")
    for ps in payslip_list:
        h    = ps["_h"]
        tips = ps["_tips"]
        comm = ps["_commission"]
        print(
            f"  OK  {ps['_name']:30s}  "
            f"wk={h.get('weekday_hrs',0):4.1f}h  "
            f"sat={h.get('saturday_hrs',0):4.1f}h  "
            f"sun={h.get('sunday_hrs',0):4.1f}h  "
            f"tips=${tips:6.2f}  comm=${comm:6.2f}"
        )

    print(f"\n  Summary — filled: {len(payslip_list)}  skipped: {len(skipped)}")
    if skipped:
        for s in skipped:
            print(f"    skipped: {s}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Refreshing Xero token...")
    token        = json.loads(TOKEN_FILE.read_text())
    token        = refresh_token(token)
    access_token = token["access_token"]
    tenants      = token.get("tenants", [])

    print("Loading Fresha data...")
    hours = load_hours()
    perf  = load_performance()
    print(f"  {len(hours)} employees in hours data")
    print(f"  {len(perf)} employees in performance data")

    for tenant in tenants:
        process_org(tenant["id"], tenant["name"], access_token, hours, perf)

    print("\n\nDone. Log in to Xero to review and post the draft pay runs.")


if __name__ == "__main__":
    main()
