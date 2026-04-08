"""
xero_payrun.py
--------------
Finds the existing DRAFT pay run in each Xero org and fills in each
employee's earnings from Fresha data:
  - Monday-Friday hours
  - Saturday hours
  - Sunday hours
  - Tips
  - Commission (10% of product sales ex-GST)

The pay run is left as DRAFT — log in to Xero to review and post.

Run AFTER:  fetch_hours.py + fetch_performance.py + xero_add_payruns.py

Run with:  python agent/xero_payrun.py
Requires:  data/xero_token.json
           data/fresha_hours_nt.json
           data/fresha_hours_qld.json
           data/performance_summary.json
           data/cairns_performance_summary.json
"""

import base64
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DATA_DIR      = Path(__file__).parent.parent / "data"
TOKEN_FILE    = DATA_DIR / "xero_token.json"
CLIENT_ID     = os.environ["XERO_CLIENT_ID"]
CLIENT_SECRET = os.environ["XERO_CLIENT_SECRET"]


# ── Earnings rate IDs per Xero org ────────────────────────────────────────────

ORG_RATES = {
    "Diamond Barbers Pty Ltd": {
        "weekday":    "ba08c024-1289-420e-8e46-4a00d989815b",
        "saturday":   "b7771727-60d0-4e37-8f60-fca50b0f4423",
        "sunday":     "c7d4a9e4-e735-485f-8700-c7d29a17dff4",
        "tips":       "759bbf1f-a20a-4123-bb17-80842dc688ec",
        "commission": "fb04b066-99fa-4b56-815b-94092a009e38",
    },
    "DIAMOND BARBERS CAIRNS PTY LTD": {
        "weekday":    "0ac27a0f-b798-4f26-b53a-7e1c1c300f03",
        "saturday":   "18b97f5d-455b-42ee-846b-63550acb6b5d",
        "sunday":     "3c88813b-e892-4b9b-9e27-22c5fa734379",
        "tips":       "d6aef20e-4ed4-4d92-88c8-3dd3afa6eb23",
        "commission": "42714ec9-fb41-4498-9cea-b0a2c8b6f4f3",
    },
    "D.B. Parap Pty Ltd": {
        "weekday":    "2c266681-811c-4c02-9ea0-f133885b214c",
        "saturday":   "3d92631e-7e25-4ba6-9c4c-d0e3a822e674",
        "sunday":     "ca82390d-dacf-4dfc-8bad-29647fc118fa",
        "tips":       "f9261b3a-0659-48e4-990c-40d770cef73c",
        "commission": "9b40d911-89b7-401b-82c1-662fa9e2c782",
    },
}

SKIP_ORGS = {"DB WULGURU PTY LTD"}

EXCLUDED_EMPLOYEES = {
    "andrew mcdevitt",
    "andrew  mcdevitt",
    "nicole diamantis",
    "nicole  diamantis",
}

# Xero name (normalised) → Fresha name where they differ
XERO_TO_FRESHA = {
    "anthony  crispo":      "anthony crispo",
    "jairo espinosa mejia": "jairo espinosa",
    "nikolaos diamantis":   "nico diamantis",
    "vincenzo vanzanella":  "vince vincenzo",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def norm(name):
    return " ".join(name.lower().split())


def resolve_fresha_name(xero_name):
    n = norm(xero_name)
    return XERO_TO_FRESHA.get(n, n)


def parse_xero_date(date_str):
    if not date_str:
        return None
    m = re.match(r"/Date\((\d+)", str(date_str))
    if m:
        ms = int(m.group(1))
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    return str(date_str)[:10]


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


def xero_post(path, tenant_id, access_token, body):
    data = json.dumps(body).encode()
    req  = urllib.request.Request(
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
        raise Exception(f"HTTP {e.code}: {raw.decode('utf-8', errors='replace')}") from None


# ── Fresha data loading ───────────────────────────────────────────────────────

def load_hours():
    """Load NT + QLD hours into one dict keyed by normalised Fresha name."""
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
    """Load tips + commission per employee from both performance files."""
    perf = {}
    for fname in ("performance_summary.json", "cairns_performance_summary.json"):
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
            key      = norm(s["name"])
            products = s.get("products", 0) or 0
            perf[key] = {
                "tips":       s.get("tips", 0) or 0,
                "commission": round((products / 1.1) * 0.1, 2),
            }
    return perf


# ── Pay run processing ────────────────────────────────────────────────────────

def process_org(tenant_id, tenant_name, access_token, hours, perf):
    print(f"\n{'='*65}")
    print(f"ORG: {tenant_name}")
    print(f"{'='*65}")

    if tenant_name in SKIP_ORGS:
        print("  Skipping.")
        return

    rates = ORG_RATES.get(tenant_name)
    if not rates:
        print("  No rate configuration — skipping.")
        return

    # Fetch employees
    try:
        emp_data  = xero_get("/payroll.xro/1.0/Employees", tenant_id, access_token)
        employees = emp_data.get("Employees", [])
    except Exception as e:
        print(f"  ERROR fetching employees: {e}")
        return

    emp_id_map = {
        norm(f"{e.get('FirstName','')} {e.get('LastName','')}".strip()): e["EmployeeID"]
        for e in employees
    }

    # Find existing DRAFT pay run
    try:
        runs_data = xero_get("/payroll.xro/1.0/PayRuns", tenant_id, access_token)
        all_runs  = runs_data.get("PayRuns", [])
    except Exception as e:
        print(f"  ERROR fetching pay runs: {e}")
        return

    draft = next((r for r in all_runs if r.get("PayRunStatus") == "DRAFT"), None)
    if not draft:
        print("  No DRAFT pay run found.")
        print("  Run xero_add_payruns.py first to create one.")
        return

    run_id = draft["PayRunID"]
    print(f"  Found DRAFT pay run: {run_id}")

    # Get PayslipIDs from the draft
    try:
        run_detail  = xero_get(f"/payroll.xro/1.0/PayRuns/{run_id}", tenant_id, access_token)
        slips       = run_detail.get("PayRuns", [{}])[0].get("Payslips", [])
        emp_to_slip = {s["EmployeeID"]: s["PayslipID"] for s in slips if "PayslipID" in s}
    except Exception as e:
        print(f"  ERROR fetching payslips: {e}")
        return

    print(f"  {len(emp_to_slip)} payslips available.")

    # Write earnings to each payslip
    filled  = []
    skipped = []

    for xero_nm_norm, emp_id in emp_id_map.items():
        if xero_nm_norm in EXCLUDED_EMPLOYEES:
            skipped.append(f"{xero_nm_norm} (excluded)")
            continue

        slip_id = emp_to_slip.get(emp_id)
        if not slip_id:
            skipped.append(f"{xero_nm_norm} (no payslip ID)")
            continue

        fname = resolve_fresha_name(xero_nm_norm)
        h     = hours.get(fname)
        p     = perf.get(fname, {})

        # First-name fallback
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

        try:
            xero_post(f"/payroll.xro/1.0/Payslip/{slip_id}", tenant_id, access_token, [{
                "PayslipID":     slip_id,
                "EarningsLines": lines,
            }])
            filled.append({
                "name": xero_nm_norm, "h": h,
                "tips": tips, "commission": commission,
            })
        except Exception as e:
            print(f"  ERROR writing {xero_nm_norm}: {e}")
            skipped.append(f"{xero_nm_norm} (write error)")

    # Summary
    print(f"\n  Filled {len(filled)} payslips:\n")
    for r in filled:
        h = r["h"]
        print(
            f"  OK  {r['name']:30s}  "
            f"wk={h.get('weekday_hrs',0):4.1f}h  "
            f"sat={h.get('saturday_hrs',0):4.1f}h  "
            f"sun={h.get('sunday_hrs',0):4.1f}h  "
            f"tips=${r['tips']:6.2f}  comm=${r['commission']:6.2f}"
        )

    print(f"\n  Filled: {len(filled)}  Skipped: {len(skipped)}")
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

    print("\nDone. Log in to Xero to review and post the draft pay runs.")


if __name__ == "__main__":
    main()
