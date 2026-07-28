"""
xero_topup_payrun.py
--------------------
Fills in ONLY the payslips that currently have zero earnings in the
existing DRAFT pay run. Leaves any already-filled payslips untouched.

Run with:  python agent/xero_topup_payrun.py
"""

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
from xero_create_payrun import (
    DATA_DIR, TOKEN_FILE, SKIP_ORGS, ORG_RATES, EXCLUDED_EMPLOYEES,
    refresh_token, xero_get, xero_post, norm,
    load_hours, load_performance, load_night_markets_bonus, load_location_products,
    build_payslip_list, resolve_fresha_name, parse_xero_date,
)


def main():
    print("Refreshing Xero token...")
    token        = json.loads(TOKEN_FILE.read_text())
    token        = refresh_token(token)
    access_token = token["access_token"]
    tenants      = token.get("tenants", [])

    print("Loading Fresha data...")
    hours            = load_hours()
    perf             = load_performance()
    bonuses          = load_night_markets_bonus()
    loc_nt, loc_qld  = load_location_products()

    for tenant in tenants:
        tenant_name = tenant["name"]
        tenant_id   = tenant["id"]

        if tenant_name in SKIP_ORGS:
            continue
        rates = ORG_RATES.get(tenant_name)
        if not rates:
            continue

        print(f"\n{'='*65}")
        print(f"ORG: {tenant_name}")
        print(f"{'='*65}")

        # Find existing DRAFT pay run
        runs_data = xero_get("/payroll.xro/1.0/PayRuns", tenant_id, access_token)
        draft = next((r for r in runs_data.get("PayRuns", []) if r.get("PayRunStatus") == "DRAFT"), None)
        if not draft:
            print("  No DRAFT pay run found — skipping.")
            continue
        run_id = draft["PayRunID"]
        print(f"  Found DRAFT: {run_id}")

        # Fetch full pay run to get payslips
        run_detail = xero_get(f"/payroll.xro/1.0/PayRuns/{run_id}", tenant_id, access_token)
        slips = run_detail.get("PayRuns", [{}])[0].get("Payslips", [])

        # Find payslips with zero earnings
        empty_slip_ids = set()
        for s in slips:
            if "PayslipID" not in s:
                continue
            earnings = sum(
                l.get("NumberOfUnits", 0) * l.get("RatePerUnit", 1)
                for l in s.get("EarningsLines", [])
            )
            if earnings == 0:
                empty_slip_ids.add(s["PayslipID"])

        print(f"  {len(empty_slip_ids)} payslips with zero earnings (will fill these only)")

        # Fetch employees and their PayTemplate rates
        emp_data  = xero_get("/payroll.xro/1.0/Employees", tenant_id, access_token)
        employees = emp_data.get("Employees", [])

        slip_to_emp  = {s["PayslipID"]: s["EmployeeID"] for s in slips if "PayslipID" in s}
        emp_to_slip  = {s["EmployeeID"]: s["PayslipID"] for s in slips if "PayslipID" in s}

        emp_id_map = {}
        for e in employees:
            full   = f"{e.get('FirstName','')} {e.get('LastName','')}".strip()
            emp_id = e["EmployeeID"]
            emp_id_map[norm(full)] = emp_id

        # Build payslip data
        payslip_list, _ = build_payslip_list(emp_id_map, hours, perf, rates, bonuses, loc_nt=loc_nt, loc_qld=loc_qld)

        def clean(ps):
            return {k: v for k, v in ps.items() if not k.startswith("_")}

        filled = 0
        skipped = 0
        for ps in payslip_list:
            slip_id = emp_to_slip.get(ps["EmployeeID"])
            if not slip_id:
                continue
            if slip_id not in empty_slip_ids:
                print(f"  SKIP (already filled): {ps['_name']}")
                skipped += 1
                continue

            # Do NOT inject RatePerUnit for hourly lines — Xero auto-generated the
            # payslip from PayTemplate so penalty rates are already stored correctly.
            # Only tips/commission/bonus carry RatePerUnit (set in build_payslip_list).
            earnings = [dict(line) for line in clean(ps)["EarningsLines"]]

            print(f"  Writing: {ps['_name']}...")
            try:
                xero_post(f"/payroll.xro/1.0/Payslip/{slip_id}", tenant_id, access_token, [{
                    "PayslipID":     slip_id,
                    "EarningsLines": earnings,
                }])
                filled += 1
            except Exception as e:
                print(f"  ERROR: {ps['_name']}: {e}")

        # Zero out payslips for employees with no Fresha hours
        filled_ids = {ps["EmployeeID"] for ps in payslip_list}
        zeroed = 0
        for emp_id, slip_id in emp_to_slip.items():
            if emp_id in filled_ids:
                continue
            xero_name = next((n for n, eid in emp_id_map.items() if eid == emp_id), "")
            if xero_name in EXCLUDED_EMPLOYEES:
                continue
            print(f"  Zeroing (no hours): {xero_name} ({slip_id})")
            try:
                xero_post(f"/payroll.xro/1.0/Payslip/{slip_id}", tenant_id, access_token, [{
                    "PayslipID":     slip_id,
                    "EarningsLines": [],
                }])
                zeroed += 1
            except Exception as e:
                print(f"  ERROR zeroing {xero_name}: {e}")

        print(f"\n  Done — filled: {filled}  skipped (already had data): {skipped}  zeroed: {zeroed}")

    print("\n\nComplete.")


if __name__ == "__main__":
    main()
