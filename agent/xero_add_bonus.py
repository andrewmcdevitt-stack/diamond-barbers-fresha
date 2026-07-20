"""
xero_add_bonus.py
-----------------
Adds Night Markets 50-50 bonus earnings lines to existing draft payslips.
Run AFTER the mobile shortcut has already pushed hours/tips/commissions.

Reads:  data/night_markets_bonus.json  (written by weekly_sync.py)
        data/xero_token.json

Run with:  python agent/xero_add_bonus.py
"""

import json
import urllib.error
from pathlib import Path

from xero_payrun import refresh_token, xero_get, xero_post, norm

DATA_DIR   = Path(__file__).parent.parent / "data"
TOKEN_FILE = DATA_DIR / "xero_token.json"
BONUS_FILE = DATA_DIR / "night_markets_bonus.json"

# Bonus earnings rate IDs per org
BONUS_RATES = {
    "Diamond Barbers Pty Ltd":        "7cd33337-ad09-42d6-b83b-ae75637afe3f",
    "DIAMOND BARBERS CAIRNS PTY LTD": "1f61ce7e-2e5b-43e6-85d4-55f9e194a9e5",
}

SKIP_ORGS = {"DB WULGURU PTY LTD", "D.B. Parap Pty Ltd"}


def main():
    if not BONUS_FILE.exists():
        print("No night_markets_bonus.json found — nothing to do.")
        return

    bonuses = {norm(k): round(float(v["bonus"]), 2)
               for k, v in json.loads(BONUS_FILE.read_text()).items()}
    print(f"Loaded bonuses for {len(bonuses)} staff: {list(bonuses.keys())}")

    token        = refresh_token(json.loads(TOKEN_FILE.read_text()))
    access_token = token["access_token"]
    tenants      = token.get("tenants", [])

    for tenant in tenants:
        name      = tenant["name"]
        tenant_id = tenant["id"]

        if name in SKIP_ORGS:
            continue
        bonus_rate_id = BONUS_RATES.get(name)
        if not bonus_rate_id:
            continue

        print(f"\n{'='*60}\nORG: {name}\n{'='*60}")

        # Find draft payrun
        runs  = xero_get("/payroll.xro/1.0/PayRuns", tenant_id, access_token)
        draft = next((r for r in runs.get("PayRuns", []) if r.get("PayRunStatus") == "DRAFT"), None)
        if not draft:
            print("  No DRAFT pay run found — skipping.")
            continue

        run_id = draft["PayRunID"]
        detail = xero_get(f"/payroll.xro/1.0/PayRuns/{run_id}", tenant_id, access_token)
        slips  = detail.get("PayRuns", [{}])[0].get("Payslips", [])

        # Build name → payslip ID map
        employees = xero_get("/payroll.xro/1.0/Employees", tenant_id, access_token).get("Employees", [])
        emp_id_to_name = {
            e["EmployeeID"]: norm(f"{e.get('FirstName','')} {e.get('LastName','')}".strip())
            for e in employees
        }
        slip_map = {emp_id_to_name.get(s["EmployeeID"], ""): s["PayslipID"]
                    for s in slips if "PayslipID" in s}

        for emp_norm, bonus in bonuses.items():
            slip_id = slip_map.get(emp_norm)
            if not slip_id:
                print(f"  SKIP  {emp_norm:30s}  (no payslip in this org)")
                continue

            try:
                xero_post(f"/payroll.xro/1.0/Payslip/{slip_id}", tenant_id, access_token, [{
                    "PayslipID":     slip_id,
                    "EarningsLines": [
                        {"EarningsRateID": bonus_rate_id, "NumberOfUnits": 1, "RatePerUnit": bonus}
                    ],
                }])
                print(f"  OK    {emp_norm:30s}  bonus=${bonus:.2f}")
            except Exception as e:
                print(f"  ERROR {emp_norm}: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
