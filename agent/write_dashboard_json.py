"""
write_dashboard_json.py
-----------------------
Reads the most recent week's GHL Weekly Payroll + Location Performance records
and writes JSON snapshots in the format the dashboard already reads:
  data/performance_summary.json       (NT: Darwin + Parap)
  data/cairns_performance_summary.json (QLD: Cairns)

Run AFTER fetch_performance.py.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"

GHL_API_KEY     = os.environ["GHL_API_KEY"]
GHL_LOCATION_ID = os.environ["GHL_LOCATION_ID"]
GHL_BASE        = "https://services.leadconnectorhq.com"
GHL_HEADERS     = {
    "Authorization": f"Bearer {GHL_API_KEY}",
    "Version":       "2021-07-28",
    "Accept":        "application/json",
    "Content-Type":  "application/json",
}

# Which xero_org values belong to which dashboard file
NT_ORGS     = {"Diamond Barbers Darwin", "Diamond Barbers Parap"}
CAIRNS_ORGS = {"Diamond Barbers Cairns"}


def ghl_get_all_records(object_key):
    """Paginate through all GHL custom object records."""
    all_records = []
    page = 1
    while True:
        r = requests.post(
            f"{GHL_BASE}/objects/{object_key}/records/search",
            headers=GHL_HEADERS,
            json={
                "locationId": GHL_LOCATION_ID,
                "page":       page,
                "pageLimit":  100,
            },
        )
        if r.status_code not in (200, 201):
            print(f"  WARNING: {object_key} page {page} returned {r.status_code}: {r.text[:100]}")
            break
        data    = r.json()
        records = data.get("records", [])
        all_records.extend(records)
        if len(records) < 100:
            break
        page += 1
    return all_records


def build_performance_doc(week_start, week_end, employees, locations):
    """
    Build a dict in the format ghl_dashboard.html already reads.
    Reconstructs inc-GST services and products from the stored ex-GST values.
    """
    staff_list = []
    for e in employees:
        svc_ex    = round(e.get("service_sales_exc_gst") or 0, 2)
        prod_ex   = round(((e.get("commissions") or 0) / 0.10 * 0.9) if (e.get("commissions") or 0) > 0 else 0, 2)
        tips      = round(e.get("tips") or 0, 2)
        total_sls = round(svc_ex + prod_ex + tips, 2)
        staff_list.append({
            "name":            e.get("name", ""),
            "services":        svc_ex,
            "products":        prod_ex,
            "tips":            tips,
            "total_sales":     total_sls,
            "total_appts":     0,
            "cancelled_appts": 0,
            "occupancy_pct":   e.get("occupancy_rate") or 0,
        })

    # Sort by total_sales descending (matches dashboard sort)
    staff_list.sort(key=lambda s: s["total_sales"], reverse=True)

    # Aggregate sales_summary
    total_svc  = round(sum(s["services"] for s in staff_list), 2)
    total_prod = round(sum(s["products"] for s in staff_list), 2)
    total_tips = round(sum(s["tips"] for s in staff_list), 2)
    total_all  = round(total_svc + total_prod + total_tips, 2)

    # Build location cards
    loc_list = []
    for loc in locations:
        svc_ex   = round(loc.get("services_ex_gst") or 0, 2)
        prod_ex  = round(((loc.get("commissions") or 0) / 0.10 * 0.9) if (loc.get("commissions") or 0) > 0 else 0, 2)
        loc_list.append({
            "name":          loc.get("name", ""),
            "services":      svc_ex,
            "products":      prod_ex,
            "total_sales":   round(svc_ex + prod_ex, 2),
            "occupancy_pct": loc.get("occupancy_rate") or 0,
        })

    return {
        "period_start": week_start,
        "period_end":   week_end,
        "sales_summary": {
            "services":               total_svc,
            "products":               total_prod,
            "tips":                   total_tips,
            "total_sales":            round(total_svc + total_prod, 2),
            "total_sales_and_other":  total_all,
            "late_cancellation_fees": 0,
            "no_show_fees":           0,
            "service_addons":         0,
        },
        "appointments": {},
        "sales_performance": {
            "avg_service_value": 0,
            "services_sold":     0,
            "products_sold":     0,
        },
        "staff":     staff_list,
        "locations": loc_list,
    }


def run():
    print("Reading GHL payroll records...")
    payroll_records = ghl_get_all_records("custom_objects.payroll")
    print(f"  Found {len(payroll_records)} payroll records.")

    print("Reading GHL location performance records...")
    loc_records = ghl_get_all_records("custom_objects.location_performance")
    print(f"  Found {len(loc_records)} location records.")

    if not payroll_records:
        print("No payroll records — nothing to write.")
        return

    # Most recent week
    weeks = sorted(set(
        rec.get("properties", {}).get("week_start", "")
        for rec in payroll_records
        if rec.get("properties", {}).get("week_start")
    ), reverse=True)

    if not weeks:
        print("No week_start values found.")
        return

    latest_week = weeks[0]
    week_end    = next(
        (rec.get("properties", {}).get("week_end", "")
         for rec in payroll_records
         if rec.get("properties", {}).get("week_start") == latest_week),
        ""
    )
    print(f"Latest week: {latest_week} to {week_end}")

    # Filter payroll records for that week
    week_payroll = [
        rec for rec in payroll_records
        if rec.get("properties", {}).get("week_start") == latest_week
    ]

    # Build employee dicts
    def to_emp(rec):
        p = rec.get("properties", {})
        return {
            "name":                  p.get("employee_name", ""),
            "xero_org":              p.get("xero_org", ""),
            "service_sales_exc_gst": float(p.get("service_sales_exc_gst") or 0),
            "commissions":           float(p.get("commissions") or 0),
            "tips":                  float(p.get("tips") or 0),
            "occupancy_rate":        float(p.get("occupancy_rate") or 0),
        }

    all_emps = [to_emp(r) for r in week_payroll]

    nt_emps     = [e for e in all_emps if e["xero_org"] in NT_ORGS]
    cairns_emps = [e for e in all_emps if e["xero_org"] in CAIRNS_ORGS]

    # Filter location records for that week
    week_locs = [
        rec for rec in loc_records
        if rec.get("properties", {}).get("week_start") == latest_week
    ]

    def to_loc(rec):
        p = rec.get("properties", {})
        return {
            "name":            p.get("location_name", ""),
            "location":        p.get("location", ""),
            "services_ex_gst": float(p.get("services_ex_gst") or 0),
            "commissions":     float(p.get("commissions") or 0),
            "occupancy_rate":  float(p.get("occupancy_rate") or 0),
        }

    all_locs = [to_loc(r) for r in week_locs]

    nt_locs     = [l for l in all_locs if l["location"] in NT_ORGS]
    cairns_locs = [l for l in all_locs if l["location"] in CAIRNS_ORGS]

    # Build and write NT file
    nt_doc = build_performance_doc(latest_week, week_end, nt_emps, nt_locs)
    nt_path = DATA_DIR / "performance_summary.json"
    # Load existing history and prepend / update
    existing_nt = []
    if nt_path.exists():
        try:
            existing_nt = json.loads(nt_path.read_text())
            if isinstance(existing_nt, dict):
                existing_nt = [existing_nt]
        except Exception:
            existing_nt = []
    # Replace record for this week if it exists, else prepend
    existing_nt = [r for r in existing_nt if r.get("period_start") != latest_week]
    existing_nt.insert(0, nt_doc)
    nt_path.write_text(json.dumps(existing_nt, indent=2))
    print(f"Written NT: {len(nt_emps)} staff, {len(nt_locs)} locations -> {nt_path.name}")

    # Build and write Cairns file
    cairns_doc = build_performance_doc(latest_week, week_end, cairns_emps, cairns_locs)
    cairns_path = DATA_DIR / "cairns_performance_summary.json"
    existing_cairns = []
    if cairns_path.exists():
        try:
            existing_cairns = json.loads(cairns_path.read_text())
            if isinstance(existing_cairns, dict):
                existing_cairns = [existing_cairns]
        except Exception:
            existing_cairns = []
    existing_cairns = [r for r in existing_cairns if r.get("period_start") != latest_week]
    existing_cairns.insert(0, cairns_doc)
    cairns_path.write_text(json.dumps(existing_cairns, indent=2))
    print(f"Written Cairns: {len(cairns_emps)} staff, {len(cairns_locs)} locations -> {cairns_path.name}")


if __name__ == "__main__":
    run()
