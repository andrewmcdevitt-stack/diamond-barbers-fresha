import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Diamond Barbers Dashboard",
    page_icon="💈",
    layout="wide",
)

DATA_FILE = Path(__file__).parent.parent / "data" / "performance_summary.json"


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    if not DATA_FILE.exists():
        return []
    with open(DATA_FILE, "r") as f:
        data = json.load(f)
    return data if isinstance(data, list) else [data]


def safe_num(val, default=0):
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def safe_int(val, default=0):
    try:
        return int(val) if val is not None else default
    except (TypeError, ValueError):
        return default


# ── UI ────────────────────────────────────────────────────────────────────────
st.title("💈 Diamond Barbers — Performance Dashboard")

history = load_data()

if not history:
    st.info("No data yet. The agent runs every Monday at 6:00 AM Darwin time and will populate this dashboard automatically.")
    st.stop()

# Filter out entries that failed (raw_output only)
valid = [r for r in history if "gross_sales" in r]
if not valid:
    st.warning("Data exists but could not be parsed correctly. Check the agent logs.")
    st.stop()

latest = valid[-1]
period_start = latest.get("period_start", "—")
period_end = latest.get("period_end", "—")
report_date = latest.get("report_date", "—")

st.caption(f"Report period: **{period_start}** → **{period_end}** · Last fetched: {report_date}")

st.divider()

# ── KPI row ───────────────────────────────────────────────────────────────────
st.subheader("Weekly Overview")
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

prev = valid[-2] if len(valid) >= 2 else None

def delta(key, fmt="currency"):
    if prev is None:
        return None
    cur = safe_num(latest.get(key))
    old = safe_num(prev.get(key))
    if old == 0:
        return None
    diff = cur - old
    if fmt == "currency":
        return f"${diff:+,.2f} vs prev week"
    return f"{diff:+} vs prev week"

kpi1.metric(
    "Gross Sales",
    f"${safe_num(latest.get('gross_sales')):,.2f}",
    delta("gross_sales"),
)
kpi2.metric(
    "Net Sales",
    f"${safe_num(latest.get('net_sales')):,.2f}",
    delta("net_sales"),
)
kpi3.metric(
    "Appointments",
    safe_int(latest.get("total_appointments")),
    delta("total_appointments", "int"),
)
kpi4.metric(
    "New Clients",
    safe_int(latest.get("new_clients")),
    delta("new_clients", "int"),
)
kpi5.metric(
    "Returning Clients",
    safe_int(latest.get("returning_clients")),
    delta("returning_clients", "int"),
)

st.divider()

# ── Revenue trend (multi-week) ────────────────────────────────────────────────
if len(valid) > 1:
    st.subheader("Revenue Trend")
    trend_df = pd.DataFrame([
        {
            "Week ending": r.get("period_end", r.get("report_date", "?")),
            "Gross Sales": safe_num(r.get("gross_sales")),
            "Net Sales": safe_num(r.get("net_sales")),
        }
        for r in valid
    ])
    fig_trend = px.line(
        trend_df,
        x="Week ending",
        y=["Gross Sales", "Net Sales"],
        markers=True,
        labels={"value": "Amount ($)", "variable": ""},
        color_discrete_sequence=["#1f77b4", "#2ca02c"],
    )
    fig_trend.update_layout(hovermode="x unified", plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_trend, use_container_width=True)
    st.divider()

# ── Staff + Services row ──────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Staff Performance")
    staff = latest.get("staff", [])
    if staff:
        staff_df = pd.DataFrame(staff)
        staff_df["revenue"] = staff_df["revenue"].apply(safe_num)
        staff_df["appointments"] = staff_df["appointments"].apply(safe_int)
        staff_df = staff_df.sort_values("revenue", ascending=True)
        fig_staff = px.bar(
            staff_df,
            x="revenue",
            y="name",
            orientation="h",
            text="revenue",
            labels={"revenue": "Revenue ($)", "name": ""},
            color="revenue",
            color_continuous_scale="Blues",
        )
        fig_staff.update_traces(texttemplate="$%{text:,.2f}", textposition="outside")
        fig_staff.update_layout(coloraxis_showscale=False, plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_staff, use_container_width=True)

        st.dataframe(
            staff_df[["name", "revenue", "appointments"]]
            .rename(columns={"name": "Staff", "revenue": "Revenue ($)", "appointments": "Appointments"})
            .sort_values("Revenue ($)", ascending=False)
            .reset_index(drop=True),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No staff data in this report.")

with col_right:
    st.subheader("Services Breakdown")
    services = latest.get("services", [])
    if services:
        svc_df = pd.DataFrame(services)
        svc_df["revenue"] = svc_df["revenue"].apply(safe_num)
        svc_df["count"] = svc_df["count"].apply(safe_int)
        fig_svc = px.pie(
            svc_df,
            names="name",
            values="revenue",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_svc.update_traces(textinfo="label+percent")
        st.plotly_chart(fig_svc, use_container_width=True)

        st.dataframe(
            svc_df[["name", "revenue", "count"]]
            .rename(columns={"name": "Service", "revenue": "Revenue ($)", "count": "Bookings"})
            .sort_values("Revenue ($)", ascending=False)
            .reset_index(drop=True),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No service breakdown data in this report.")

st.divider()

# ── Appointments breakdown ────────────────────────────────────────────────────
st.subheader("Appointment Breakdown")
appt_cols = st.columns(4)
appt_cols[0].metric("Total", safe_int(latest.get("total_appointments")))
appt_cols[1].metric("Completed", safe_int(latest.get("completed_appointments")))
appt_cols[2].metric("Cancelled", safe_int(latest.get("cancelled_appointments")))
appt_cols[3].metric("No Shows", safe_int(latest.get("no_shows")))

st.divider()

# ── Financials detail ─────────────────────────────────────────────────────────
st.subheader("Financial Detail")
fin_cols = st.columns(4)
fin_cols[0].metric("Gross Sales", f"${safe_num(latest.get('gross_sales')):,.2f}")
fin_cols[1].metric("Discounts", f"${safe_num(latest.get('discounts')):,.2f}")
fin_cols[2].metric("Taxes", f"${safe_num(latest.get('taxes')):,.2f}")
fin_cols[3].metric("Tips", f"${safe_num(latest.get('tips')):,.2f}")

st.divider()

# ── Full history table ────────────────────────────────────────────────────────
with st.expander("View all weekly history"):
    history_df = pd.DataFrame([
        {
            "Week ending": r.get("period_end", r.get("report_date")),
            "Gross Sales": safe_num(r.get("gross_sales")),
            "Net Sales": safe_num(r.get("net_sales")),
            "Appointments": safe_int(r.get("total_appointments")),
            "New Clients": safe_int(r.get("new_clients")),
            "Returning Clients": safe_int(r.get("returning_clients")),
        }
        for r in valid
    ])
    st.dataframe(history_df.sort_values("Week ending", ascending=False).reset_index(drop=True),
                 hide_index=True, use_container_width=True)

st.caption("Diamond Barbers · Auto-refreshes every 5 minutes · Data updated every Monday 6:00 AM Darwin time")
