import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="Diamond Barbers Dashboard",
    page_icon="💈",
    layout="wide",
)

DATA_FILE = Path(__file__).parent.parent / "data" / "performance_summary.json"


@st.cache_data(ttl=300)
def load_data():
    if not DATA_FILE.exists():
        return []
    with open(DATA_FILE, "r") as f:
        data = json.load(f)
    records = data if isinstance(data, list) else [data]
    return [r for r in records if "sales_summary" in r]


def fmt_currency(val):
    try:
        return f"${float(val):,.2f}"
    except (TypeError, ValueError):
        return "$0.00"


def fmt_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


st.title("💈 Diamond Barbers — Weekly Performance")

history = load_data()

if not history:
    st.info("No data yet. The report runs every Monday at 6:00 AM Darwin time.")
    st.stop()

week_labels = [
    f"{r.get('period_start', '?')} → {r.get('period_end', '?')}"
    for r in history
]
selected = st.selectbox("Select week", options=week_labels[::-1], index=0)
latest = history[week_labels.index(selected)]

period_start = latest.get("period_start", "—")
period_end = latest.get("period_end", "—")
report_date = latest.get("report_date", "—")
st.caption(f"Week: **{period_start}** to **{period_end}** · Fetched: {report_date}")

st.divider()

sales = latest.get("sales_summary", {})
appts = latest.get("appointments", {})
perf = latest.get("sales_performance", {})

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Sales", fmt_currency(sales.get("total_sales")))
col2.metric("Total + Tips & Charges", fmt_currency(sales.get("total_sales_and_other")))
col3.metric("Appointments", fmt_int(appts.get("total")))
col4.metric("Avg Service Value", fmt_currency(perf.get("avg_service_value")))
col5.metric("Tips", fmt_currency(sales.get("tips")))

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Sales Breakdown")
    sales_items = {
        "Services": sales.get("services", 0),
        "Service Add-ons": sales.get("service_addons", 0),
        "Products": sales.get("products", 0),
        "Memberships": sales.get("memberships", 0),
        "Late Cancellation Fees": sales.get("late_cancellation_fees", 0),
        "No-Show Fees": sales.get("no_show_fees", 0),
        "Service Charges": sales.get("service_charges", 0),
        "Tips": sales.get("tips", 0),
    }
    sales_df = pd.DataFrame(
        [(k, float(v)) for k, v in sales_items.items() if float(v or 0) > 0],
        columns=["Category", "Amount"]
    )
    fig = px.bar(
        sales_df,
        x="Amount", y="Category",
        orientation="h",
        text="Amount",
        color="Amount",
        color_continuous_scale="Blues",
    )
    fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, plot_bgcolor="rgba(0,0,0,0)", xaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Appointments")
    c1, c2 = st.columns(2)
    c1.metric("Total", fmt_int(appts.get("total")))
    c2.metric("Online", f"{fmt_int(appts.get('online'))} ({appts.get('pct_online', 0):.1f}%)")
    c3, c4 = st.columns(2)
    c3.metric("Cancelled", f"{fmt_int(appts.get('cancelled'))} ({appts.get('pct_cancelled', 0):.1f}%)")
    c4.metric("No-Shows", f"{fmt_int(appts.get('no_shows'))} ({appts.get('pct_no_show', 0):.1f}%)")

    st.subheader("Sales Performance")
    p1, p2 = st.columns(2)
    p1.metric("Services Sold", fmt_int(perf.get("services_sold")))
    p2.metric("Avg Service Value", fmt_currency(perf.get("avg_service_value")))
    p3, p4 = st.columns(2)
    p3.metric("Products Sold", fmt_int(perf.get("products_sold")))
    p4.metric("Avg Product Value", fmt_currency(perf.get("avg_product_value")))

st.divider()

st.subheader("Staff Performance")

staff = latest.get("staff", [])
if staff:
    staff_df = pd.DataFrame(staff)
    staff_df = staff_df.sort_values("total_sales", ascending=True)

    fig_staff = px.bar(
        staff_df,
        x="total_sales", y="name",
        orientation="h",
        text="total_sales",
        color="total_sales",
        color_continuous_scale="Blues",
        labels={"total_sales": "Total Sales ($)", "name": ""},
    )
    fig_staff.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
    fig_staff.update_layout(coloraxis_showscale=False, plot_bgcolor="rgba(0,0,0,0)", height=500)
    st.plotly_chart(fig_staff, use_container_width=True)

    display_df = staff_df[["name", "total_sales", "services", "products", "tips", "total_appts", "cancelled_appts", "no_show_appts", "services_sold"]].copy()
    display_df = display_df.sort_values("total_sales", ascending=False).reset_index(drop=True)
    display_df.columns = ["Staff", "Total Sales", "Services", "Products", "Tips", "Appts", "Cancelled", "No-Shows", "Services Sold"]
    for col in ["Total Sales", "Services", "Products", "Tips"]:
        display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}")
    st.dataframe(display_df, hide_index=True, use_container_width=True)

st.divider()

if len(history) > 1:
    st.subheader("Weekly Trend")
    trend_data = []
    for r in history:
        s = r.get("sales_summary", {})
        trend_data.append({
            "Week ending": r.get("period_end", r.get("report_date")),
            "Total Sales": float(s.get("total_sales", 0) or 0),
        })
    trend_df = pd.DataFrame(trend_data).sort_values("Week ending")
    fig_trend = px.line(
        trend_df, x="Week ending", y="Total Sales",
        markers=True,
        labels={"Total Sales": "Total Sales ($)"},
    )
    fig_trend.update_layout(plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_trend, use_container_width=True)

st.caption("Diamond Barbers · Auto-refreshes every 5 minutes · Updated every Monday 6:00 AM Darwin time")
