import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="Diamond Barbers",
    page_icon="💈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Dark colour palette ────────────────────────────────────────────────────────
BG       = "#000000"   # black background
CARD     = "#1A1A1A"   # dark grey cards
BORDER   = "#2E2E2E"   # subtle dark border
TEXT     = "#FFFFFF"   # white body text
MUTED    = "#9B9B9B"   # muted grey
PURPLE   = "#7B7BFF"   # purple accent
PURPLE_L = "#9B9BE8"   # lighter purple
GREEN_BG = "#0D2E1A"
GREEN_FG = "#34D399"
RED_BG   = "#2E0D0D"
RED_FG   = "#F87171"
GREY_BG  = "#2A2A2A"
GREY_FG  = "#9B9B9B"
WARN_FG  = "#FBBF24"
WARN_BG  = "#2E1F00"
GRID     = "#2E2E2E"

DATA_FILE = Path(__file__).parent.parent / "data" / "performance_summary.json"
LOGO_FILE = Path(__file__).parent / "logo.png"

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
/* ── Base ── */
.stApp,
.stApp > div,
.appview-container,
.stAppViewBlockContainer,
section.main,
.main,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewBlockContainer"],
[data-testid="stMainBlockContainer"],
[data-testid="stMain"],
[data-testid="stBottom"],
[data-testid="stHeader"] {{
    background: #000000 !important;
    background-color: #000000 !important;
    border: none !important;
    box-shadow: none !important;
    border-radius: 0 !important;
}}
section[data-testid="stSidebar"] {{ display: none !important; }}
#MainMenu, footer, header {{ visibility: hidden !important; }}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body, p, span, div, label, td, th {{
    color: {TEXT};
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}}

.main .block-container {{
    padding-top: 2rem !important;
    padding-left: 2.5rem !important;
    padding-right: 2.5rem !important;
    padding-bottom: 2rem !important;
    max-width: 100% !important;
}}

/* ── Page header ── */
.db-page-title {{
    font-size: 1.6rem;
    font-weight: 700;
    color: {TEXT};
    line-height: 1.2;
    margin-bottom: 0.1rem;
}}
.db-page-sub {{
    font-size: 0.82rem;
    color: {MUTED};
}}

/* ── Filter pill buttons (decorative, matching Fresha style) ── */
.db-filter-row {{
    display: flex;
    gap: 0.5rem;
    margin: 1rem 0 1.5rem;
    flex-wrap: wrap;
}}
.db-filter-pill {{
    background: {CARD};
    border: 1px solid {BORDER};
    border-radius: 20px;
    padding: 0.35rem 0.85rem;
    font-size: 0.8rem;
    color: {TEXT};
    font-weight: 500;
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    white-space: nowrap;
}}

/* ── White card ── */
.db-card {{
    background: {CARD};
    border: 1px solid {BORDER};
    border-radius: 16px;
    padding: 1.4rem 1.5rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
    margin-bottom: 0.9rem;
}}

/* ── Card title row ── */
.db-card-header {{
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 0.9rem;
}}
.db-card-title {{
    font-size: 0.95rem;
    font-weight: 700;
    color: {TEXT};
}}
.db-view-link {{
    font-size: 0.82rem;
    font-weight: 500;
    color: {PURPLE};
    text-decoration: none;
    white-space: nowrap;
}}

/* ── Big metric number ── */
.db-big-num {{
    font-size: 2.4rem;
    font-weight: 700;
    color: {TEXT};
    letter-spacing: -0.03em;
    line-height: 1.1;
    margin-bottom: 0.4rem;
}}

/* ── Change badge (green / red / grey / amber) ── */
.chg-pill {{
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
    border-radius: 20px;
    padding: 0.18rem 0.55rem;
    font-size: 0.72rem;
    font-weight: 600;
    white-space: nowrap;
}}
.chg-pill.up   {{ background: {GREEN_BG}; color: {GREEN_FG}; }}
.chg-pill.down {{ background: {RED_BG};   color: {RED_FG};   }}
.chg-pill.flat {{ background: {GREY_BG};  color: {GREY_FG};  }}

/* ── Breakdown rows inside card ── */
.bd-row {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.55rem 0;
    border-bottom: 1px solid {BORDER};
    font-size: 0.85rem;
}}
.bd-row:last-child {{ border-bottom: none; }}
.bd-label {{ color: {MUTED}; }}
.bd-value {{ font-weight: 600; color: {TEXT}; margin-right: 0.5rem; }}

/* ── KPI stat card (top row) ── */
.kpi-card {{
    background: {CARD};
    border: 1px solid {BORDER};
    border-radius: 16px;
    padding: 1.3rem 1.5rem 1.2rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
}}
.kpi-label {{
    font-size: 0.82rem;
    font-weight: 700;
    color: {TEXT};
    margin-bottom: 0.5rem;
    display: flex;
    align-items: center;
    gap: 0.3rem;
}}
.kpi-big {{
    font-size: 1.9rem;
    font-weight: 700;
    color: {TEXT};
    letter-spacing: -0.02em;
    line-height: 1.1;
    margin-bottom: 0.35rem;
}}
.kpi-sub {{
    font-size: 0.75rem;
    color: {MUTED};
}}

/* ── Occupancy big % ── */
.occ-big {{
    font-size: 2rem;
    font-weight: 700;
    letter-spacing: -0.02em;
    line-height: 1.1;
    margin-bottom: 0.35rem;
}}

/* ── Staff table ── */
.table-wrap {{ overflow-x: auto; -webkit-overflow-scrolling: touch; }}
.staff-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85rem;
}}
.staff-table thead tr {{
    border-bottom: 2px solid {BORDER};
}}
.staff-table thead th {{
    color: {MUTED};
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    padding: 0.5rem 0.85rem;
    text-align: left;
    white-space: nowrap;
    background: transparent;
}}
.staff-table thead th.r {{ text-align: right; }}
.staff-table tbody tr {{
    border-bottom: 1px solid {BORDER};
    transition: background 0.1s;
}}
.staff-table tbody tr:last-child {{ border-bottom: none; }}
.staff-table tbody tr:hover {{ background: #FAFAFA; }}
.staff-table tbody td {{
    padding: 0.65rem 0.85rem;
    color: {TEXT};
    white-space: nowrap;
}}
.staff-table tbody td.r {{
    text-align: right;
    font-variant-numeric: tabular-nums;
}}

/* ── Rank pills ── */
.rank-pill {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    background: {GREY_BG};
    border-radius: 6px;
    padding: 0.12rem 0.45rem;
    font-size: 0.7rem;
    font-weight: 700;
    color: {GREY_FG};
    min-width: 2rem;
}}
.rank-pill.r1 {{ background: #EEF2FF; color: {PURPLE}; }}
.rank-pill.r2 {{ background: {GREEN_BG}; color: {GREEN_FG}; }}
.rank-pill.r3 {{ background: {WARN_BG}; color: {WARN_FG}; }}

/* ── Occupancy pill ── */
.occ-pill {{
    display: inline-block;
    border-radius: 20px;
    padding: 0.18rem 0.6rem;
    font-size: 0.72rem;
    font-weight: 600;
    white-space: nowrap;
}}

/* ── Appointments mini grid ── */
.appt-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.5rem;
    margin-top: 0.5rem;
}}
.appt-cell {{
    background: #FAFAFA;
    border: 1px solid {BORDER};
    border-radius: 10px;
    padding: 0.75rem;
    text-align: center;
}}
.appt-label {{
    font-size: 0.68rem;
    color: {MUTED};
    text-transform: uppercase;
    letter-spacing: 0.07em;
    margin-bottom: 0.2rem;
}}
.appt-val {{
    font-size: 1.15rem;
    font-weight: 700;
    color: {TEXT};
}}
.appt-pct {{
    font-size: 0.68rem;
    color: {MUTED};
    margin-top: 0.1rem;
}}

/* ── Selectbox ── */
.stSelectbox {{ margin-top: 1rem !important; }}
.stSelectbox [data-baseweb="select"] {{
    background: {CARD} !important;
    border: 1px solid {BORDER} !important;
    border-radius: 20px !important;
}}
.stSelectbox [data-baseweb="select"] > div {{
    background: {CARD} !important;
}}
.stSelectbox [data-baseweb="select"] * {{ color: {TEXT} !important; }}
.stSelectbox label {{ color: {MUTED} !important; font-size: 0.72rem !important; }}

/* ── Charts ── */
[data-testid="stPlotlyChart"] > div {{
    background: transparent !important;
    border: none !important;
    padding: 0 !important;
}}

/* ── Column gaps ── */
[data-testid="column"] {{ padding: 0 0.4rem !important; }}
[data-testid="column"]:first-child {{ padding-left: 0 !important; }}
[data-testid="column"]:last-child {{ padding-right: 0 !important; }}

/* ── Logo ── */
[data-testid="stImage"] img {{ max-height: 192px; width: auto; }}
[data-testid="stImage"] {{ margin: 0 !important; padding: 0 !important; }}

.occ-card-marker {{ display: none; }}
.trend-card-marker {{ display: none; }}

/* ── Occupancy chart card ── */
[data-testid="stVerticalBlock"]:has(.occ-card-marker) {{
    background: {CARD} !important;
    border: 1px solid {BORDER} !important;
    border-radius: 16px !important;
    padding: 1.4rem 1.5rem !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04) !important;
}}

/* ── Trend chart card ── */
[data-testid="stVerticalBlock"]:has(.trend-card-marker) {{
    background: {CARD} !important;
    border: 1px solid {BORDER} !important;
    border-radius: 16px !important;
    padding: 1.4rem 1.5rem !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04) !important;
}}

/* ── Responsive ── */
@media (max-width: 900px) {{
    .main .block-container {{
        padding-left: 1rem !important;
        padding-right: 1rem !important;
    }}
    .appt-grid {{ grid-template-columns: 1fr 1fr; }}
}}
</style>
""", unsafe_allow_html=True)


# ── Helpers ────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    if not DATA_FILE.exists():
        return []
    with open(DATA_FILE, "r") as f:
        data = json.load(f)
    records = data if isinstance(data, list) else [data]
    return [r for r in records if "sales_summary" in r]

def c2(val):
    try:    return f"A$ {float(val):,.2f}"
    except: return "A$ 0.00"

def c(val):
    try:    return f"A$ {float(val):,.2f}"
    except: return "A$ 0.00"

def pct(val):
    try:    return f"{float(val):.1f}%"
    except: return "—"

def n(val):
    try:    return int(val)
    except: return 0

def fmt_date_long(d):
    try:
        from datetime import datetime
        dt = datetime.strptime(d, "%Y-%m-%d")
        return f"{dt.day} {dt.strftime('%B')} {dt.year}"
    except:
        return d or "—"

def occ_color(val):
    try:
        v = float(val)
        if v >= 80: return PURPLE
        if v >= 65: return WARN_FG
        return RED_FG
    except:
        return MUTED

def occ_badge(val):
    try:
        v   = float(val)
        col = occ_color(v)
        if v >= 80:   bg = "#EEF2FF"
        elif v >= 65: bg = WARN_BG
        else:         bg = RED_BG
        return f"<span class='occ-pill' style='background:{bg};color:{col};'>{v:.1f}%</span>"
    except:
        return "—"

def rank_pill(i):
    cls = {1: "r1", 2: "r2", 3: "r3"}.get(i, "")
    return f"<span class='rank-pill {cls}'>#{i}</span>"


# ── Load ──────────────────────────────────────────────────────────────────────
history = load_data()

if not history:
    st.title("💈 Diamond Barbers")
    st.info("No data yet. The report runs every Monday at 6:00 AM Darwin time.")
    st.stop()

reversed_history = list(reversed(history))

# ── Page header ────────────────────────────────────────────────────────────────
if LOGO_FILE.exists():
    st.image(str(LOGO_FILE))
st.markdown(
    "<div class='db-page-title'>Performance dashboard</div>"
    "<div class='db-page-sub'>Weekly performance report for Diamond Barbers</div>",
    unsafe_allow_html=True,
)

# ── Week selector ───────────────────────────────────────────────────────────────
sel_col, _ = st.columns([2, 3])
with sel_col:
    selected_idx = st.selectbox(
        "Select week",
        options=range(len(reversed_history)),
        format_func=lambda i: (
            f"{fmt_date_long(reversed_history[i].get('period_start','?'))}  →  "
            f"{fmt_date_long(reversed_history[i].get('period_end','?'))}"
        ),
        index=0,
        label_visibility="collapsed",
    )

# ── Selected data ──────────────────────────────────────────────────────────────
latest     = reversed_history[selected_idx]
sales      = latest.get("sales_summary", {})
appts      = latest.get("appointments", {})
perf       = latest.get("sales_performance", {})
staff_list = latest.get("staff", [])

ps           = fmt_date_long(latest.get("period_start", "—"))
pe           = fmt_date_long(latest.get("period_end", "—"))
period_label = f"{ps}  →  {pe}"

total_s  = float(sales.get("total_sales", 0) or 0)
inc_tips = float(sales.get("total_sales_and_other", 0) or 0)
net_svc  = float(sales.get("services", 0) or 0)
net_prod = float(sales.get("products", 0) or 0)
tips_val = float(sales.get("tips", 0) or 0)
avg_svc  = float(perf.get("avg_service_value", 0) or 0)
canc_fee = float(sales.get("late_cancellation_fees", 0) or 0)
noshow_f = float(sales.get("no_show_fees", 0) or 0)
svc_add  = float(sales.get("service_addons", 0) or 0)

occ_vals     = [float(s.get("occupancy_pct", 0) or 0) for s in staff_list if s.get("occupancy_pct")]
overall_occ  = sum(occ_vals) / len(occ_vals) if occ_vals else None
sorted_staff = sorted(staff_list, key=lambda s: float(s.get("total_sales", 0) or 0), reverse=True)
valid_trend  = [r for r in history if "sales_summary" in r]



# ══════════════════════════════════════════════════════════════════════════════
# TOP ROW — Net Service Sales | Net Product Sales | Overall Occupancy
# ══════════════════════════════════════════════════════════════════════════════
t1, t2, t3 = st.columns(3)

with t1:
    occ_col_top = occ_color(overall_occ) if overall_occ is not None else MUTED
    if overall_occ is not None:
        if overall_occ >= 80:   occ_bg_top = "#EEF2FF"
        elif overall_occ >= 65: occ_bg_top = WARN_BG
        else:                   occ_bg_top = RED_BG
    else:
        occ_bg_top = "#F3F4F6"

    occ_display = pct(overall_occ) if overall_occ is not None else "No data"

    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Net Service Sales</div>
        <div class="kpi-big">{c(net_svc)}</div>
        <div class="kpi-sub">{n(perf.get("services_sold"))} services sold &nbsp;·&nbsp; avg {c(avg_svc)}</div>
    </div>
    """, unsafe_allow_html=True)

with t2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Net Product Sales</div>
        <div class="kpi-big">{c(net_prod)}</div>
        <div class="kpi-sub">{n(perf.get("products_sold"))} products sold</div>
    </div>
    """, unsafe_allow_html=True)

with t3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Overall Occupancy</div>
        <div class="occ-big" style="color:{occ_col_top};">{occ_display}</div>
        <div>
            <span class="occ-pill" style="background:{occ_bg_top};color:{occ_col_top};font-size:0.75rem;">
                {"On target ≥ 80%" if (overall_occ or 0) >= 80 else ("Needs attention 65–79%" if (overall_occ or 0) >= 65 else "Below target < 65%")}
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# OCCUPANCY BAR CHART
# ══════════════════════════════════════════════════════════════════════════════
if occ_vals:
    occ_df = pd.DataFrame(staff_list)
    occ_df["occupancy_pct"] = pd.to_numeric(occ_df["occupancy_pct"], errors="coerce").fillna(0)
    occ_df = (occ_df[occ_df["occupancy_pct"] > 0]
              .sort_values("occupancy_pct", ascending=True)
              .reset_index(drop=True))
    total_o         = len(occ_df)
    occ_df["rank"]  = [total_o - i for i in range(total_o)]
    occ_df["label"] = occ_df.apply(
        lambda r: f"#{int(r['rank'])} {r['name'].split()[0]}", axis=1
    )
    bar_colors = [occ_color(v) for v in occ_df["occupancy_pct"]]

    fig_occ = go.Figure(go.Bar(
        x=occ_df["occupancy_pct"],
        y=occ_df["label"],
        orientation="h",
        marker_color=bar_colors,
        marker_line_width=0,
        text=[f"{v:.0f}%" for v in occ_df["occupancy_pct"]],
        textposition="inside",
        textfont=dict(color="#FFFFFF", size=11),
        cliponaxis=False,
    ))
    fig_occ.add_vline(x=80, line_dash="dot", line_color=PURPLE,  line_width=1.5)
    fig_occ.add_vline(x=65, line_dash="dot", line_color=WARN_FG, line_width=1.5)
    fig_occ.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor=CARD,
        font=dict(color=TEXT, size=11, family="sans-serif"),
        height=max(200, len(occ_df) * 36),
        xaxis=dict(
            range=[0, 100],
            showgrid=True, gridcolor=GRID,
            ticksuffix="%", color=MUTED,
            zeroline=False, tickfont=dict(size=10),
        ),
        yaxis=dict(color=TEXT, tickfont=dict(size=11)),
        margin=dict(l=10, r=20, t=10, b=10),
        bargap=0.3,
    )

    with st.container():
        st.markdown(f"""
        <span class="occ-card-marker"></span>
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem;">
            <div class="db-card-title">Occupancy rate</div>
            <span style="font-size:0.78rem;color:{MUTED};">{period_label}</span>
        </div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_occ, use_container_width=True, config={"displayModeBar": False})
        st.markdown(f"""
        <div style="display:flex;gap:1.2rem;padding:0.3rem 0 1.2rem;flex-wrap:wrap;">
            <span style="display:flex;align-items:center;gap:0.35rem;font-size:0.75rem;color:{MUTED};">
                <span style="width:10px;height:10px;border-radius:50%;background:{PURPLE};display:inline-block;"></span>
                On target (≥ 80%)
            </span>
            <span style="display:flex;align-items:center;gap:0.35rem;font-size:0.75rem;color:{MUTED};">
                <span style="width:10px;height:10px;border-radius:50%;background:{WARN_FG};display:inline-block;"></span>
                Needs attention (65–79%)
            </span>
            <span style="display:flex;align-items:center;gap:0.35rem;font-size:0.75rem;color:{MUTED};">
                <span style="width:10px;height:10px;border-radius:50%;background:{RED_FG};display:inline-block;"></span>
                Below target (< 65%)
            </span>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# MIDDLE ROW — Total Sales breakdown (left) + Sales trend chart (right)
# ══════════════════════════════════════════════════════════════════════════════
col_sales, col_trend = st.columns([2, 3])

with col_sales:
    st.markdown(f"""
    <div class="db-card">
        <div class="db-card-header">
            <div class="db-card-title">Total sales</div>
            <span class="db-view-link">View report</span>
        </div>
        <div class="db-big-num">{c2(total_s)}</div>
        <div style="margin-bottom:0.9rem;">
            <span class="chg-pill flat">↕ inc. tips &amp; charges: {c2(inc_tips)}</span>
        </div>
        <div class="bd-row">
            <span class="bd-label">Services</span>
            <span style="display:flex;align-items:center;gap:0.4rem;">
                <span class="bd-value">{c(net_svc)}</span>
            </span>
        </div>
        <div class="bd-row">
            <span class="bd-label">Products</span>
            <span style="display:flex;align-items:center;gap:0.4rem;">
                <span class="bd-value">{c(net_prod)}</span>
            </span>
        </div>
        <div class="bd-row">
            <span class="bd-label">Tips</span>
            <span style="display:flex;align-items:center;gap:0.4rem;">
                <span class="bd-value">{c(tips_val)}</span>
            </span>
        </div>
        <div class="bd-row">
            <span class="bd-label">No-show fees</span>
            <span style="display:flex;align-items:center;gap:0.4rem;">
                <span class="bd-value">{c(noshow_f)}</span>
            </span>
        </div>
        <div class="bd-row">
            <span class="bd-label">Cancellation fees</span>
            <span style="display:flex;align-items:center;gap:0.4rem;">
                <span class="bd-value">{c(canc_fee)}</span>
            </span>
        </div>
        <div class="bd-row">
            <span class="bd-label">Service add-ons</span>
            <span style="display:flex;align-items:center;gap:0.4rem;">
                <span class="bd-value">{c(svc_add)}</span>
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

with col_trend:
    if len(valid_trend) > 1:
        trend_df = pd.DataFrame([{
            "Week": r.get("period_end", ""),
            "Sales": float(r.get("sales_summary", {}).get("total_sales", 0) or 0),
        } for r in valid_trend]).sort_values("Week")

        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=trend_df["Week"],
            y=trend_df["Sales"],
            mode="lines+markers",
            name="Weekly sales",
            line=dict(color=PURPLE, width=2.5),
            marker=dict(color=PURPLE, size=6),
            fill="tozeroy",
            fillcolor="rgba(91,91,214,0.07)",
        ))
        fig_trend.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor=CARD,
            font=dict(color=TEXT, size=10),
            height=300,
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis=dict(showgrid=True, gridcolor=GRID, color=MUTED, zeroline=False,
                       tickfont=dict(size=10)),
            yaxis=dict(showgrid=True, gridcolor=GRID, color=MUTED, zeroline=False,
                       tickprefix="A$ ", tickfont=dict(size=10)),
            showlegend=False,
            hovermode="x unified",
        )
        with st.container():
            st.markdown(f"""
            <span class="trend-card-marker"></span>
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem;">
                <div class="db-card-title">Total sales over time</div>
                <span class="db-view-link">View report</span>
            </div>
            """, unsafe_allow_html=True)
            st.plotly_chart(fig_trend, use_container_width=True, config={"displayModeBar": False})
    else:
        st.markdown(f"""
        <div class="db-card" style="height:100%;display:flex;align-items:center;justify-content:center;">
            <span style="color:{MUTED};font-size:0.85rem;">More data will appear here after multiple weeks.</span>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# BOTTOM ROW — Staff performance table + Appointments
# ══════════════════════════════════════════════════════════════════════════════
col_staff, col_appts = st.columns([3, 2])

with col_staff:
    has_occ  = any(s.get("occupancy_pct") for s in staff_list)
    occ_th   = '<th class="r">Occupancy</th>' if has_occ else ""

    rows = ""
    for i, s in enumerate(sorted_staff, 1):
        occ_td = f"<td class='r'>{occ_badge(s.get('occupancy_pct', 0))}</td>" if has_occ else ""
        rows += (
            f"<tr>"
            f"<td>{rank_pill(i)}</td>"
            f"<td style='font-weight:600;'>{s.get('name', '—')}</td>"
            f"<td class='r' style='color:{PURPLE};font-weight:700;'>{c(s.get('total_sales'))}</td>"
            f"<td class='r'>{c(s.get('services'))}</td>"
            f"<td class='r'>{c(s.get('products'))}</td>"
            f"<td class='r'>{c(s.get('tips'))}</td>"
            f"<td class='r'>{n(s.get('total_appts'))}</td>"
            f"<td class='r' style='color:{RED_FG};'>{n(s.get('cancelled_appts'))}</td>"
            f"{occ_td}"
            f"</tr>"
        )

    st.markdown(f"""
    <div class="db-card">
        <div class="db-card-header">
            <div class="db-card-title">Staff performance</div>
            <span style="font-size:0.78rem;color:{MUTED};">{period_label} · ranked by total sales</span>
        </div>
        <div class="table-wrap">
        <table class="staff-table">
            <thead><tr>
                <th>Rank</th>
                <th>Name</th>
                <th class="r">Total</th>
                <th class="r">Services</th>
                <th class="r">Products</th>
                <th class="r">Tips</th>
                <th class="r">Appts</th>
                <th class="r">Cancelled</th>
                {occ_th}
            </tr></thead>
            <tbody>{rows}</tbody>
        </table>
        </div>
    </div>
    """, unsafe_allow_html=True)

with col_appts:
    st.markdown(f"""
    <div class="db-card">
        <div class="db-card-header">
            <div class="db-card-title">Appointments</div>
            <span class="db-view-link">View report</span>
        </div>
        <div class="db-big-num" style="font-size:2rem;">{n(appts.get("total"))}</div>
        <div style="margin-bottom:0.9rem;">
            <span class="chg-pill flat">{pct(appts.get("pct_online"))} online</span>
        </div>
        <div class="bd-row">
            <span class="bd-label">Total appointments</span>
            <span class="bd-value">{n(appts.get("total"))}</span>
        </div>
        <div class="bd-row">
            <span class="bd-label">Online bookings</span>
            <span style="display:flex;align-items:center;gap:0.4rem;">
                <span class="bd-value">{n(appts.get("online"))}</span>
                <span class="chg-pill up">↑ {pct(appts.get("pct_online"))}</span>
            </span>
        </div>
        <div class="bd-row">
            <span class="bd-label">Walk-ins / offline</span>
            <span class="bd-value">{n(appts.get("offline"))}</span>
        </div>
        <div class="bd-row">
            <span class="bd-label">Cancelled</span>
            <span style="display:flex;align-items:center;gap:0.4rem;">
                <span class="bd-value">{n(appts.get("cancelled"))}</span>
                <span class="chg-pill down">↓ {pct(appts.get("pct_cancelled"))}</span>
            </span>
        </div>
        <div class="bd-row">
            <span class="bd-label">No-shows</span>
            <span style="display:flex;align-items:center;gap:0.4rem;">
                <span class="bd-value">{n(appts.get("no_shows"))}</span>
                <span class="chg-pill down">↓ {pct(appts.get("pct_no_show"))}</span>
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown(
    f"<div style='text-align:center;color:{BORDER};font-size:0.68rem;padding:1.5rem 0 0.5rem;'>"
    "Diamond Barbers  ·  Auto-refreshes every 5 min  ·  Updated every Monday 6:00 AM Darwin time"
    "</div>",
    unsafe_allow_html=True,
)
