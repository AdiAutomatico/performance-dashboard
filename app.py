"""
app.py — Streamlit Performance Dashboard
Run: streamlit run app.py
"""

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# ── Load env: local .env first, then Streamlit Cloud secrets ─────────────────
load_dotenv(Path(__file__).parent / ".env")

# Push Streamlit secrets into os.environ (for Streamlit Cloud deployment)
if hasattr(st, "secrets"):
    for key, val in st.secrets.items():
        if isinstance(val, str):
            os.environ.setdefault(key, val)

from airtable_client import AirtableClient
from metrics import compute_metrics
from facebook_client import fetch_fb_spend
from formatting import (
    INDUSTRY_CPL_THRESHOLDS,
    INDUSTRY_CPA_THRESHOLDS,
    INDUSTRY_COST_PER_TOTAL_THRESHOLDS,
    _get_industry_thresholds,
)
from sheets_client import SheetsClient

# ── Colors ────────────────────────────────────────────────────────────────────
GREEN  = "#57bb8a"
YELLOW = "#ffd666"
RED    = "#e67c73"

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Performance Dashboard",
    page_icon="📊",
    layout="wide",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    thead tr th { background-color: #1e1e1e !important; color: white !important; font-size: 12px; }
    .stDataFrame { border-radius: 8px; overflow: hidden; }
    .legend-box { display:inline-block; width:14px; height:14px; border-radius:3px; margin-right:6px; vertical-align:middle; }
    .summary-metric { background:#f8f9fa; border-radius:8px; padding:12px 16px; text-align:center; }
    .summary-metric h2 { margin:0; font-size:28px; font-weight:700; }
    .summary-metric p  { margin:0; font-size:12px; color:#666; }
</style>
""", unsafe_allow_html=True)


# ── Config ────────────────────────────────────────────────────────────────────
def get_env(key, required=True):
    val = os.getenv(key, "").strip()
    if required and not val:
        st.error(f"Missing `{key}` in .env file.")
        st.stop()
    return val or None


# ── Data Loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def load_raw(start_iso: str, end_iso: str):
    """Fetch leads, appointments, calls, industry map. Cached 30 min."""
    pat      = get_env("AIRTABLE_PAT")
    base_id  = get_env("AIRTABLE_BASE_ID")
    creds    = get_env("GOOGLE_SHEETS_CREDS_PATH")
    sheet_id = get_env("GOOGLE_SHEET_ID")

    airtable = AirtableClient(pat, base_id)

    leads        = airtable.fetch_leads(start_iso, end_iso)
    appointments = airtable.fetch_appointments(start_iso, end_iso)
    calls        = airtable.fetch_calls(start_iso, end_iso)

    # Industry map from Settings tab
    try:
        sheets       = SheetsClient(creds, sheet_id)
        industry_map = sheets.read_industry_from_settings()
    except Exception:
        industry_map = {}

    return leads, appointments, calls, industry_map


@st.cache_data(ttl=1800, show_spinner=False)
def load_fb_spend(start_iso: str, end_iso: str):
    """Fetch FB spend for the exact selected date range. Cached separately per range."""
    fb_token = get_env("FB_ACCESS_TOKEN", required=False)
    pat      = get_env("AIRTABLE_PAT")
    base_id  = get_env("AIRTABLE_BASE_ID")

    fb_spend  = {}
    fb_errors = []

    if not fb_token:
        return fb_spend, fb_errors

    try:
        airtable   = AirtableClient(pat, base_id)
        fb_clients = airtable.fetch_clients_with_fb_accounts()
        for c in fb_clients:
            try:
                spend = fetch_fb_spend(c["fb_account_id"], start_iso, end_iso, fb_token)
                if spend > 0:
                    fb_spend[c["client_name"]] = spend
            except Exception as e:
                fb_errors.append(f"{c['client_name']}: {e}")
    except Exception as e:
        fb_errors.append(f"fetch_clients error: {e}")

    return fb_spend, fb_errors


def build_df(leads, appointments, calls, fb_spend, industry_map,
             client_filter=None, campaign_filter=None, source_filter=None):
    """Filter raw records and compute metrics into a display DataFrame."""

    def get_field(r, *keys):
        f = r.get("fields", {})
        for k in keys:
            v = f.get(k)
            if v is not None:
                if isinstance(v, list): v = v[0]
                return str(v).strip()
        return ""

    # Apply lead-level filters
    filtered_leads = leads
    if client_filter and client_filter != "All":
        filtered_leads = [r for r in filtered_leads if get_field(r, "Client") == client_filter]
    if campaign_filter and campaign_filter != "All":
        filtered_leads = [r for r in filtered_leads if get_field(r, "Campaign") == campaign_filter]
    if source_filter and source_filter != "All":
        filtered_leads = [r for r in filtered_leads if get_field(r, "Service") == source_filter]

    # Filter calls and appointments by the same clients
    active_clients = {get_field(r, "Client") for r in filtered_leads}
    filtered_calls = [r for r in calls if get_field(r, "Client (from Leads)") in active_clients]
    filtered_appts = [r for r in appointments if get_field(r, "Client (from Lead)") in active_clients]

    metrics = compute_metrics(filtered_leads, filtered_appts, filtered_calls)

    rows = []
    for client in sorted(metrics.keys()):
        m        = metrics[client]
        industry = industry_map.get(client, "")
        spend    = fb_spend.get(client, None)

        cpl = cpa_total = cpa = None
        if spend and spend > 0:
            if m["total_leads"]:        cpl       = round(spend / m["total_leads"], 2)
            if m["total_appts"]:        cpa_total = round(spend / m["total_appts"], 2)
            if m["confirmed_appts"]:    cpa       = round(spend / m["confirmed_appts"], 2)

        rows.append({
            "Client":                  client,
            "Industry":                industry,
            "Total Leads":             m["total_leads"],
            "Total Calls":             m["total_calls"],
            "Answer Rate":             m["answer_rate"],
            "Confirmed Appts":         m["confirmed_appts"],
            "Total Appts":             m["total_appts"],
            "Appt Rate":               m["appt_rate"],
            "Ad Spend":                spend,
            "Cost Per Lead":           cpl,
            "Cost Per Total Appt":     cpa_total,
            "Cost Per Confirmed Appt": cpa,
            "Show Rate":               m["show_rate"],
        })

    return pd.DataFrame(rows)


# ── Styling ───────────────────────────────────────────────────────────────────
def style_row(row):
    styles = [""] * len(row)
    cols   = list(row.index)

    def c(col, color):
        if col in cols:
            styles[cols.index(col)] = f"background-color: {color}; color: #000"

    def grade_hi(val, g, y):
        if val is None or val == "" or (isinstance(val, float) and pd.isna(val)): return None
        try: v = float(val)
        except (ValueError, TypeError): return None
        return GREEN if v >= g else (YELLOW if v >= y else RED)

    def grade_lo(val, g, y):
        if val is None or val == "" or (isinstance(val, float) and pd.isna(val)): return None
        try: v = float(val)
        except (ValueError, TypeError): return None
        return GREEN if v < g else (YELLOW if v < y else RED)

    industry = row.get("Industry", "")

    # Answer Rate  ≥15% green, ≥10% yellow, else red
    col = grade_hi(row.get("Answer Rate"), 0.15, 0.10)
    if col: c("Answer Rate", col)

    # Appt Rate  ≥35% green, ≥25% yellow, else red
    col = grade_hi(row.get("Appt Rate"), 0.35, 0.25)
    if col: c("Appt Rate", col)

    # Show Rate  ≥60% green, ≥40% yellow, else red
    col = grade_hi(row.get("Show Rate"), 0.60, 0.40)
    if col: c("Show Rate", col)

    # CPL
    cpl_g, cpl_y = _get_industry_thresholds(industry, INDUSTRY_CPL_THRESHOLDS)
    if cpl_g:
        col = grade_lo(row.get("Cost Per Lead"), cpl_g, cpl_y)
        if col: c("Cost Per Lead", col)

    # Cost Per Total Appt
    cpt_g, cpt_y = _get_industry_thresholds(industry, INDUSTRY_COST_PER_TOTAL_THRESHOLDS)
    if cpt_g:
        col = grade_lo(row.get("Cost Per Total Appt"), cpt_g, cpt_y)
        if col: c("Cost Per Total Appt", col)

    # Cost Per Confirmed Appt
    cpa_g, cpa_y = _get_industry_thresholds(industry, INDUSTRY_CPA_THRESHOLDS)
    if cpa_g:
        col = grade_lo(row.get("Cost Per Confirmed Appt"), cpa_g, cpa_y)
        if col: c("Cost Per Confirmed Appt", col)

    return styles


def format_df(df):
    pct  = lambda x: f"{x:.1%}" if pd.notna(x) and x != "" and isinstance(x, (int, float)) else "—"
    curr = lambda x: f"${x:,.2f}" if pd.notna(x) and x != "" and isinstance(x, (int, float)) else "—"
    num  = lambda x: f"{int(x):,}" if pd.notna(x) and x != "" and isinstance(x, (int, float)) else "—"

    return (
        df.style
        .apply(style_row, axis=1)
        .format({
            "Total Leads":             num,
            "Total Calls":             num,
            "Confirmed Appts":         num,
            "Total Appts":             num,
            "Answer Rate":             pct,
            "Appt Rate":               pct,
            "Show Rate":               pct,
            "Ad Spend":                curr,
            "Cost Per Lead":           curr,
            "Cost Per Total Appt":     curr,
            "Cost Per Confirmed Appt": curr,
        }, na_rep="—")
    )


# ── Sidebar ───────────────────────────────────────────────────────────────────
def sidebar(leads):
    st.sidebar.title("📊 Filters")
    st.sidebar.divider()

    # Date range
    st.sidebar.subheader("Date Range")
    preset = st.sidebar.radio(
        "", ["Last 7 Days", "Last 14 Days", "Last 30 Days", "Custom"],
        label_visibility="collapsed"
    )
    today = date.today()
    if preset == "Last 7 Days":
        start, end = today - timedelta(days=7), today
    elif preset == "Last 14 Days":
        start, end = today - timedelta(days=14), today
    elif preset == "Last 30 Days":
        start, end = today - timedelta(days=30), today
    else:
        col1, col2 = st.sidebar.columns(2)
        start = col1.date_input("From", today - timedelta(days=7), label_visibility="visible")
        end   = col2.date_input("To",   today,                     label_visibility="visible")

    st.sidebar.divider()

    # Filters from lead data
    def get_vals(field):
        vals = set()
        for r in leads:
            v = r.get("fields", {}).get(field)
            if v:
                if isinstance(v, list): v = v[0]
                vals.add(str(v).strip())
        return sorted(vals)

    clients   = ["All"] + get_vals("Client")
    campaigns = ["All"] + get_vals("Campaign")
    services  = ["All"] + get_vals("Service")

    st.sidebar.subheader("Client")
    client_filter = st.sidebar.selectbox("", clients, label_visibility="collapsed")

    st.sidebar.subheader("Campaign")
    campaign_filter = st.sidebar.selectbox("", campaigns, label_visibility="collapsed")

    st.sidebar.subheader("Service")
    source_filter = st.sidebar.selectbox("", services, label_visibility="collapsed")

    st.sidebar.divider()

    # Legend
    st.sidebar.subheader("Legend")
    st.sidebar.markdown(f"""
    <div style="margin:4px 0"><span class="legend-box" style="background:{GREEN}"></span>Good / On Track</div>
    <div style="margin:4px 0"><span class="legend-box" style="background:{YELLOW}"></span>Needs Attention</div>
    <div style="margin:4px 0"><span class="legend-box" style="background:{RED}"></span>Action Required</div>
    """, unsafe_allow_html=True)

    return start, end, client_filter, campaign_filter, source_filter


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # Load data for a broad range first (for filter options)
    today = date.today()
    default_start = (today - timedelta(days=30)).isoformat()

    with st.spinner("Loading data..."):
        try:
            leads, appointments, calls, industry_map = load_raw(
                default_start, today.isoformat()
            )
        except Exception as e:
            st.error(f"Failed to load data: {e}")
            return

    # Sidebar (uses lead data for filter options)
    start, end, client_filter, campaign_filter, source_filter = sidebar(leads)

    # Re-fetch Airtable data if date range extends beyond default
    if start < today - timedelta(days=30):
        with st.spinner("Loading extended date range..."):
            leads, appointments, calls, industry_map = load_raw(
                start.isoformat(), end.isoformat()
            )

    # Always fetch FB spend for the exact selected date range
    fb_spend, fb_errors = load_fb_spend(start.isoformat(), end.isoformat())

    # Header
    active_filters = [f for f in [
        client_filter   if client_filter   != "All" else None,
        campaign_filter if campaign_filter != "All" else None,
        source_filter   if source_filter   != "All" else None,
    ] if f]

    title = "Performance Dashboard"
    if active_filters:
        title += f" — {' · '.join(active_filters)}"
    st.title(title)
    st.caption(f"📅 {start.strftime('%b %d, %Y')} → {end.strftime('%b %d, %Y')}")

    # Filter all records to the selected date range
    start_str, end_str = start.isoformat(), end.isoformat()

    def in_range_field(r, field):
        d = r.get("fields", {}).get(field, "")
        if not d: return False
        return start_str <= d[:10] <= end_str

    def in_range_created(r):
        d = r.get("createdTime", "")
        if not d: return False
        return start_str <= d[:10] <= end_str

    range_leads = [r for r in leads       if in_range_field(r, "Created Date")]
    range_appts = [r for r in appointments if in_range_created(r)]
    range_calls = [r for r in calls        if in_range_field(r, "Call Timestamp")]

    # Build dataframe
    df = build_df(range_leads, range_appts, range_calls, fb_spend, industry_map,
                  client_filter, campaign_filter, source_filter)

    if df.empty:
        st.warning("No data found for the selected filters.")
        return

    # Summary metrics
    total_leads = int(df["Total Leads"].sum())
    total_appts = int(df["Confirmed Appts"].sum())
    avg_answer  = df[df["Answer Rate"].notna()]["Answer Rate"].mean()
    avg_appt    = df[df["Appt Rate"].notna()]["Appt Rate"].mean()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Leads",      f"{total_leads:,}")
    col2.metric("Confirmed Appts",  f"{total_appts:,}")
    col3.metric("Avg Answer Rate",  f"{avg_answer:.1%}" if pd.notna(avg_answer) else "—")
    col4.metric("Avg Appt Rate",    f"{avg_appt:.1%}"   if pd.notna(avg_appt)   else "—")

    st.divider()

    # Table
    st.dataframe(
        format_df(df),
        use_container_width=True,
        hide_index=True,
        height=600,
    )

    fb_count = sum(1 for v in df["Ad Spend"] if v is not None and str(v) not in ("", "None") and not (isinstance(v, float) and pd.isna(v)))
    st.caption(f"⏱ Data refreshes every 30 minutes · {len(df)} clients shown · {fb_count} with FB spend")

    if fb_errors:
        with st.expander(f"⚠️ {len(fb_errors)} Facebook API issue(s)"):
            for e in fb_errors:
                st.text(e)


if __name__ == "__main__":
    main()
