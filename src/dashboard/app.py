"""Streamlit monitoring dashboard for the ChatGPT Data Platform.

Launch: streamlit run src/dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import random
import numpy as np

st.set_page_config(
    page_title="ChatGPT Data Platform",
    page_icon="🔧",
    layout="wide",
)

st.title("ChatGPT Data Platform — Monitoring Dashboard")
st.caption("Real-time visibility into pipeline health, data quality, safety, and business metrics")

# =========================================================================
# Sidebar
# =========================================================================
with st.sidebar:
    st.header("Configuration")
    date_range = st.date_input(
        "Date Range",
        value=(datetime.now() - timedelta(days=30), datetime.now()),
    )
    refresh = st.button("Refresh Data")
    st.divider()
    st.markdown("**Data Sources**")
    st.markdown("- Snowflake: `CHATGPT_PLATFORM`")
    st.markdown("- MinIO: `staging/`, `processed/`")
    st.markdown("- Kafka: 5 topics")

# =========================================================================
# Generate demo data (in production, this would query Snowflake)
# =========================================================================
@st.cache_data(ttl=300)
def generate_demo_data(days: int = 30):
    dates = pd.date_range(end=datetime.now(), periods=days, freq="D")
    np.random.seed(42)

    # DAU data
    base_dau = 150000
    dau_data = pd.DataFrame({
        "date": dates,
        "total_dau": [int(base_dau + np.random.normal(0, 5000) + i * 500) for i in range(days)],
        "dau_free": [int(100000 + np.random.normal(0, 3000) + i * 300) for i in range(days)],
        "dau_plus": [int(40000 + np.random.normal(0, 1500) + i * 150) for i in range(days)],
        "dau_enterprise": [int(10000 + np.random.normal(0, 500) + i * 50) for i in range(days)],
    })

    # Revenue data
    revenue_data = pd.DataFrame({
        "date": dates,
        "mrr": [2500000 + i * 30000 + np.random.normal(0, 50000) for i in range(days)],
        "new_subscriptions": [int(500 + np.random.normal(0, 50)) for _ in range(days)],
        "churned": [int(100 + np.random.normal(0, 20)) for _ in range(days)],
    })

    # Safety data
    safety_data = pd.DataFrame({
        "date": dates,
        "total_signals": [int(500 + np.random.normal(0, 50)) for _ in range(days)],
        "prompt_injections": [int(100 + np.random.normal(0, 15)) for _ in range(days)],
        "rate_limit_violations": [int(200 + np.random.normal(0, 30)) for _ in range(days)],
        "accounts_quarantined": [int(10 + np.random.normal(0, 3)) for _ in range(days)],
    })

    # Pipeline health
    pipeline_data = pd.DataFrame({
        "dag": ["daily_batch_etl", "hourly_safety_monitoring"] * days,
        "date": list(dates) * 2,
        "status": [random.choices(["success", "success", "success", "failed"], weights=[90, 3, 3, 4])[0] for _ in range(days * 2)],
        "duration_minutes": [random.uniform(15, 45) if i < days else random.uniform(3, 8) for i in range(days * 2)],
    })

    return dau_data, revenue_data, safety_data, pipeline_data


dau_data, revenue_data, safety_data, pipeline_data = generate_demo_data()

# =========================================================================
# Tabs
# =========================================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "Pipeline Health", "Data Quality", "Safety Monitor", "Business Metrics"
])

# ---- Tab 1: Pipeline Health ----
with tab1:
    st.subheader("Pipeline Health")

    col1, col2, col3, col4 = st.columns(4)

    daily_etl = pipeline_data[pipeline_data["dag"] == "daily_batch_etl"]
    safety_runs = pipeline_data[pipeline_data["dag"] == "hourly_safety_monitoring"]

    success_rate_daily = (daily_etl["status"] == "success").mean() * 100
    success_rate_safety = (safety_runs["status"] == "success").mean() * 100

    col1.metric("Daily ETL Success Rate", f"{success_rate_daily:.1f}%")
    col2.metric("Safety Monitoring Success", f"{success_rate_safety:.1f}%")
    col3.metric("Avg ETL Duration", f"{daily_etl['duration_minutes'].mean():.1f} min")
    col4.metric("Avg Safety Duration", f"{safety_runs['duration_minutes'].mean():.1f} min")

    fig_duration = px.line(
        pipeline_data,
        x="date", y="duration_minutes", color="dag",
        title="Pipeline Duration Over Time",
        labels={"duration_minutes": "Duration (min)", "date": "Date"},
    )
    st.plotly_chart(fig_duration, use_container_width=True)

    # Recent failures
    failures = pipeline_data[pipeline_data["status"] == "failed"].tail(10)
    if not failures.empty:
        st.warning(f"{len(failures)} recent failures detected")
        st.dataframe(failures[["date", "dag", "duration_minutes"]], use_container_width=True)

# ---- Tab 2: Data Quality ----
with tab2:
    st.subheader("Data Quality Monitoring")

    quality_checks = pd.DataFrame({
        "Check": [
            "conversation_events.no_nulls:event_id",
            "conversation_events.unique:event_id",
            "conversation_events.values_in_set:event_type",
            "api_usage_events.no_nulls:event_id",
            "api_usage_events.values_in_set:http_status_code",
            "billing_events.column_between:amount_usd",
            "user_events.column_matches_regex:user_id",
            "warehouse.referential_integrity",
        ],
        "Status": ["PASS"] * 7 + ["WARN"],
        "Last Run": [datetime.now().strftime("%Y-%m-%d %H:%M")] * 8,
        "Details": [
            "0 nulls / 1,250,000 rows",
            "100% unique (1,250,000 / 1,250,000)",
            "0 invalid values",
            "0 nulls / 500,000 rows",
            "0 invalid status codes",
            "0 out of range [0, 10000]",
            "0 non-matching patterns",
            "12 orphaned conversations (0.001%)",
        ],
    })

    for _, row in quality_checks.iterrows():
        icon = "✅" if row["Status"] == "PASS" else "⚠️"
        st.markdown(f"{icon} **{row['Check']}** — {row['Details']}")

    st.divider()
    st.subheader("Data Volume Trends")

    volume_data = pd.DataFrame({
        "date": pd.date_range(end=datetime.now(), periods=30, freq="D"),
        "conversations": np.random.randint(1_000_000, 1_500_000, 30),
        "api_calls": np.random.randint(500_000, 800_000, 30),
        "billing_events": np.random.randint(50_000, 80_000, 30),
    })

    fig_volume = px.bar(
        volume_data.melt(id_vars="date", var_name="event_type", value_name="count"),
        x="date", y="count", color="event_type",
        title="Daily Event Volume by Type",
        barmode="group",
    )
    st.plotly_chart(fig_volume, use_container_width=True)

# ---- Tab 3: Safety Monitor ----
with tab3:
    st.subheader("Trust & Safety Dashboard")

    col1, col2, col3, col4 = st.columns(4)
    latest = safety_data.iloc[-1]
    prev = safety_data.iloc[-2]

    col1.metric(
        "Safety Signals (Today)", f"{int(latest['total_signals']):,}",
        delta=f"{int(latest['total_signals'] - prev['total_signals']):+d}",
    )
    col2.metric(
        "Prompt Injection Attempts", f"{int(latest['prompt_injections']):,}",
        delta=f"{int(latest['prompt_injections'] - prev['prompt_injections']):+d}",
        delta_color="inverse",
    )
    col3.metric(
        "Rate Limit Violations", f"{int(latest['rate_limit_violations']):,}",
        delta=f"{int(latest['rate_limit_violations'] - prev['rate_limit_violations']):+d}",
        delta_color="inverse",
    )
    col4.metric(
        "Accounts Quarantined", f"{int(latest['accounts_quarantined']):,}",
        delta=f"{int(latest['accounts_quarantined'] - prev['accounts_quarantined']):+d}",
        delta_color="inverse",
    )

    fig_safety = px.area(
        safety_data.melt(id_vars="date", var_name="signal_type", value_name="count"),
        x="date", y="count", color="signal_type",
        title="Safety Signals Over Time",
    )
    st.plotly_chart(fig_safety, use_container_width=True)

    st.subheader("Violation Type Breakdown")
    violation_types = pd.DataFrame({
        "type": ["Prompt Injection", "Content Policy", "Rate Limit Abuse",
                 "Jailbreak Attempt", "Automated Scraping", "Other"],
        "count": [1200, 1500, 2000, 800, 600, 400],
    })
    fig_pie = px.pie(violation_types, values="count", names="type", title="Violation Distribution (Last 30 Days)")
    st.plotly_chart(fig_pie, use_container_width=True)

# ---- Tab 4: Business Metrics ----
with tab4:
    st.subheader("Business Metrics")

    col1, col2, col3, col4 = st.columns(4)
    latest_dau = dau_data.iloc[-1]
    prev_dau = dau_data.iloc[-2]

    col1.metric(
        "DAU", f"{int(latest_dau['total_dau']):,}",
        delta=f"{int(latest_dau['total_dau'] - prev_dau['total_dau']):+,}",
    )
    col2.metric(
        "MRR", f"${revenue_data.iloc[-1]['mrr']:,.0f}",
        delta=f"${revenue_data.iloc[-1]['mrr'] - revenue_data.iloc[-2]['mrr']:+,.0f}",
    )
    col3.metric(
        "New Subs (Today)", f"{int(revenue_data.iloc[-1]['new_subscriptions']):,}",
    )
    col4.metric(
        "Churn (Today)", f"{int(revenue_data.iloc[-1]['churned']):,}",
        delta_color="inverse",
    )

    # DAU trend
    fig_dau = px.line(
        dau_data, x="date", y=["total_dau", "dau_free", "dau_plus", "dau_enterprise"],
        title="Daily Active Users by Tier",
        labels={"value": "Users", "date": "Date"},
    )
    st.plotly_chart(fig_dau, use_container_width=True)

    # Revenue trend
    fig_revenue = px.area(
        revenue_data, x="date", y="mrr",
        title="Monthly Recurring Revenue (MRR) Trend",
        labels={"mrr": "MRR ($)", "date": "Date"},
    )
    st.plotly_chart(fig_revenue, use_container_width=True)

    # Subscription metrics
    col_left, col_right = st.columns(2)
    with col_left:
        fig_subs = px.bar(
            revenue_data, x="date", y=["new_subscriptions", "churned"],
            title="New Subscriptions vs Churn",
            barmode="group",
        )
        st.plotly_chart(fig_subs, use_container_width=True)

    with col_right:
        model_usage = pd.DataFrame({
            "model": ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo", "o1-preview", "o1-mini"],
            "requests_millions": [45.2, 32.1, 12.5, 8.3, 3.2, 2.1],
        })
        fig_models = px.bar(
            model_usage, x="model", y="requests_millions",
            title="Model Usage (Last 30 Days)",
            labels={"requests_millions": "Requests (M)"},
        )
        st.plotly_chart(fig_models, use_container_width=True)
