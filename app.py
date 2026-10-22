from __future__ import annotations
from datetime import datetime, timedelta
import random
import pandas as pd
import streamlit as st

st.set_page_config(page_title="SignalDesk | Data Platform", page_icon="📡", layout="wide")
SERVICES = ["Spark", "Flink", "Airflow", "Trino", "Druid", "Notebook Hub"]
RUNBOOKS = {
    "Spark": "Check executor saturation, shuffle spill, and recent autoscaling events.",
    "Flink": "Inspect checkpoint duration and backpressure before restarting a job.",
    "Airflow": "Check scheduler heartbeat, DAG parse errors, and queued task capacity.",
    "Trino": "Review coordinator memory, queued queries, and worker availability.",
    "Druid": "Inspect segment handoff lag and historical node disk pressure.",
    "Notebook Hub": "Check workspace pod scheduling, image pulls, and shared volume mounts.",
}

@st.cache_data
def make_data(seed: int = 18):
    rng = random.Random(seed)
    now = datetime.now().replace(second=0, microsecond=0)
    health = [{"Service": s, "Availability": round(rng.uniform(97.8, 99.99), 2),
               "P95 latency (ms)": rng.randint(90, 740), "Error rate (%)": round(rng.uniform(.05, 2.8), 2),
               "Open incidents": rng.randint(0, 4)} for s in SERVICES]
    symptoms = ["Elevated query latency", "Task queue growing", "Checkpoint failures", "Worker capacity low", "API error rate spike"]
    severities = ["P1", "P2", "P2", "P3", "P3"]
    incidents = []
    for i in range(16):
        service = rng.choice(SERVICES)
        incidents.append({"ID": f"INC-{4100+i}", "Opened": now-timedelta(minutes=rng.randint(8, 520)),
                          "Severity": rng.choice(severities), "Service": service, "Signal": rng.choice(symptoms),
                          "Status": rng.choice(["Investigating", "Monitoring", "Awaiting owner", "Mitigated"]),
                          "Impact": rng.choice(["1 team", "3 teams", "5 teams", "12 teams"])})
    frame = pd.DataFrame(incidents).sort_values("Opened", ascending=False).reset_index(drop=True)
    points = [{"Hour": (now-timedelta(hours=23-h)).strftime("%H:%M"),
               "Requests (k)": round(18+6*rng.random()+(3 if 8 <= h <= 18 else 0), 1),
               "Error rate (%)": round(.25+.8*rng.random(), 2)} for h in range(24)]
    return pd.DataFrame(health), frame, pd.DataFrame(points)

health, incidents, traffic = make_data()
st.title("📡 SignalDesk")
st.caption("Data Platform Support Workbench · demo telemetry for internal SRE workflows")
with st.sidebar:
    st.header("Platform scope")
    selected_services = st.multiselect("Services", SERVICES, default=SERVICES)
    severity_filter = st.multiselect("Severity", ["P1", "P2", "P3"], default=["P1", "P2", "P3"])
    st.divider()
    st.caption("Demo mode · generated sample data · no production systems connected")
visible_health = health[health.Service.isin(selected_services)]
visible_incidents = incidents[incidents.Service.isin(selected_services) & incidents.Severity.isin(severity_filter)]
p1_count = int((visible_incidents.Severity == "P1").sum())
open_count = int((visible_incidents.Status != "Mitigated").sum())
avg_availability = visible_health.Availability.mean() if not visible_health.empty else 0
avg_error = visible_health["Error rate (%)"].mean() if not visible_health.empty else 0
m1,m2,m3,m4 = st.columns(4)
m1.metric("Platform availability", f"{avg_availability:.2f}%", "24h · sampled")
m2.metric("Open support incidents", open_count, f"{p1_count} P1")
m3.metric("Mean error rate", f"{avg_error:.2f}%", "across selected services")
m4.metric("Services in scope", len(visible_health), "of 6 platform services")
left,right = st.columns([1.25,1])
with left:
    st.subheader("Traffic and error signals")
    chart_a, chart_b = st.columns(2)
    with chart_a:
        st.caption("Requests (thousands)")
        st.line_chart(traffic.set_index("Hour")[["Requests (k)"]], height=220)
    with chart_b:
        st.caption("Error rate (%)")
        st.line_chart(traffic.set_index("Hour")[["Error rate (%)"]], height=220)
with right:
    st.subheader("Service health")
    if visible_health.empty: st.info("Select at least one service to see health signals.")
    else: st.dataframe(visible_health.sort_values("Error rate (%)", ascending=False), hide_index=True, use_container_width=True)
st.subheader("Support queue")
st.dataframe(visible_incidents[["ID","Opened","Severity","Service","Signal","Status","Impact"]], hide_index=True, use_container_width=True)
st.subheader("Triage assistant")
if visible_incidents.empty:
    st.info("No incidents match the current filters.")
else:
    selected_id = st.selectbox("Choose an incident", visible_incidents.ID.tolist())
    incident = visible_incidents.loc[visible_incidents.ID == selected_id].iloc[0]
    c1,c2 = st.columns([1,2])
    with c1:
        st.markdown(f"**{incident['Severity']} · {incident['Service']}**")
        st.write(f"Signal: {incident['Signal']}")
        st.write(f"Customer impact: {incident['Impact']}")
        st.write(f"Status: {incident['Status']}")
    with c2:
        st.markdown("**Suggested first checks**")
        st.write(RUNBOOKS[incident["Service"]])
        st.caption("Rule-based demo suggestion. Confirm against service runbooks and live telemetry before action.")
    with st.expander("Draft customer update"):
        st.text_area("Status message", value=f"We are investigating {incident['Signal'].lower()} affecting {incident['Service']} ({incident['Impact']} reported). The team is checking service health and will share an update when impact changes.", height=100)
with st.expander("About this prototype"):
    st.write("SignalDesk turns service telemetry and support events into a customer-facing operations view. This version uses generated data and transparent, rule-based runbook suggestions. A production prototype could replace the sample generators with Prometheus or platform APIs, add an authenticated Slack webhook, and use an approved LLM with citations to internal runbooks.")


