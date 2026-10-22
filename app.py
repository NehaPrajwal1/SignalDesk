from __future__ import annotations

import json
import os
from urllib.parse import quote

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="SignalDesk | Data Platform", page_icon="📡", layout="wide")
SERVICES = ["Spark", "Flink", "Airflow", "Trino", "Druid", "Notebook Hub"]

st.title("📡 SignalDesk")
st.caption("Data Platform Support Workbench · synthetic telemetry + persistent incident workflow")

with st.sidebar:
    st.header("Connections")
    api_url = st.text_input("SignalDesk API", os.getenv("SIGNALDESK_API_URL", "http://127.0.0.1:8000")).rstrip("/")
    ingest_token = st.text_input("Alert API token (optional for local demo)", os.getenv("SIGNALDESK_API_TOKEN", ""), type="password")
    ollama_url = st.text_input("Local Ollama URL", os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")).rstrip("/")
    ollama_model = st.text_input("Ollama model", os.getenv("OLLAMA_MODEL", "qwen2.5:3b"))
    st.divider()
    st.caption("Demo signals are generated. Incident records are stored by the API in SQLite.")


def api_get(path: str, params: dict | None = None):
    try:
        response = requests.get(f"{api_url}{path}", params=params, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        st.error(f"Cannot reach the SignalDesk API at {api_url}: {exc}")
        st.info("Start it in another terminal with: uvicorn api:app --reload")
        st.stop()


def api_write(method: str, path: str, payload: dict):
    headers = {"Authorization": f"Bearer {ingest_token}"} if ingest_token else {}
    try:
        response = requests.request(method, f"{api_url}{path}", json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        detail = exc.response.text if exc.response is not None else str(exc)
        st.error(f"API request failed: {detail}")
        return None


health = api_get("/api/health")
services = api_get("/api/services")
traffic = api_get("/api/traffic")
incidents = api_get("/api/incidents")

with st.sidebar:
    selected_services = st.multiselect("Services", SERVICES, default=SERVICES)
    severity_filter = st.multiselect("Severity", ["P1", "P2", "P3"], default=["P1", "P2", "P3"])
    if st.button("Refresh dashboard", use_container_width=True):
        st.rerun()

health_rows = [row for row in services if row["service"] in selected_services]
incident_rows = [row for row in incidents if row["service"] in selected_services and row["severity"] in severity_filter]
open_rows = [row for row in incident_rows if row["status"] != "Mitigated"]
p1_rows = [row for row in open_rows if row["severity"] == "P1"]
availability = sum(row["availability_pct"] for row in health_rows) / len(health_rows) if health_rows else 0
mean_error = sum(row["error_rate_pct"] for row in health_rows) / len(health_rows) if health_rows else 0

m1, m2, m3, m4 = st.columns(4)
m1.metric("Platform availability", f"{availability:.2f}%", "synthetic sample")
m2.metric("Open support incidents", len(open_rows), f"{len(p1_rows)} P1")
m3.metric("Mean error rate", f"{mean_error:.2f}%", "synthetic sample")
m4.metric("Services in scope", len(health_rows), "of 6 services")

left, right = st.columns([1.25, 1])
with left:
    st.subheader("Traffic and error signals")
    st.caption("Synthetic 24-hour sample · separate scales")
    frame = pd.DataFrame(traffic)
    if not frame.empty:
        frame = frame.set_index("time")
        charts = st.columns(2)
        with charts[0]:
            st.caption("Requests (thousands)")
            st.line_chart(frame[["requests_k"]], height=220)
        with charts[1]:
            st.caption("Error rate (%)")
            st.line_chart(frame[["error_rate_pct"]], height=220)
with right:
    st.subheader("Service health")
    if health_rows:
        st.dataframe(pd.DataFrame(health_rows), hide_index=True, use_container_width=True)
    else:
        st.info("Select at least one service.")

st.subheader("Support queue")
if incident_rows:
    st.dataframe(pd.DataFrame(incident_rows)[["id", "opened_at", "severity", "service", "signal", "status", "impact", "source"]], hide_index=True, use_container_width=True)
else:
    st.info("No incidents match the selected filters.")

st.subheader("Incident triage")
if incident_rows:
    chosen_id = st.selectbox("Choose an incident", [row["id"] for row in incident_rows])
    incident = next(row for row in incident_rows if row["id"] == chosen_id)
    triage = api_get(f"/api/triage/{quote(chosen_id, safe='')}")
    detail, guidance = st.columns([1, 2])
    with detail:
        st.markdown(f"**{incident['severity']} · {incident['service']}**")
        st.write(f"Signal: {incident['signal']}")
        st.write(f"Customer impact: {incident['impact']}")
        st.write(f"Status: {incident['status']}")
        st.write(f"Source: {incident['source']}")
    with guidance:
        st.markdown("**Suggested first checks**")
        st.write(triage["runbook"])
        st.caption("Rule-based guidance from the demo runbook map. Verify against live service procedures.")

    with st.expander("Incident history"):
        events = api_get(f"/api/incidents/{quote(chosen_id, safe='')}/events")
        if events:
            st.dataframe(pd.DataFrame(events), hide_index=True, use_container_width=True)
        else:
            st.caption("No status changes recorded yet.")

    with st.form("status_update"):
        statuses = ["Investigating", "Monitoring", "Awaiting owner", "Mitigated"]
        status = st.selectbox("Update status", statuses, index=statuses.index(incident["status"]))
        note = st.text_input("Update note", placeholder="What changed? (optional)")
        update_status = st.form_submit_button("Save status update")
    if update_status:
        result = api_write("PATCH", f"/api/incidents/{quote(chosen_id, safe='')}", {"status": status, "note": note, "actor": "dashboard"})
        if result:
            st.success("Incident updated and history recorded.")
            st.rerun()

    with st.expander("Draft customer update"):
        st.text_area("Status message", value=triage["customer_update"], height=100)

    with st.expander("Optional local LLM draft (Ollama)"):
        st.caption("The model receives only this selected synthetic incident and its demo runbook. No external model is called.")
        if st.button("Generate draft with Ollama"):
            prompt = (
                "You are drafting a concise internal support update. Use only these incident facts and runbook text; "
                "do not invent causes, timelines, or impact. State uncertainty.\n\n"
                + json.dumps({"incident": incident, "runbook": triage["runbook"]}, ensure_ascii=False)
            )
            try:
                response = requests.post(f"{ollama_url}/api/generate", json={"model": ollama_model, "prompt": prompt, "stream": False}, timeout=90)
                response.raise_for_status()
                st.text_area("Ollama draft · review before sharing", value=response.json().get("response", ""), height=150)
            except requests.RequestException as exc:
                st.error(f"Ollama request failed: {exc}. Start Ollama and pull the selected model, or use the rule-based draft above.")
else:
    st.info("Choose filters that include at least one incident to view triage details.")

with st.expander("Create an alert (webhook demo)"):
    st.write("This form sends a new incident to the API. The same endpoint can receive alerts from approved internal webhooks.")
    with st.form("new_alert"):
        service = st.selectbox("Service", SERVICES)
        severity = st.selectbox("Severity", ["P1", "P2", "P3"])
        signal = st.text_input("Signal", value="Elevated query latency")
        impact = st.text_input("Customer impact", value="1 team")
        source = st.text_input("Source", value="dashboard demo")
        submit_alert = st.form_submit_button("Create incident")
    if submit_alert:
        if not signal.strip():
            st.error("Signal is required.")
        else:
            created = api_write("POST", "/api/alerts", {"service": service, "severity": severity, "signal": signal, "impact": impact, "source": source})
            if created:
                st.success(f"Created {created['id']}.")
                st.rerun()

with st.expander("API and observability"):
    st.write(f"API status: **{health['status']}** · data mode: **{health['data_mode']}**")
    st.write(f"Prometheus metrics endpoint: [{api_url}/metrics]({api_url}/metrics)")
    st.code('''$body = @{ service="Spark"; severity="P2"; signal="Executor saturation"; impact="3 teams"; source="prometheus-demo" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/alerts -ContentType application/json -Headers @{ Authorization = "Bearer $env:SIGNALDESK_API_TOKEN" } -Body $body''', language="powershell")
    st.caption("For local development only. Configure an API token and Slack signing secret before exposing the API beyond localhost.")
