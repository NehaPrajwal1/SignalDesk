"""Customer-facing support dashboard. Run: streamlit run app.py."""
import json
import os
import uuid
from datetime import datetime, timezone
from urllib.parse import quote

import pandas as pd
import requests
import streamlit as st
from core import SERVICES, STATUSES, demo_rows

st.set_page_config(page_title="SignalDesk", page_icon="📡", layout="wide")
st.title("SignalDesk · Data Platform Support")
st.caption("Customer impact, operational evidence, and a reviewable support workflow")
api_url = os.getenv("SIGNALDESK_API_URL", "http://127.0.0.1:8000").rstrip("/")
with st.sidebar:
    st.header("Workspace")
    st.caption(f"API: {api_url}")
    token = st.text_input("API token", os.getenv("SIGNALDESK_API_TOKEN", ""), type="password")
    chosen_service = st.selectbox("Service scope", ["All services", *SERVICES])
    severities = st.multiselect("Severity", ["P1", "P2", "P3"], default=["P1", "P2", "P3"])
    if st.button("Refresh"):
        st.rerun()
headers = {"Authorization": "Bearer " + token} if token else {}


def call(method, path, payload=None, extra_headers=None):
    try:
        response = requests.request(method, api_url + path, json=payload, headers={**headers, **(extra_headers or {})}, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        status = exc.response.status_code if exc.response is not None else "unreachable"
        st.error(f"API request failed ({status}). Check the API process and token. For a 409 conflict, refresh and retry.")
        return None


def load(path):
    result = call("GET", path)
    if result is None:
        st.info("Start the API with: uvicorn api:app --reload")
        st.stop()
    return result


health = load("/api/health")
st.info(f"Data source: {health['data_mode']}. Charts describe the stored sample window; they are not a live production availability claim.")
scope = "" if chosen_service == "All services" else "?service=" + quote(chosen_service)
services = load("/api/services" + scope)
traffic = load("/api/traffic" + scope)
incidents = [item for item in load("/api/incidents" + scope) if item["severity"] in severities]
support = load("/api/support")
quality = load("/api/quality")
requests_total = sum(row["requests"] for row in services)
failures_total = sum(row["failures"] for row in services)
availability = 100 * (requests_total - failures_total) / requests_total if requests_total else None
open_items = [item for item in incidents if item["status"] != "Mitigated"]
a, b, c, d = st.columns(4)
a.metric("Request success (sample)", f"{availability:.2f}%" if availability is not None else "No data")
b.metric("Open incidents in scope", len(open_items))
c.metric("P1 in scope", sum(item["severity"] == "P1" for item in open_items))
d.metric("Telemetry records in scope", requests_total)
st.caption("Success SLI = requests without HTTP 5xx / all requests. The 99% target and burn ratio are illustrative sample-window measures, not a 30-day SLO.")
if services:
    st.subheader("Service health and ownership")
    st.dataframe(pd.DataFrame(services), hide_index=True, use_container_width=True)
    newest = max(datetime.fromisoformat(item["last_sample"]) for item in services)
    age = (datetime.now(timezone.utc) - newest).total_seconds() / 3600
    st.caption(f"Latest sample: {newest.isoformat()} · age: {age:.1f} hours. Historical fixture data is intentionally dated.")
else:
    st.warning("No telemetry for this scope. Ingest records below.")
if traffic:
    frame = pd.DataFrame(traffic)
    frame["time"] = pd.to_datetime(frame["time"], utc=True)
    frame = frame.set_index("time")
    left, right = st.columns(2)
    with left:
        st.subheader("Request volume")
        st.line_chart(frame[["requests"]])
    with right:
        st.subheader("HTTP 5xx rate (%)")
        st.line_chart(frame[["error_rate_pct"]])

st.subheader("Support operations · all services")
a, b, c = st.columns(3)
a.metric("Open backlog", support["open"])
b.metric("Oldest open (minutes)", support["oldest_open_minutes"])
c.metric("Mean time to mitigation (minutes)", support["mean_resolution_minutes"] if support["mean_resolution_minutes"] is not None else "No mitigations")
st.caption(f"Resolution statistic uses {support['resolved_count']} currently mitigated incidents; reopened incidents are excluded. Zero-duration demo updates are not production benchmarks.")
if support["created_daily"]:
    st.caption("Incident arrivals by day and severity · all services")
    trends = pd.DataFrame(support["created_daily"]).pivot(index="day", columns="severity", values="incidents").fillna(0)
    st.bar_chart(trends)
st.subheader("Incident queue")
if incidents:
    st.dataframe(pd.DataFrame(incidents)[["id", "service", "severity", "signal", "impact", "status", "opened_at"]], hide_index=True, use_container_width=True)
    selected = st.selectbox("Incident", [item["id"] for item in incidents])
    incident = next(item for item in incidents if item["id"] == selected)
    triage = load("/api/triage/" + selected)
    st.write(triage["summary"])
    st.markdown("**First checks · human approval required for operational changes**")
    st.text(triage["runbook"])
    with st.expander("Evidence and tool trace"):
        st.write(" → ".join(triage["tool_trace"]))
        st.write("Citations: " + ", ".join(triage["citations"]))
        st.json(triage["evidence"])
    with st.expander("Incident history"):
        st.dataframe(pd.DataFrame(load(f"/api/incidents/{selected}/events")), hide_index=True, use_container_width=True)
    with st.form("status-" + selected):
        new_status = st.selectbox("New status", STATUSES, index=STATUSES.index(incident["status"]))
        note = st.text_input("Update note")
        save = st.form_submit_button("Save update")
    if save:
        result = call("PATCH", "/api/incidents/" + selected, {"status": new_status, "note": note, "expected_version": incident["version"]})
        if result:
            st.rerun()
    with st.expander("Optional local LLM draft"):
        st.caption("Uses the API server's configured Ollama endpoint. Model output is unverified and cannot execute tools or send messages.")
        if st.button("Draft from evidence"):
            result = call("POST", "/api/agent/" + selected, {"use_llm": True})
            if result:
                st.write(result["llm_status"])
                st.text(result.get("llm_draft", result["summary"]))
else:
    st.info("No incidents match the filters.")

with st.expander("Create support alert"):
    if "alert_key" not in st.session_state:
        st.session_state.alert_key = uuid.uuid4().hex
    with st.form("new-alert"):
        service = st.selectbox("Affected service", SERVICES)
        severity = st.selectbox("Alert severity", ["P1", "P2", "P3"], index=1)
        signal = st.text_input("Observed signal", "Elevated error rate")
        impact = st.text_input("Reported impact", "Demo team")
        create = st.form_submit_button("Create incident")
    if create:
        result = call("POST", "/api/alerts", {"service": service, "severity": severity, "signal": signal, "impact": impact, "source": "dashboard"}, {"Idempotency-Key": st.session_state.alert_key})
        if result:
            st.session_state.alert_key = uuid.uuid4().hex
            st.rerun()

with st.expander("Telemetry ingestion and data quality"):
    st.write(f"Stored unique records: {quality['stored_records']}")
    st.caption("JSONL contract: event_id, at (timezone-aware ISO timestamp), service, status_code, latency_ms, optional message. Use synthetic or approved non-sensitive data only. Up to 1,000 rows per batch.")
    st.download_button("Download synthetic fixture", "\n".join(json.dumps(row) for row in demo_rows()), "signaldesk-demo.jsonl", "application/x-ndjson")
    uploaded = st.file_uploader("Upload JSONL telemetry", type=["jsonl"])
    if uploaded is not None and st.button("Ingest batch"):
        if uploaded.size > 2_000_000:
            st.error("Use a file smaller than 2 MB.")
        else:
            try:
                records = [json.loads(line) for line in uploaded.getvalue().decode("utf-8").splitlines() if line.strip()]
                result = call("POST", "/api/telemetry", {"records": records})
                if result:
                    st.json(result)
                    st.info("Refresh the dashboard to see the updated data. Replaying identical IDs does not duplicate records.")
            except (ValueError, UnicodeDecodeError):
                st.error("Each non-empty line must contain valid UTF-8 JSON.")
    if quality["recent_batches"]:
        st.dataframe(pd.DataFrame(quality["recent_batches"]), hide_index=True, use_container_width=True)


@st.fragment(run_every="5s")
def operational_panel():
    st.subheader("Live workload and container telemetry")
    st.caption("Refreshes every 5 seconds. Start simulate.py or POST /api/workloads. Samples are not a live Kubernetes connection.")
    rows = call("GET", "/api/workloads" + scope)
    if rows:
        frame = pd.DataFrame(rows)
        st.dataframe(frame[["at", "service", "kind", "pod", "cpu_percent", "memory_percent", "restarts", "failed"]], hide_index=True, use_container_width=True)
        chart = frame.copy()
        chart["at"] = pd.to_datetime(chart["at"], utc=True)
        chart = chart.sort_values("at").set_index("at")
        st.line_chart(chart[["cpu_percent", "memory_percent"]])
        st.caption("Container percentages are sample values; mixed services are shown in timestamp order. Filter Service scope for one service.")
        with st.expander("Ingested log evidence"):
            st.json(rows[:10])
    elif rows is not None:
        st.info("No operational samples yet. Run: python simulate.py --count 60")


operational_panel()
