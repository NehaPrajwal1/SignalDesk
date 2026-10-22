# SignalDesk: Data Platform Support Workbench

A compact portfolio prototype aligned with the Apple Services Engineering Data Platform Intern role. It combines customer-facing service health, traffic/error trends, an incident queue, and transparent first-step triage suggestions.

## Run locally

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

## Demo contents

- Sample signals for Spark, Flink, Airflow, Trino, Druid, and Notebook Hub.
- Service and severity filters, health metrics, and traffic/error charts.
- Incident queue with severity, current status, and reported customer impact.
- Triage panel with service-specific first checks and a draft customer update.
- Reproducible synthetic data; no credentials or production connections required.

## Next iteration

1. Replace generated telemetry with Prometheus or approved platform API queries.
2. Add an authenticated Slack slash-command or webhook adapter for incident intake and status lookup.
3. Ground an optional LLM assistant in approved runbooks, require source links, and log operator feedback.
4. Add service ownership, data freshness indicators, and role-based access before exposing real customer data.

All values shown are synthetic. The dashboard is a prototype, not an Apple system or operational source of truth.
