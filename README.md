# SignalDesk — Data Platform Support Workbench

SignalDesk is a portfolio project for practicing operational work around a data platform: ingest an alert, persist an incident, inspect service signals, update status with an audit trail, and expose metrics for monitoring. It is aligned with the Apple Services Engineering Data Platform Intern role, but is an independent demo and is not an Apple system.

## What it demonstrates

- Streamlit dashboard for service health, request volume, error rate, severity filtering, and incident triage.
- FastAPI service that accepts alerts and stores incidents plus status history in SQLite.
- Rule-based runbook lookup and customer update draft, with an optional local Ollama draft grounded in the selected incident.
- Slack slash-command handling with request signature verification and replay-window checks.
- Prometheus counters and open-incident gauge at /metrics.
- Input validation, optional bearer-token protection for write routes, and separate dashboard/API processes.

## Architecture

    Alert source or demo form --POST /api/alerts--> FastAPI --> SQLite
    Streamlit dashboard <--------- /api/* --------- FastAPI
    Slack /signaldesk command --signed request----> FastAPI
    Prometheus scraper <------------- /metrics----- FastAPI

## Run locally (PowerShell)

Python 3.10+ is recommended.

    python -m venv .venv
    .venv\Scripts\Activate.ps1
    python -m pip install -r requirements.txt

    # Optional: protect alert creation and status updates with a bearer token.
    $env:SIGNALDESK_API_TOKEN = "replace-with-a-long-random-demo-token"

    # Terminal 1: API and docs at http://127.0.0.1:8000/docs
    uvicorn api:app --reload

    # Terminal 2, same activated environment:
    streamlit run app.py

The dashboard defaults to http://127.0.0.1:8000. If the API token is set, enter the same value in the dashboard sidebar. Keep both services on localhost for this demo.

At first API startup, SignalDesk creates signaldesk.db beside api.py and inserts four synthetic incidents. Runtime data is ignored by Git. Remove the database file to reset. Service health and 24-hour traffic are generated examples, not measurements from a live platform.

## Alert API

POST /api/alerts accepts JSON. If the API token is configured, include Authorization: Bearer TOKEN.

    $body = @{ service = "Spark"; severity = "P2"; signal = "Executor saturation"; impact = "3 teams"; source = "webhook-demo" } | ConvertTo-Json
    $headers = @{}
    if ($env:SIGNALDESK_API_TOKEN) { $headers.Authorization = "Bearer $env:SIGNALDESK_API_TOKEN" }
    Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/alerts -ContentType "application/json" -Headers $headers -Body $body

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| GET /api/health | API status and data mode |
| GET /api/services | Synthetic service health rows |
| GET /api/traffic | Synthetic traffic and error samples |
| GET /api/incidents | Incident queue |
| GET /api/incidents/{id}/events | Status history |
| GET /api/triage/{id} | Runbook checks and cautious draft update |
| PATCH /api/incidents/{id} | Update status and append event |
| GET /metrics | Prometheus exposition format |
| GET /docs | OpenAPI / Swagger UI |

## Optional Slack slash command

Create a Slack app slash command named /signaldesk and point its Request URL to https://YOUR_APPROVED_TUNNEL_HOST/slack/commands. Set the app signing secret before starting the API:

    $env:SLACK_SIGNING_SECRET = "<Slack app signing secret>"
    uvicorn api:app --reload

Supported command text: status, triage INC-2401, and ack INC-2401. The handler checks Slack's HMAC signature and rejects timestamps older than five minutes. Slack needs a secure, approved development tunnel; do not expose this demo API or use real customer data. The ack command writes an audit event through the status-update path.

## Optional local Ollama draft

Install and run Ollama locally, pull an available model (for example: ollama pull qwen2.5:3b), then use the dashboard's Ollama section. Only the selected synthetic incident and its runbook text are sent to the local Ollama endpoint. Review generated text before use; rule-based guidance remains available without Ollama.

## Scope and next improvements

This is an interview portfolio prototype, not a production incident platform. It has no live Apple data, production integrations, identity system, high availability, database migrations, alert deduplication, or automated remediation. Useful next steps:

1. Add automated unit and API integration tests for validation, auth, Slack signatures, event history, and persistence.
2. Add alert deduplication/idempotency and transaction handling for concurrent incident creation.
3. Add structured logs, request IDs, latency/error metrics, and a dashboard for metrics.
4. Replace synthetic telemetry with a documented local fixture or clearly scoped public sample dataset.
5. Add a deployment guide, CI checks, and a threat model before any hosted demo.

All default incident and telemetry data is fictional. Never put credentials, customer information, or proprietary operational data in this public repository.
