# SignalDesk

[![SignalDesk verification](https://github.com/NehaPrajwal1/SignalDesk/actions/workflows/verify.yml/badge.svg)](https://github.com/NehaPrajwal1/SignalDesk/actions/workflows/verify.yml)

A data-platform support workbench: turn raw operational events into customer-facing health and support metrics, investigate incidents with cited evidence, and bring safe self-service workflows to Slack.

**Independent portfolio demo.** Default data is synthetic. This repository does not represent Apple infrastructure or production operating experience. See [role coverage and interview walkthrough](PORTFOLIO.md).

## Start with the evidence

Open [GitHub Actions](https://github.com/NehaPrajwal1/SignalDesk/actions/workflows/verify.yml) and inspect the latest run for the commit you want to evaluate. A green run verifies:

- Behavioral API/data tests and real-HTTP dashboard tests on Python 3.11 and 3.12.
- A minimum 80% combined line-coverage gate for the API and core, with an XML coverage report.
- Chromium rendering of both running services and a captured dashboard image.
- A Docker Compose build, API/database readiness, dashboard readiness, and exposed metrics.

Run artifacts include tests.xml, coverage.xml, environment.txt, commit.txt, dashboard.png (Python 3.11), browser-result.json and service logs. Artifacts expire after 30 days and can be regenerated with **Run workflow**. CI success is evidence for that exact code and environment, not a production certification. Live Slack delivery and real Ollama generation are not claimed by protocol/mocked tests.

## Architecture

~~~mermaid
flowchart LR
    E[JSONL request and log events] --> V[Validation and UTC normalization]
    V --> D[(SQLite: events, incidents, audit history)]
    A[Alerts with idempotency keys] --> API[FastAPI]
    API --> D
    D --> Q[SQL aggregates and support metrics]
    Q --> UI[Streamlit customer dashboard]
    S[Signed Slack commands] --> API
    D --> T[Read-only evidence tools]
    T --> R[Summary, runbook citations, tool trace]
    R --> L[Optional local LLM draft for human review]
    API --> P[Prometheus metrics and request-ID logs]
~~~

## Run without Docker

Python 3.11 or 3.12 is the verified target. From a checkout:

~~~powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
# Terminal 1
python -m uvicorn api:app --reload
# Terminal 2: activate the same environment
python -m streamlit run app.py
~~~

Open http://localhost:8501. API docs: http://127.0.0.1:8000/docs.

## Run with Docker

~~~sh
docker compose up --build
~~~

The stack binds host ports to localhost. SQLite lives in a named volume and survives container replacement. Stop with docker compose down. Removing the runtime-data volume deliberately resets demo records.

## Demo data and ingestion contract

The first demo startup loads 360 unique synthetic request events across Spark, Flink, Airflow, Trino, Druid and Notebook Hub. The fixture also contains one duplicate and one invalid record: accepted=360, duplicates=1, rejected=1. Records cover January 1, 2026, so the UI explicitly shows their age. Generated service names illustrate platform support; no actual Spark/Flink cluster is deployed.

Download the JSONL fixture from the dashboard and upload it again to demonstrate replay handling. Identical event IDs and payloads are counted as duplicates; conflicting reuse or invalid records is rejected with a batch/row reference. Valid records in the same batch still commit. Raw rejected content is not retained.

Each JSONL line has this shape:

~~~json
{"event_id":"request-001","at":"2026-01-01T00:00:00Z","service":"Spark","status_code":503,"latency_ms":1500,"message":"Synthetic upstream timeout"}
~~~

The HTTP ingestion form is POST /api/telemetry with a JSON object containing a records array. A batch contains 1–1,000 objects. Timestamps must be timezone-aware, services must be in the supported list, HTTP status codes must be integers 100–599, and latency must be finite and non-negative. The dashboard also enforces a 2 MB upload limit. Use only synthetic or approved non-sensitive input.

The dashboard computes success from non-5xx requests divided by total requests, nearest-rank p95, an illustrative 99% sample target, error trends, incident arrivals, backlog, oldest-open age and mean time to current mitigation. These are explicitly scoped sample measures; missing data is not reported as perfect health.

## API usage

~~~powershell
$headers = @{ "Idempotency-Key" = "example-alert-001" }
# If an API token is configured:
# $headers.Authorization = "Bearer $env:SIGNALDESK_API_TOKEN"
$body = @{ service="Spark"; severity="P2"; signal="Executor errors"; impact="Demo team"; source="webhook-demo" } | ConvertTo-Json
$incident = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/alerts -ContentType application/json -Headers $headers -Body $body
$update = @{ status="Monitoring"; note="Investigating recovery"; expected_version=$incident.version } | ConvertTo-Json
Invoke-RestMethod -Method Patch -Uri ("http://127.0.0.1:8000/api/incidents/" + $incident.id) -ContentType application/json -Headers $headers -Body $update
~~~

Repeat an alert with the same key/payload to receive the same ID (200 replay, 201 new). Reusing the key for a different payload returns 409. Updates require the last-read version; stale writes return 409. Incident changes and audit history commit atomically.

| Endpoint | Purpose |
| --- | --- |
| GET /api/health | Database-backed readiness and data mode |
| GET /api/services?service=Spark | Request-weighted service metrics and owners |
| GET /api/traffic?service=Spark | Hourly request/error aggregates |
| POST /api/telemetry | Validate and ingest an event batch |
| GET /api/quality | Stored count and recent ingestion outcomes |
| GET /api/support | Backlog, mitigation time and daily arrivals |
| GET /api/incidents | Queue; optional service and limit (1–1,000) |
| POST /api/alerts | Create or replay an alert |
| PATCH /api/incidents/{id} | Version-checked status update |
| GET /api/incidents/{id}/events | Audit history |
| GET /api/triage/{id} | Evidence, citations and runbook |
| POST /api/agent/{id} | Same evidence workflow; optional use_llm=true |
| POST /slack/commands | Signed Slack command adapter |
| GET /metrics | Prometheus exposition |

## Configuration

| Variable | Default / purpose |
| --- | --- |
| SIGNALDESK_DB_PATH | signaldesk.db; Docker uses /data/signaldesk.db |
| SIGNALDESK_DEMO | 1 seeds synthetic examples; set 0 for an empty ingestion workspace |
| SIGNALDESK_API_TOKEN | Empty for localhost demo; when set, protects operational API reads and writes |
| SIGNALDESK_API_URL | Dashboard's API base, default http://127.0.0.1:8000 |
| SLACK_SIGNING_SECRET | Required to enable Slack requests |
| SLACK_TEAM_ID | Optional workspace allowlist; set it for a real installation |
| OLLAMA_BASE_URL | API server's model URL, default http://127.0.0.1:11434 |
| OLLAMA_MODEL | qwen2.5:3b; model must be installed separately |

Set environment variables before starting processes. With Docker, configure optional Ollama networking explicitly; container localhost is not the host's Ollama process. Health and metrics remain public on the localhost-bound demo. The shared token is not a multi-user identity system. Do not publicly expose this demo without authentication, request-size/rate limits, TLS, backups and a deployment review.

## Slack and LLM workflow

Configure a Slack app slash command /signaldesk with Request URL https://YOUR_APPROVED_HOST/slack/commands. Commands: status, triage INC-ID, ack INC-ID. The raw-body HMAC signature and five-minute timestamp window are checked before parsing. Retried acknowledgements do not append duplicate audit events. Responses are ephemeral. Credentials belong in runtime configuration, never this repository. A real installation requires your own Slack workspace and secure endpoint.

The support workflow invokes four read-only tools: incident lookup, service SLI query, error-log retrieval and runbook retrieval. The UI shows the tool trace and evidence IDs. Optional Ollama generation receives this evidence, produces an explicitly unverified draft and falls back cleanly when unavailable. Model text never executes commands, writes incident state or posts messages. The deterministic workflow works without a model; the tests do not establish live model quality or immunity to hallucination.

## Observability

The API exposes signaldesk_http_requests_total, signaldesk_http_duration_seconds, signaldesk_ingestion_rows_total and signaldesk_open_incidents. Request IDs are included in responses and structured logs. Configure a Prometheus scrape target for the reachable API address on port 8000; /metrics is the default metrics path. Useful queries:

~~~promql
sum(rate(signaldesk_http_requests_total{status=~"5.."}[5m]))
histogram_quantile(0.95, sum by (le) (rate(signaldesk_http_duration_seconds_bucket[5m])))
signaldesk_open_incidents
sum by (outcome) (signaldesk_ingestion_rows_total)
~~~

Ingestion counters are process-lifetime counters for API submissions; database batch history persists across restarts and also includes the startup fixture. No Grafana or production alerting deployment is implied.

## Verify and extend

~~~sh
python -m pip install -r requirements-dev.txt
python -m pytest -q --cov=api --cov=core --cov-report=term-missing
python -m playwright install chromium
python scripts/capture_demo.py
~~~

Dependency ranges are resolved at installation time; each CI run records the exact installed versions in its environment artifact. The workflow runs static correctness checks, behavior tests, browser rendering and container smoke checks. See [FastAPI lifespan testing](https://fastapi.tiangolo.com/advanced/testing-events/), [Streamlit AppTest](https://docs.streamlit.io/develop/api-reference/app-testing/st.testing.v1.apptest) and [GitHub Actions](https://docs.github.com/en/actions/get-started/quickstart).

The code is organized into core.py (storage, normalization, aggregates and evidence workflow), api.py (HTTP/Slack/model adapters), app.py (dashboard), tests/ (behavioral verification), and scripts/capture_demo.py (real-browser evidence). The additive migration preserves v1 incidents. SQLite and in-memory p95 calculations suit the small demo; multi-instance service operation, streaming processing, user roles and real service instrumentation remain beyond this implementation.
