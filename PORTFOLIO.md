# Portfolio evidence and interview walkthrough

SignalDesk targets the customer-support side of a data-platform SRE internship. It demonstrates software behaviors in a small, reproducible environment. It does not claim production experience, Apple infrastructure access, or real customer adoption.

## Role coverage

| Role capability | Concrete implementation | Evidence and limits |
| --- | --- | --- |
| Customer-facing dashboards | Request-weighted success rate, error trend, p95 latency, owners, incident arrivals, backlog age and time to mitigation | Dashboard tests execute the real script against a running HTTP API; Chromium smoke run captures the page. Metrics use a disclosed synthetic sample window. |
| Raw operational signals | JSONL request/log events -> validation and UTC normalization -> SQLite -> SQL aggregations | Golden input/output assertions, malformed rows, duplicate and conflict handling, batch lineage. This is a bounded batch pipeline, not distributed streaming. |
| APIs and webhooks | FastAPI, OpenAPI docs, optional bearer auth, idempotency keys, version checks | Tests cover invalid requests, unauthorized reads, identical retries, conflicting payloads, concurrent writers and stale updates. |
| Support agents | Four read-only tools assemble incident, SLI, logs and runbook evidence; optional Ollama summarization | Tests cover all six service runbooks, visible citations, model failure fallback and no model-triggered writes. Live model quality is not claimed from mocked tests. |
| Slack integration | Signed slash commands for status, triage and acknowledgement | Protocol tests verify HMAC, timestamp bounds, workspace restriction and retry-safe acknowledgement. A real Slack workspace installation still requires a signing secret and a separately verified HTTPS endpoint. |
| Observability | Prometheus request counts, latency histogram, ingestion outcomes and open backlog; request IDs in structured logs | Tests inspect actual metric output. No Grafana, Splunk or PagerDuty deployment is claimed. |
| Reliability and maintainability | Transactional writes, audit events, additive migration, non-root container and startup readiness | Regression tests include v1 data preservation, concurrent retries, persistence and outage handling; CI builds and starts the container stack. |
| Documentation and presentation | Architecture, data contract, run commands, failure drills and explicit limitations | This guide supports a live demonstration. Stakeholder feedback and presentation experience require actually doing those activities. |

MCP experience can be demonstrated separately through VaultSearch. SignalDesk adds operational support evidence rather than duplicating a retrieval project.

## Five-minute demo

1. Open a green run under Actions -> SignalDesk verification. Show its commit SHA, test report, environment manifest and dashboard image. An old green run does not verify a later commit.
2. Start the stack using the README. Explain that 360 unique request events are synthetic fixtures dated January 1, 2026. One duplicate and one invalid input deliberately exercise data quality.
3. Select Spark. Show the request denominator, error rate, nearest-rank p95, owner and latest-sample timestamp. Explain why the UI displays sample age and why this is not a 30-day availability report.
4. Create an incident with a customer impact. Open its evidence and tool trace. Distinguish observed errors from a confirmed root cause. Update its status and show the audit history.
5. Download the fixture, upload it again, and show that accepted=0, duplicates=361, rejected=1 for the seeded database. This proves that a replay does not double the telemetry totals.
6. Show one failure scenario from the tests: a stale version receives 409, a modified Slack body receives 401, or an unavailable model falls back to an evidence summary.

## Design decisions you should be able to explain

- **SQLite:** keeps a small single-node demo easy to run. BEGIN IMMEDIATE serializes competing writers. Transactions commit an incident change and its event together. PostgreSQL, migrations and operational backups would be appropriate next steps for multi-instance service deployment.
- **Idempotency:** an alert's caller-supplied key is tied to a canonical payload hash. Repeating that payload returns the same incident; changing it under the same key returns 409. Telemetry uses event IDs and canonical row hashes. Unkeyed alerts intentionally create new incidents.
- **Optimistic concurrency:** updates include the version the UI last read. If another update has occurred, the API asks the user to refresh instead of silently overwriting it.
- **SLI math:** success = non-5xx requests / all requests. Overall success uses summed numerators and denominators, not an average of per-service percentages. p95 uses nearest rank over stored latencies. The illustrative 99% budget ratio is sample error rate / 1%; it is not a production SLO alert policy.
- **Customer support measures:** backlog is all non-mitigated incidents; oldest age measures the longest currently open item. Time to mitigation is current mitigation timestamp minus creation time, excluding reopened items. It is not a claim of reduced real-world MTTR.
- **Agent boundary:** code invokes a fixed sequence of four read-only tools. The optional LLM writes a draft from their evidence. It cannot choose a write tool, run commands, acknowledge incidents or post messages. Human review is always required; prompting does not guarantee factual accuracy.
- **Slack boundary:** verify the raw body before parsing, check the five-minute window, optionally restrict the workspace, and use the verified signature as the acknowledgement receipt key. Status replies are ephemeral. Retry receipts and status changes commit together.
- **Observability cardinality:** HTTP metrics use route templates instead of incident IDs. Request IDs go into logs and responses, not metric labels. Telemetry content, tokens and signing secrets are not logged by the application.

## Honest resume wording

After checking a passing run for the submitted commit, a defensible starting point is:

> Built SignalDesk, a Python/FastAPI/Streamlit support workbench that normalizes operational telemetry, calculates service and support metrics, and persists incident history; implemented retry-safe alert intake, signed Slack command handling, and evidence-grounded triage with optional local LLM drafts.

> Added automated API, concurrency, data-quality, Slack-protocol and live-API dashboard checks, plus GitHub Actions verification on Python 3.11/3.12, Chromium rendering and Docker startup.

Add test counts or coverage only from the linked run. Do not claim actual Slack deployment, live Ollama evaluation, production traffic, large-scale Spark operations, performance improvements, or customer impact without separate evidence.

## Remaining experience gaps

A repository cannot replace collaboration with SREs, customer interviews, presenting findings, operating a real distributed platform, or explaining the code yourself. Before an interview, rehearse the demo, inspect the tests and failures, and explain the tradeoffs above without relying on generated text. Live Slack installation and local-model evaluation are optional follow-up demonstrations once the required environment is available.
