# Operational integrations and deployment guide

This is a reproducible single-node portfolio environment. It demonstrates the four requested pillars; production deployment needs the controls and operational evidence listed below.

See [OPERATIONS.md](OPERATIONS.md) for named-user login and roles, the authenticated Alertmanager receiver, worker leases, verified backups, production host configuration and model evaluation.

## 1. Live workload telemetry

POST /api/workloads accepts a WorkloadEvent schema (see /docs): event_id, timezone-aware at, service, kind, pod, CPU and memory percentages, latency_ms, restart count, failed, and log. Kinds include dag_failure, job_memory, job_latency, query_error and container_health. Unknown fields, invalid services, unsafe pod names and out-of-range metrics are rejected. SQLite indexes service/time; identical replays are accepted without duplication, conflicting payloads return 409. Recognizable token/password patterns are redacted before storage. Redaction is best-effort: send synthetic or approved non-sensitive data only.

A failed event, three or more restarts, memory >=90%, or CPU >=95% creates an idempotent incident. This is an illustrative threshold policy, not a tuned production alert policy. Replays reconcile a missing incident after an interrupted request. Creating an incident and its investigation job is one SQLite transaction. API workers claim queued jobs under unique 120-second leases, retry failures up to five attempts with exponential backoff, and recover expired work without resetting active workers. GET /api/investigations/{id} reports queued, running, completed or failed state plus attempt count. POST /api/agent/{id} remains an explicit synchronous retry. Result writes are fenced by the current lease and complete atomically. SQLite still requires one host and local disk.

Run python simulate.py --count 60 --interval 1, or docker compose --profile demo up --build. The generator sends operational samples plus request events. The dashboard's workload section refreshes every five seconds; use Refresh for the rest of the dashboard. Synthetic timestamps use current UTC; historical request fixtures remain clearly labeled. A service filter makes the resource chart easier to interpret.

## 2. MCP and bounded agents

mcp_server.py uses the official MCP Python SDK. It exposes exactly three tools:

| Tool | Input | Source |
| --- | --- | --- |
| fetch_pod_logs | service enum | Last ten ingested pod log samples; no direct Kubernetes access |
| search_runbooks | service enum | Curated, service-specific checks |
| check_service_health | service enum | Stored request SLIs plus recent operational samples |

For a process-owned stdio connection, run python mcp_server.py with SIGNALDESK_API_URL and SIGNALDESK_API_TOKEN. For HTTP, run uvicorn mcp_server:app --host 127.0.0.1 --port 8001 and connect to /mcp with Authorization: Bearer followed by SIGNALDESK_MCP_TOKEN. HTTP denies access if the MCP token is unset. The API token is separate. The HTTP server allows only localhost/127.0.0.1 and the Compose service host; adding a different approved hostname requires explicitly configuring transport security. This is a single-operator bearer integration, not a multi-tenant OAuth service.

The LLM loop uses Ollama /api/chat tool calling. The same dispatcher supplies the agent and the MCP-facing HTTP routes; the internal agent does not unnecessarily round-trip through MCP. The model dynamically chooses the three tools. The dispatcher enforces the incident's service scope and rejects extra arguments or unknown tools. Four model turns, six calls, per-request timeouts and a 45-second loop deadline bound ordinary execution. Requests timeouts are not a hard process-kill deadline. Model strings never execute code or modify infrastructure. Tool results are evidence, not instructions. Final text is an unverified draft requiring review; this does not guarantee freedom from hallucination or prompt injection.

Default investigations are deterministic and need no model. Set SIGNALDESK_AUTO_LLM=1 for model-assisted alert investigations, or request use_llm=true through the API/UI. Fallback summaries remain available when the model fails.

## 3. Slack deployment

Two alternative transports are implemented. Choose one slash-command installation:

- HTTPS: the existing /slack/commands route verifies Slack's signature and timestamp and supports status, triage INC-ID, ack INC-ID. Responses are ephemeral; set SLACK_SIGNING_SECRET and SLACK_TEAM_ID.
- Socket Mode: slack_bot.py uses Slack Bolt/SDK and supports /signaldesk alert Spark (also the other service names). Enable Socket Mode and Interactivity in your own Slack app. Give its bot commands and chat:write scopes and its app token connections:write. Invite the bot to a dedicated demo channel. Set SLACK_BOT_TOKEN, SLACK_APP_TOKEN, SLACK_TEAM_ID, SLACK_ALLOWED_CHANNELS (comma-separated channel IDs), SIGNALDESK_API_TOKEN and SIGNALDESK_API_URL. Then run python slack_bot.py or docker compose --profile slack up --build.

Socket listeners acknowledge before API/model work, restrict workspace/channel, create incidents with stable idempotency keys and post an investigation button. Draft evidence summaries are posted in the parent message's thread. Clicking Investigate performs a new read-only investigation and adds a threaded draft. Text is rendered as plain_text to avoid interpreting telemetry as Slack mentions/markup. Set SIGNALDESK_SLACK_LLM=1 only if you want model drafts posted to the approved channel. They are labeled for human review. There is no remediation button.

Incident creation is retry-safe; Slack delivery is not a durable exactly-once outbox. A crash after incident creation but before message delivery may require opening the incident in the dashboard. Real workspace installation, permissions and message delivery still require a live acceptance check; mocked SDK tests alone do not prove those.

## 4. Compose and CI

compose.yml is Docker Compose's conventional auto-discovered filename. It starts API, dashboard and MCP using one non-root application image. Host ports bind only to 127.0.0.1. SQLite persists in runtime-data. Set SIGNALDESK_API_TOKEN and a distinct SIGNALDESK_MCP_TOKEN to non-empty random values in runtime environment, then run docker compose up --build. Do not commit tokens.

Optional profiles: demo (finite simulator), slack (credentialed bot), llm (Ollama). The model is deliberately not downloaded by default. For a machine with enough space: docker compose --profile llm up --build -d, then docker compose exec llm ollama pull qwen2.5:3b. Model weights can consume several GB; this is not required for CI or the deterministic demo. Compose routes model calls to http://llm:11434. For non-Docker execution the default is http://127.0.0.1:11434.

.github/workflows/verify.yml is the single CI workflow. Every push/PR runs Python compilation, Ruff correctness checks, pytest on 3.11/3.12, a combined API/core/platform_ops coverage gate of 80%, real MCP stdio handshakes/tool calls, dashboard interaction tests and Chromium rendering. The container job checks API/dashboard readiness, metrics, MCP denial without credentials and an authenticated MCP initialization. Reports and screenshots are retained as Actions artifacts for 30 days. This is continuous integration and deployment smoke verification, not an automatic public production release.

## Acceptance checklist and remaining work

| Area | Automated evidence | Environment-dependent acceptance |
| --- | --- | --- |
| Ingestion | Validation, duplicate/conflict cases, redaction, automatic incident/result | Connect an approved real source and verify source-specific semantics |
| MCP | Real SDK handshake, tool listing/calls, invalid-tool rejection, HTTP auth | Configure the intended MCP client and assess operator permissions |
| Agent | Dynamic calls using a mock model, service scope, loop budget, failure fallback | Evaluate a real model on labeled incidents; measure citation correctness and unsafe suggestions |
| Slack | Mock SDK command/button, acknowledgement order, channel denial, thread placement, retries; signed HTTP tests | Install in a sandbox workspace and verify actual delivery, retry behavior and permissions |
| Deployment | Real image build/start and service handshakes | TLS gateway, identity/RBAC, request limits, secrets management, dependency/image scanning and rollback |
| Data | SQLite transactions, persistence and additive migration | Backup/restore drills, retention, capacity/load tests, migrations and PostgreSQL for multi-instance operation |

Investigation jobs now persist transactionally with incidents, retry transient failures up to five attempts, recover interrupted work on startup and expose status and failure type through the API. Workers now use expiring lease tokens and reject stale results. Multi-host operation still requires PostgreSQL or another suitable shared store and capacity evidence. Slack message delivery still has no durable outbox or exactly-once guarantee. SQLite aggregates scan stored data and p95 uses memory: do not imply high-volume performance. Full credential/PII detection, tenant isolation, Kubernetes collectors, Airflow/Spark/Trino integrations, SLO paging policies, model quality and real customer feedback remain unverified.

## Reference APIs

- [MCP Python SDK v1.26.0](https://github.com/modelcontextprotocol/python-sdk/tree/v1.26.0)
- [Ollama tool calling](https://docs.ollama.com/capabilities/tool-calling)
- [Slack Socket Mode](https://docs.slack.dev/tools/bolt-python/concepts/socket-mode/)
- [Slack command acknowledgement](https://docs.slack.dev/tools/bolt-python/concepts/commands/)
