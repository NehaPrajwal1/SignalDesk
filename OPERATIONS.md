# Operating SignalDesk beyond the demo

This branch adds named local users, revocable sessions, role checks, an Alertmanager receiver, expiring worker leases, verified SQLite snapshots, optional PostgreSQL storage with a guarded migration tool, and a model evaluation harness. A live monitoring system and public host have not been provisioned. These instructions run on your chosen remote host; the developer PC does not need a checkout, Docker, dependencies or model weights.

## Accounts and roles

Build the image on the remote host, then run the password tool there:

~~~sh
docker compose build api
docker compose run --rm api python manage.py hash-password
~~~

The prompt accepts a password of at least 12 characters and outputs a salted PBKDF2-SHA256 hash with 600,000 iterations. Store the resulting hash in a secret-managed environment variable, never commit passwords, session tokens or runtime configuration:

~~~text
SIGNALDESK_USERS_JSON={"admin1":{"role":"admin","password_hash":"REPLACE_WITH_GENERATED_HASH"},"reader1":{"role":"viewer","password_hash":"REPLACE_WITH_ANOTHER_GENERATED_HASH"}}
~~~

Each person needs a distinct account and password. Viewers read evidence; operators also create incidents, ingest events, update status and request investigations; admins additionally inspect the queue and retry failed jobs. The dashboard provides sign-in and disables write actions for viewers. The API independently enforces roles. Login returns an opaque bearer session valid for one hour, with only its SHA256 hash stored in SQLite. Signing in again revokes that user's previous session. Sign-out revokes the session. Restart the API after changing account configuration; removed users and changed passwords or roles invalidate existing sessions. Expired sessions require sign-in again.

The legacy SIGNALDESK_API_TOKEN remains a machine credential with operator privileges for MCP and other existing clients. It cannot use admin endpoints. Use separate secrets for the API, MCP and Alertmanager. Local demo mode remains available when no users or API token are configured. SIGNALDESK_REQUIRE_AUTH=1 rejects startup without credentials and protects /metrics. /api/health and /api/auth/config remain public. Login is limited to ten attempts per client address per minute; configure the proxy and Uvicorn to trust forwarded addresses only from your gateway. Accounts are configured at startup: there is no public registration, password-reset email, OIDC/MFA or tenant isolation in this implementation.

## Persistent remote deployment

Use a host with durable disk, Docker Compose, a DNS name and a TLS gateway. Provide SIGNALDESK_USERS_JSON, SIGNALDESK_API_TOKEN, SIGNALDESK_MCP_TOKEN and SIGNALDESK_ALERTMANAGER_TOKEN through your host's secret manager, then:

~~~sh
docker compose -f compose.yml -f compose.production.yml up --build -d
~~~

The override disables synthetic seeding, requires authentication, restarts services and removes API Linux capabilities. It inherits the durable /data volume and localhost port bindings from compose.yml. The dashboard uses individual sign-in. Put the API and dashboard behind TLS; keep MCP private unless an approved MCP transport hostname is configured. Do not publish the existing container ports directly. No paid hosting, DNS, certificate account or automatic deployment is created by this repository.

An example host Caddy configuration, after replacing the two domains with DNS names you control:

~~~caddyfile
desk.example.com {
    reverse_proxy 127.0.0.1:8501
}
api.example.com {
    reverse_proxy 127.0.0.1:8000
}
~~~

Review your gateway's connection/rate limits, trusted proxy addresses, secret handling, patching and logging before public use. The API rejects bodies over 2 MB even when Content-Length is absent. Login throttling is implemented; general traffic rate limits belong at the gateway. Retain the exact Git commit and image digest used for each deployment. CI does not automatically deploy.

## Connect Prometheus Alertmanager

The receiver implements the official [Alertmanager v4 webhook payload](https://prometheus.io/docs/alerting/latest/configuration/#webhook_config). Configure a unique SIGNALDESK_ALERTMANAGER_SOURCE for each Alertmanager cluster and an independent SIGNALDESK_ALERTMANAGER_TOKEN on the API. Mount that same token into Alertmanager as a secret file:

~~~yaml
route:
  receiver: signaldesk
  group_by: ['...']
receivers:
  - name: signaldesk
    webhook_configs:
      - url: https://api.example.com/api/integrations/alertmanager
        send_resolved: true
        max_alerts: 100
        http_config:
          authorization:
            type: Bearer
            credentials_file: /run/secrets/signaldesk_receiver_token
~~~

Grouping by all labels avoids large mixed groups. The receiver accepts at most 100 alerts and rejects truncated notifications instead of silently losing alerts. Each alert must include a supported service label (Spark, Flink, Airflow, Trino, Druid or Notebook Hub), alertname, a timezone-aware startsAt and a 16-character hexadecimal fingerprint. Severity accepts critical/warning/info or P1/P2/P3. Example Prometheus rule labels:

~~~yaml
labels:
  service: Spark
  severity: critical
~~~

A firing alert creates one incident and durable investigation job. Repeated notifications with the same source/fingerprint/start time reuse that incident. A later firing cycle with a new start time creates a new incident. Resolved alerts move matching active incidents to Monitoring and add an audit event; an operator must confirm mitigation. Unknown resolutions are ignored. Annotations and source URLs are not fetched or stored. The source credential authorizes only this receiver, not other operational API routes. It is disabled with HTTP 503 when no receiver token is configured.

Live acceptance: trigger a controlled rule in your actual Prometheus system, confirm one incident after repeated notifications, resolve the rule and confirm Monitoring plus its audit record, then trigger a new cycle. CI exercises webhook requests but does not claim a real Prometheus installation is connected. This supplies real alerts; request-SLI charts still require actual request telemetry through /api/telemetry.

## Worker failures and retries

Claims are serialized by database write transactions and get a unique lease valid for 120 seconds. Starting another API process preserves unexpired leases. Each claim also recovers expired leases, so recovery does not require another restart. A stale worker cannot persist results or failure state after its lease is replaced. Saving the result and completing the job share one transaction. Retries use exponential backoff and stop after five attempts, including expired attempts. Job status exposes attempts, last failure class and timing. Exception messages are not stored as queue errors.

Admins can inspect GET /api/admin/queue and POST /api/admin/investigations/{id}/retry. Only failed jobs can be requeued, and the admin identity is recorded. Re-running an expired job can execute read-only investigation/model calls again; this is not exactly-once execution. SQLite still requires a single host and local disk. Do not place it on NFS or claim multi-host scaling. PostgreSQL is available as an alternative backend; retention/capacity work and load evidence remain future work. Slack message delivery still needs its own durable outbox.

## Verified backup and restore

On the remote host, create a backup directory writable by the application user:

~~~sh
docker compose exec api python -c "from pathlib import Path; Path('/data/backups').mkdir(exist_ok=True)"
docker compose exec api python manage.py backup /data/signaldesk.db /data/backups/snapshot-001.db
~~~

The tool uses SQLite's online backup API, checks integrity and foreign keys, writes only to a new destination and returns a SHA256 checksum. Keep the checksum and an encrypted copy on independent storage according to your retention policy. A copy in the same volume alone does not protect against host loss. Backups contain operational data and session hashes; restrict access.

For recovery, stop services that read or write the database, run manage.py restore BACKUP NEW_DATABASE on the remote host, and set the API environment override SIGNALDESK_DB_PATH to the new database path before restarting. Use a one-off image/container with the same persistent volume when the API is stopped. Restore revokes all saved login sessions. The tool refuses to overwrite a destination. Inspect record counts and a known incident before switching traffic. Rehearse this on an isolated copy and record the recovery time and checksum. CI covers snapshot validation, persisted incidents, overwrite refusal and session invalidation.

For application rollback, redeploy the previously recorded commit/image. These schema changes are additive; preserve the database and verify compatibility before rollback. If a future migration is destructive, use a validated backup and an explicit recovery plan.

## PostgreSQL and migration from SQLite

Set SIGNALDESK_DATABASE_URL in the remote API secret environment to a PostgreSQL URL. It takes precedence over SIGNALDESK_DB_PATH. Use a private network, a dedicated database/schema and a database user that owns the application schema for startup migrations. For connections across networks, configure TLS certificate validation (for example sslmode=verify-full with the appropriate CA). Keep the URL and password out of Git and logs. The production Compose override forwards this variable to the API. No PostgreSQL server is automatically provisioned.

Multiple API instances can share PostgreSQL. Each database write transaction takes a PostgreSQL advisory transaction lock, preserving the application's atomic read-then-write and queue-claim semantics. Worker lease tokens fence result and failure writes across instances. This initial backend deliberately serializes writes: it supplies shared durable storage, not a measured high-throughput guarantee. There is no connection pool yet; capacity/load testing, pool sizing and narrower locking are required before scaling traffic. Keep all instances on compatible schema versions and synchronize their clocks.

To migrate existing data, stop all API writers and readers first, keep a verified SQLite backup, configure an empty PostgreSQL target, and run the migration from a one-off application container on the remote host:

~~~sh
docker compose -f compose.yml -f compose.production.yml stop api dashboard mcp slack simulator
docker compose -f compose.yml -f compose.production.yml run --rm --no-deps -T api python manage.py migrate-sqlite /data/signaldesk.db
~~~

The target URL comes from SIGNALDESK_DATABASE_URL. The tool validates a consistent read-only SQLite snapshot, rejects a target with application records, checks column compatibility and copies rows in one PostgreSQL transaction. It restores sequence values and resets interrupted jobs. It does not copy login sessions; users must sign in again. The SQLite source is unchanged. Compare the reported table counts, a known incident, audit history and telemetry before restarting services against PostgreSQL. Do not run this against a live target or change traffic before validation. Retain the source and backup for rollback.

PostgreSQL backup and point-in-time recovery should use the database provider's verified backup service or PostgreSQL tools such as pg_dump/pg_restore on the remote host. The manage.py backup/restore commands apply to SQLite files only. After a PostgreSQL restore, revoke rows in auth_sessions before accepting traffic and verify queue state, record counts and a known incident. Rehearse provider recovery separately; CI verifies database behavior and migration, not your hosted backup service.

The PostgreSQL CI job uses a real PostgreSQL 17 service to exercise concurrent idempotent creation and claims, aggregates, sessions/roles, source notifications, rollback, migration refusal on populated targets, retained source data and imported sequence values. This is compatibility and correctness evidence, not a capacity benchmark.

## Model evaluation

~~~sh
docker compose exec api python manage.py evaluate
docker compose exec api python manage.py evaluate --llm
~~~

The first command emits a deterministic baseline. The second calls your configured OLLAMA_BASE_URL and OLLAMA_MODEL; it never installs or downloads a model. Prefer a remote model host if the developer PC has limited disk. The JSON includes six synthetic service cases, expected incident/runbook citations, generation status, draft text, heuristic checks and blank human ratings. Review evidence support, useful checks and unsafe suggestions, scoring each case 0-4. Failed generation is visible rather than counted as a successful model evaluation.

Extend this initial set with labeled real incidents, missing evidence, misleading logs and prompt-injection cases. Reviewers must define acceptance thresholds and assess drafts. The regex checks are only review aids and cannot certify safety. CI verifies the baseline harness; real model quality remains unverified until model runs and human reviews exist.

## Remaining acceptance work

- Provision the remote host, TLS gateway, independent backup storage and actual monitoring rules.
- Run the live source and backup/restore acceptance procedures above.
- Complete real model runs and human scoring; add representative customer incidents.
- For multiple API hosts, configure PostgreSQL, rehearse the migration/recovery procedure and measure capacity, retention and worker behavior under load.
- Add organization identity/SSO and tenant isolation if serving multiple organizations.
- Install and verify real Slack delivery and its durable outbox separately.
