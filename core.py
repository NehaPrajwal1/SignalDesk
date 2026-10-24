"""Transactional storage, telemetry normalization, and evidence-based support workflow."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
import uuid

SERVICES = ("Spark", "Flink", "Airflow", "Trino", "Druid", "Notebook Hub")
STATUSES = ("Investigating", "Monitoring", "Awaiting owner", "Mitigated")
RUNBOOKS = {
    "Spark": {"id": "RB-SPARK", "owner": "Compute", "checks": ["Inspect failed stages and executor loss.", "Compare shuffle spill and memory pressure with the last healthy run.", "Escalate to the job owner before changing resources."]},
    "Flink": {"id": "RB-FLINK", "owner": "Streaming", "checks": ["Compare source lag and downstream backpressure.", "Inspect checkpoint failures and duration.", "Ask the owner before restarting a stateful job."]},
    "Airflow": {"id": "RB-AIRFLOW", "owner": "Orchestration", "checks": ["Inspect scheduler heartbeat and queued task age.", "Check worker capacity and DAG changes.", "Confirm task idempotency before retrying."]},
    "Trino": {"id": "RB-TRINO", "owner": "Query", "checks": ["Inspect coordinator queues and worker availability.", "Compare query plans and connector errors.", "Confirm affected query owners before cancellation."]},
    "Druid": {"id": "RB-DRUID", "owner": "Analytics", "checks": ["Inspect ingestion failures and segment availability.", "Compare broker latency against historical samples.", "Confirm data completeness with the ingestion owner."]},
    "Notebook Hub": {"id": "RB-NOTEBOOK", "owner": "Developer Experience", "checks": ["Inspect pod scheduling and image pull failures.", "Check resource quotas and storage mounts.", "Confirm user work is saved before restarting."]},
}


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class Conflict(ValueError):
    pass


class NotFound(ValueError):
    pass


class Store:
    def __init__(self, path):
        self.postgres = str(path).startswith(("postgresql://", "postgres://"))
        self.path = str(path) if self.postgres else Path(path)

    @contextmanager
    def transaction(self, write=False):
        if self.postgres:
            from database import PostgresConnection
            conn = PostgresConnection(self.path)
        else:
            conn = sqlite3.connect(self.path, timeout=15)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
        try:
            if write:
                # Preserve atomic read-then-write semantics across API instances.
                # This initial PostgreSQL backend serializes write transactions.
                conn.execute("SELECT pg_advisory_xact_lock(734821905)") if self.postgres else conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize(self):
        if not self.postgres:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction(True) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS incidents(id TEXT PRIMARY KEY, opened_at TEXT, severity TEXT, service TEXT, signal TEXT, status TEXT, impact TEXT, source TEXT)")
            # Additive migration preserves records made by the original demo.
            columns = {row[1] for row in conn.execute("PRAGMA table_info(incidents)")}
            for name, kind in {"version": "INTEGER NOT NULL DEFAULT 1", "updated_at": "TEXT", "resolved_at": "TEXT", "request_key": "TEXT", "request_digest": "TEXT"}.items():
                if name not in columns:
                    conn.execute(f"ALTER TABLE incidents ADD COLUMN {name} {kind}")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS incident_request_key ON incidents(request_key)")
            conn.execute("CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, incident_id TEXT, at TEXT, actor TEXT, old_status TEXT, new_status TEXT, note TEXT)")
            conn.execute("CREATE INDEX IF NOT EXISTS incident_events ON events(incident_id,id)")
            conn.execute("CREATE TABLE IF NOT EXISTS telemetry(event_id TEXT PRIMARY KEY, at TEXT NOT NULL, service TEXT NOT NULL, status_code INTEGER NOT NULL, latency_ms REAL NOT NULL, message TEXT NOT NULL, digest TEXT NOT NULL)")
            conn.execute("CREATE INDEX IF NOT EXISTS telemetry_service_time ON telemetry(service,at)")
            conn.execute("CREATE TABLE IF NOT EXISTS batches(id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT, accepted INTEGER, duplicates INTEGER, rejected INTEGER)")
            conn.execute("CREATE TABLE IF NOT EXISTS rejected_rows(batch_id INTEGER, row_number INTEGER, reason TEXT)")
            conn.execute("CREATE TABLE IF NOT EXISTS slack_receipts(signature TEXT PRIMARY KEY, at REAL NOT NULL, response TEXT NOT NULL)")
            conn.execute("""CREATE TABLE IF NOT EXISTS investigation_jobs(
                incident_id TEXT PRIMARY KEY REFERENCES incidents(id),
                status TEXT NOT NULL CHECK(status IN ('queued','running','completed','failed')),
                use_llm INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at REAL NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_error TEXT
            )""")
            conn.execute("CREATE INDEX IF NOT EXISTS investigation_jobs_ready ON investigation_jobs(status,available_at,created_at)")
            job_columns = {row[1] for row in conn.execute("PRAGMA table_info(investigation_jobs)")}
            for name, kind in {"lease_token": "TEXT", "lease_until": "REAL"}.items():
                if name not in job_columns:
                    conn.execute(f"ALTER TABLE investigation_jobs ADD COLUMN {name} {kind}")

    def incidents(self, service=None, limit=200):
        with self.transaction() as conn:
            rows = conn.execute("SELECT * FROM incidents WHERE (? IS NULL OR service=?) ORDER BY opened_at DESC,id DESC LIMIT ?", (service, service, limit)).fetchall()
        return [dict(row) for row in rows]

    def incident(self, ident, conn=None):
        if conn is None:
            with self.transaction() as db:
                return self.incident(ident, db)
        row = conn.execute("SELECT * FROM incidents WHERE id=?", (ident,)).fetchone()
        if row is None:
            raise NotFound("Incident not found")
        return dict(row)

    def create(self, data, key=None, use_llm=False, actor="alert intake"):
        payload_digest = digest(data)
        with self.transaction(True) as conn:
            if key:
                old = conn.execute("SELECT * FROM incidents WHERE request_key=?", (key,)).fetchone()
                if old:
                    if old["request_digest"] != payload_digest:
                        raise Conflict("Idempotency key reused with a different payload")
                    return dict(old), True
            ident = "INC-" + uuid.uuid4().hex[:12].upper()
            stamp = utcnow()
            conn.execute("INSERT INTO incidents(id,opened_at,severity,service,signal,status,impact,source,updated_at,request_key,request_digest) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (ident, stamp, data["severity"], data["service"], data["signal"], "Investigating", data["impact"], data["source"], stamp, key, payload_digest))
            conn.execute("INSERT INTO events(incident_id,at,actor,old_status,new_status,note) VALUES(?,?,?,?,?,?)", (ident, stamp, actor, None, "Investigating", "Alert accepted"))
            now = time.time()
            conn.execute("INSERT INTO investigation_jobs(incident_id,status,use_llm,attempts,available_at,created_at,updated_at) VALUES(?,'queued',?,0,?,?,?)",
                         (ident, int(use_llm), now, now, now))
            return self.incident(ident, conn), False

    def _recover_expired_jobs(self, conn, now):
        conn.execute("UPDATE investigation_jobs SET status=CASE WHEN attempts>=5 THEN 'failed' ELSE 'queued' END,available_at=?,updated_at=?,lease_token=NULL,lease_until=NULL,last_error='WorkerLeaseExpired' WHERE status='running' AND (lease_until IS NULL OR lease_until<=?)", (now, now, now))

    def claim_investigation_job(self):
        now = time.time()
        with self.transaction(True) as conn:
            self._recover_expired_jobs(conn, now)
            row = conn.execute("SELECT incident_id,use_llm FROM investigation_jobs WHERE status='queued' AND available_at<=? AND attempts<5 ORDER BY created_at,incident_id LIMIT 1", (now,)).fetchone()
            if row is None:
                return None
            lease = uuid.uuid4().hex
            conn.execute("UPDATE investigation_jobs SET status='running',attempts=attempts+1,updated_at=?,last_error=NULL,lease_token=?,lease_until=? WHERE incident_id=?", (now, lease, now + 120, row["incident_id"]))
            return {"incident_id": row["incident_id"], "use_llm": bool(row["use_llm"]), "lease_token": lease}

    def fail_investigation_job(self, ident, error, lease_token):
        with self.transaction(True) as conn:
            row = conn.execute("SELECT attempts FROM investigation_jobs WHERE incident_id=? AND status='running' AND lease_token=? AND lease_until>?", (ident, lease_token, time.time())).fetchone()
            if row is None:
                return False
            attempts = row["attempts"]
            delay = min(300, 2 ** max(0, attempts - 1))
            conn.execute("UPDATE investigation_jobs SET status=?,available_at=?,updated_at=?,last_error=?,lease_token=NULL,lease_until=NULL WHERE incident_id=?", ("failed" if attempts >= 5 else "queued", time.time() + delay, time.time(), type(error).__name__[:100], ident))
            return True

    def recover_investigation_jobs(self):
        now = time.time()
        with self.transaction(True) as conn:
            self._recover_expired_jobs(conn, now)
            conn.execute("""INSERT OR IGNORE INTO investigation_jobs(incident_id,status,use_llm,attempts,available_at,created_at,updated_at)
                            SELECT i.id,'queued',0,0,?,?,? FROM incidents i
                            LEFT JOIN investigations x ON x.incident_id=i.id
                            LEFT JOIN investigation_jobs j ON j.incident_id=i.id
                            WHERE x.incident_id IS NULL AND j.incident_id IS NULL""", (now, now, now))

    def investigation_job(self, ident):
        with self.transaction() as conn:
            row = conn.execute("SELECT status,attempts,last_error,updated_at,available_at,lease_until FROM investigation_jobs WHERE incident_id=?", (ident,)).fetchone()
            return dict(row) if row else None

    def investigation_job_counts(self):
        with self.transaction() as conn:
            return {row["status"]: row["count"] for row in conn.execute("SELECT status,COUNT(*) AS count FROM investigation_jobs GROUP BY status")}

    def retry_investigation_job(self, ident, actor):
        with self.transaction(True) as conn:
            self.incident(ident, conn)
            row = conn.execute("SELECT status FROM investigation_jobs WHERE incident_id=?", (ident,)).fetchone()
            if row is None or row["status"] != "failed":
                raise Conflict("Only failed jobs can be retried")
            now = time.time()
            conn.execute("UPDATE investigation_jobs SET status='queued',attempts=0,available_at=?,updated_at=?,last_error=NULL,lease_token=NULL,lease_until=NULL WHERE incident_id=?", (now, now, ident))
            conn.execute("INSERT INTO events(incident_id,at,actor,note) VALUES(?,?,?,?)", (ident, utcnow(), actor, "Failed investigation requeued"))
        return self.investigation_job(ident)

    def _update(self, conn, ident, status, note, actor, expected_version=None):
        old = self.incident(ident, conn)
        if expected_version is not None and old["version"] != expected_version:
            raise Conflict("Incident changed; refresh before saving")
        stamp = utcnow()
        resolved = (old["resolved_at"] or stamp) if status == "Mitigated" else None
        conn.execute("UPDATE incidents SET status=?,version=version+1,updated_at=?,resolved_at=? WHERE id=?", (status, stamp, resolved, ident))
        conn.execute("INSERT INTO events(incident_id,at,actor,old_status,new_status,note) VALUES(?,?,?,?,?,?)", (ident, stamp, actor, old["status"], status, note))
        return self.incident(ident, conn)

    def update(self, ident, status, note, actor, expected_version=None):
        with self.transaction(True) as conn:
            return self._update(conn, ident, status, note, actor, expected_version)

    def events(self, ident):
        self.incident(ident)
        with self.transaction() as conn:
            return [dict(row) for row in conn.execute("SELECT at,actor,old_status,new_status,note FROM events WHERE incident_id=? ORDER BY id DESC", (ident,))]

    def slack_ack(self, ident, actor, signature, timestamp):
        with self.transaction(True) as conn:
            conn.execute("DELETE FROM slack_receipts WHERE at<?", (timestamp - 600,))
            receipt = conn.execute("SELECT response FROM slack_receipts WHERE signature=?", (signature,)).fetchone()
            if receipt:
                return json.loads(receipt[0])
            self._update(conn, ident, "Investigating", "Acknowledged through a signed Slack request", actor)
            result = {"response_type": "ephemeral", "text": f"Acknowledged {ident}; status is Investigating."}
            conn.execute("INSERT INTO slack_receipts VALUES(?,?,?)", (signature, timestamp, json.dumps(result)))
            return result

    def ingest(self, rows):
        counts = {"accepted": 0, "duplicates": 0, "rejected": 0}
        errors = []
        with self.transaction(True) as conn:
            for index, raw in enumerate(rows, 1):
                try:
                    row = normalize_event(raw)
                    fingerprint = digest(row)
                    old = conn.execute("SELECT digest FROM telemetry WHERE event_id=?", (row["event_id"],)).fetchone()
                    if old:
                        if old[0] != fingerprint:
                            raise ValueError("event_id already exists with different data")
                        counts["duplicates"] += 1
                        continue
                    conn.execute("INSERT INTO telemetry VALUES(?,?,?,?,?,?,?)", (*row.values(), fingerprint))
                    counts["accepted"] += 1
                except (ValueError, TypeError, KeyError):
                    counts["rejected"] += 1
                    errors.append({"row": index, "reason": "Invalid record or conflicting event_id; check the telemetry contract"})
            cursor = conn.execute("INSERT INTO batches(at,accepted,duplicates,rejected) VALUES(?,?,?,?)", (utcnow(), *counts.values()))
            for error in errors:
                conn.execute("INSERT INTO rejected_rows VALUES(?,?,?)", (cursor.lastrowid, error["row"], error["reason"]))
        return {"batch_id": cursor.lastrowid, **counts, "errors": errors}

    def overview(self, service=None):
        with self.transaction() as conn:
            aggregate = conn.execute("SELECT service,COUNT(*) requests,SUM(CASE WHEN status_code>=500 THEN 1 ELSE 0 END) failures,MAX(at) last_sample FROM telemetry WHERE (? IS NULL OR service=?) GROUP BY service", (service, service)).fetchall()
            result = []
            for row in aggregate:
                latencies = [item[0] for item in conn.execute("SELECT latency_ms FROM telemetry WHERE service=? ORDER BY latency_ms", (row["service"],))]
                availability = 100 * (row["requests"] - row["failures"]) / row["requests"]
                result.append({**dict(row), "availability_pct": round(availability, 3), "error_rate_pct": round(100 - availability, 3), "p95_latency_ms": latencies[math.ceil(.95 * len(latencies)) - 1], "slo_target_pct": 99.0, "budget_burn": round((100 - availability) / 1.0, 3), "owner": RUNBOOKS[row["service"]]["owner"]})
        return result

    def traffic(self, service=None):
        with self.transaction() as conn:
            rows = conn.execute("SELECT substr(at,1,13)||':00:00+00:00' time,COUNT(*) requests,SUM(CASE WHEN status_code>=500 THEN 1 ELSE 0 END) failures FROM telemetry WHERE (? IS NULL OR service=?) GROUP BY substr(at,1,13) ORDER BY time", (service, service)).fetchall()
        return [{**dict(row), "requests_k": row["requests"] / 1000, "error_rate_pct": round(100 * row["failures"] / row["requests"], 3)} for row in rows]

    def logs(self, service):
        with self.transaction() as conn:
            return [dict(row) for row in conn.execute("SELECT event_id,at,status_code,latency_ms,message FROM telemetry WHERE service=? AND status_code>=500 ORDER BY at DESC LIMIT 10", (service,))]

    def quality(self):
        with self.transaction() as conn:
            batches = [dict(row) for row in conn.execute("SELECT * FROM batches ORDER BY id DESC LIMIT 20")]
            records = conn.execute("SELECT COUNT(*) FROM telemetry").fetchone()[0]
        return {"stored_records": records, "recent_batches": batches}

    def support(self):
        with self.transaction() as conn:
            rows = [dict(row) for row in conn.execute("SELECT * FROM incidents")]
            daily = [dict(row) for row in conn.execute("SELECT substr(opened_at,1,10) day,severity,COUNT(*) incidents FROM incidents GROUP BY day,severity ORDER BY day")]
        now = datetime.now(timezone.utc)
        opened = [row for row in rows if row["status"] != "Mitigated"]
        resolved = [row for row in rows if row["resolved_at"]]
        durations = [(datetime.fromisoformat(row["resolved_at"]) - datetime.fromisoformat(row["opened_at"])).total_seconds() / 60 for row in resolved]
        return {"created_daily": daily, "total": len(rows), "open": len(opened), "p1_open": sum(row["severity"] == "P1" for row in opened), "oldest_open_minutes": round(max([(now - datetime.fromisoformat(row["opened_at"])).total_seconds() / 60 for row in opened], default=0), 1), "mean_resolution_minutes": round(sum(durations) / len(durations), 2) if durations else None, "resolved_count": len(durations)}


def normalize_event(raw):
    ident, service, stamp = raw["event_id"], raw["service"], raw["at"]
    if not isinstance(stamp, str) or not isinstance(ident, str) or not ident.strip() or len(ident) > 100 or service not in SERVICES:
        raise ValueError("Invalid identifier or service")
    at = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    if at.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    code, latency = raw["status_code"], raw["latency_ms"]
    if type(code) is not int or not 100 <= code <= 599 or type(latency) not in (int, float) or not math.isfinite(latency) or not 0 <= latency <= 3600000:
        raise ValueError("Invalid metric")
    message = raw.get("message", "")
    if not isinstance(message, str) or len(message) > 500:
        raise ValueError("Invalid log message")
    return {"event_id": ident.strip(), "at": at.astimezone(timezone.utc).isoformat(timespec="seconds"), "service": service, "status_code": code, "latency_ms": float(latency), "message": message}


def demo_rows():
    """Deterministic raw fixtures, deliberately including duplicate and invalid rows."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(360):
        service = SERVICES[index % len(SERVICES)]
        failed = index % 17 == 0
        rows.append({"event_id": f"demo-{index}", "at": (start + timedelta(minutes=index * 4)).isoformat(), "service": service, "status_code": 503 if failed else 200, "latency_ms": 1500 if failed else 30 + index % 200, "message": "Synthetic upstream timeout" if failed else "Synthetic request completed"})
    return rows + [dict(rows[0]), {"event_id": "bad-row", "service": "unknown"}]


def investigate(store, ident):
    """Bounded, read-only tool orchestration; no model can invoke a write tool."""
    incident = store.incident(ident)
    service = incident["service"]
    metrics = store.overview(service)
    logs = store.logs(service)
    runbook = RUNBOOKS[service]
    evidence = {"incident": incident, "telemetry": metrics, "recent_errors": logs, "runbook": runbook}
    citations = [ident, runbook["id"]] + [row["event_id"] for row in logs[:3]]
    status = incident["status"]
    summary = f"{ident}: {service} is {status}. Reported impact: {incident['impact']}. Root cause and recovery time are not confirmed."
    if status == "Mitigated":
        summary = f"{ident}: {service} is marked Mitigated. Reported impact was {incident['impact']}. Confirm recovery with the customer before closure."
    return {"mode": "deterministic read-only workflow", "summary": summary, "runbook": "\n".join(runbook["checks"]), "customer_update": summary, "citations": citations, "evidence": evidence, "tool_trace": ["get_incident", "query_service_sli", "retrieve_error_logs", "retrieve_runbook"], "requires_human_review": True}
