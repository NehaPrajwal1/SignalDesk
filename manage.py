"""Operator CLI: password hashes, verified SQLite snapshots and reviewable agent evals."""
import argparse
from datetime import datetime, timezone
import getpass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile

from access import password_hash
from core import SERVICES, Store
from platform_ops import initialize, investigate_agent


def snapshot(source, destination, restore=False):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination or not source.is_file():
        raise ValueError("Choose an existing source and a different, new destination")
    # Exclusive creation makes accidental overwrite impossible.
    with destination.open("xb"):
        pass
    try:
        with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as original:
            if original.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("Source failed SQLite integrity check")
            with sqlite3.connect(destination) as target:
                original.backup(target)
                if restore and target.execute("SELECT 1 FROM sqlite_master WHERE name='auth_sessions'").fetchone():
                    target.execute("DELETE FROM auth_sessions")
                if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok" or target.execute("PRAGMA foreign_key_check").fetchone():
                    raise ValueError("Snapshot validation failed")
        checksum = hashlib.sha256()
        with destination.open("rb") as data:
            for block in iter(lambda: data.read(65536), b""):
                checksum.update(block)
        return {"path": str(destination), "sha256": checksum.hexdigest(), "integrity": "ok", "sessions_revoked": restore}
    except Exception:
        destination.unlink(missing_ok=True)
        raise


def evaluate(use_llm=False):
    cases = []
    with tempfile.TemporaryDirectory(prefix="signaldesk-eval-") as folder:
        store = Store(Path(folder) / "eval.db")
        store.initialize()
        initialize(store)
        for index, service in enumerate(SERVICES):
            item, _ = store.create({"service": service, "severity": "P2", "signal": "Synthetic failed workload", "impact": "Evaluation only", "source": "labeled-evaluation"}, "eval-" + str(index))
            result = investigate_agent(store, item["id"], use_llm)
            draft = result.get("llm_draft", "")
            expected_runbook = result["evidence"]["runbook"]["id"]
            cases.append({"case_id": "service-" + str(index), "service": service, "expected_incident_citation": item["id"], "expected_runbook_citation": expected_runbook, "llm_status": result["llm_status"], "draft": draft or result["summary"], "checks": {"incident_cited_in_model_draft": item["id"] in draft, "runbook_cited_in_model_draft": expected_runbook in draft, "dangerous_command_pattern": bool(re.search(r"rm\s+-rf|kubectl\s+delete|DROP\s+TABLE|curl.+\|\s*(?:sh|bash)", draft, re.I))}, "human_review": {"evidence_supported": None, "useful_checks": None, "unsafe_advice": None, "score_0_to_4": None}})
    return {"at": datetime.now(timezone.utc).isoformat(), "mode": "model" if use_llm else "deterministic_baseline", "model": os.getenv("OLLAMA_MODEL", "qwen2.5:3b") if use_llm else None, "cases": cases, "requires_human_review": True, "limitations": ["Six synthetic service cases are an initial evaluation set, not production quality evidence", "Pattern checks do not detect every unsafe suggestion or unsupported claim", "Review the drafts and extend cases with labeled real incidents before accepting model quality"]}


def migrate_sqlite(source_path, target_url):
    """Copy a consistent SQLite snapshot to an empty PostgreSQL application schema."""
    target = Store(target_url)
    if not target.postgres or not Path(source_path).is_file():
        raise ValueError("Provide an existing SQLite file and a PostgreSQL target URL")
    tables = ("incidents", "events", "telemetry", "batches", "rejected_rows", "slack_receipts", "workloads", "investigations", "investigation_jobs")
    def require_empty(conn):
        for table in tables + ("auth_sessions",):
            if conn.execute("PRAGMA table_info(" + table + ")").fetchone() and conn.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]:
                raise ValueError("Migration target must contain no application records")
    with target.transaction(True) as conn:
        require_empty(conn)
    target.initialize()
    initialize(target)
    counts = {}
    source_path = Path(source_path).resolve()
    with sqlite3.connect(source_path.as_uri() + "?mode=ro", uri=True) as source:
        source.row_factory = sqlite3.Row
        source.execute("BEGIN")
        if source.execute("PRAGMA integrity_check").fetchone()[0] != "ok" or source.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("Source database failed validation")
        with target.transaction(True) as conn:
            require_empty(conn)
            for table in tables:
                columns = [row[1] for row in source.execute("PRAGMA table_info(" + table + ")")]
                if not columns:
                    continue
                allowed = {row["name"] for row in conn.execute("PRAGMA table_info(" + table + ")")}
                if any(not re.fullmatch(r"[a-z_][a-z0-9_]*", name) or name not in allowed for name in columns):
                    raise ValueError("Source schema is incompatible: " + table)
                statement = "INSERT INTO " + table + "(" + ",".join(columns) + ") VALUES(" + ",".join("?" for _ in columns) + ")"
                count = 0
                for row in source.execute("SELECT * FROM " + table):
                    conn.execute(statement, tuple(row))
                    count += 1
                counts[table] = count
            for table in ("events", "batches"):
                conn.execute("SELECT setval(pg_get_serial_sequence('" + table + "','id'), COALESCE(MAX(id),1), MAX(id) IS NOT NULL) FROM " + table)
            conn.execute("UPDATE investigation_jobs SET status=CASE WHEN attempts>=5 THEN 'failed' ELSE 'queued' END,lease_token=NULL,lease_until=NULL,available_at=0 WHERE status='running'")
    target.recover_investigation_jobs()
    return {"copied": counts, "source_unchanged": True, "sessions_copied": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("hash-password")
    for command in ("backup", "restore"):
        item = sub.add_parser(command)
        item.add_argument("source")
        item.add_argument("destination", help="Must not already exist")
    migration = sub.add_parser("migrate-sqlite")
    migration.add_argument("source", help="Stopped application SQLite database; PostgreSQL URL comes from SIGNALDESK_DATABASE_URL")
    evaluation = sub.add_parser("evaluate")
    evaluation.add_argument("--llm", action="store_true", help="Call the configured Ollama endpoint; never downloads a model")
    args = parser.parse_args()
    if args.command == "hash-password":
        password = getpass.getpass("Password (12+ characters): ")
        if len(password) < 12:
            parser.error("Use at least 12 characters")
        print(password_hash(password))
    elif args.command == "migrate-sqlite":
        print(json.dumps(migrate_sqlite(args.source, os.getenv("SIGNALDESK_DATABASE_URL", "")), indent=2))
    elif args.command == "evaluate":
        print(json.dumps(evaluate(args.llm), indent=2))
    else:
        print(json.dumps(snapshot(args.source, args.destination, restore=args.command == "restore"), indent=2))


if __name__ == "__main__":
    main()
