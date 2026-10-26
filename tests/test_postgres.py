"""Real PostgreSQL integration checks, enabled by the dedicated CI service."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
import uuid

from fastapi.testclient import TestClient
import psycopg
from psycopg import sql
import pytest

from access import AccessControl, password_hash
from api import create_app
from core import Store, demo_rows
from manage import migrate_sqlite
from platform_ops import initialize, save_investigation

ALERT = {"service": "Spark", "severity": "P2", "signal": "Failed executor", "impact": "Integration test", "source": "test"}


@pytest.fixture
def pg_store():
    dsn = os.getenv("SIGNALDESK_TEST_POSTGRES")
    if not dsn:
        pytest.skip("PostgreSQL service is tested in its dedicated CI job")
    name = "test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    query = parse_qsl(parts.query) + [("options", "-c search_path=" + name)]
    isolated = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote), parts.fragment))
    store = Store(isolated)
    try:
        store.initialize()
        initialize(store)
        yield store
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))


def test_postgres_atomic_replays_aggregates_and_worker_claims(pg_store):
    store = pg_store
    with ThreadPoolExecutor(max_workers=4) as workers:
        results = list(workers.map(lambda _: store.create(ALERT, "same-key"), range(4)))
    assert len({item["id"] for item, _ in results}) == 1
    assert sum(not replayed for _, replayed in results) == 1
    with ThreadPoolExecutor(max_workers=4) as workers:
        claims = list(workers.map(lambda _: store.claim_investigation_job(), range(4)))
    jobs = [job for job in claims if job]
    assert len(jobs) == 1
    job = jobs[0]
    save_investigation(store, job["incident_id"], lease_token=job["lease_token"])
    assert store.investigation_job(job["incident_id"])["status"] == "completed"
    result = store.ingest(demo_rows())
    assert result["accepted"] == 360 and result["duplicates"] == 1 and result["rejected"] == 1
    assert sum(item["requests"] for item in store.overview()) == 360
    assert store.overview("Spark") and store.traffic("Spark") and store.logs("Spark")
    assert store.support()["total"] == 1 and store.quality()["stored_records"] == 360
    restarted = Store(store.path)
    restarted.initialize()
    initialize(restarted)
    assert restarted.incident(job["incident_id"])["id"] == job["incident_id"]
    with pytest.raises(RuntimeError):
        with store.transaction(True) as conn:
            conn.execute("INSERT INTO batches(at,accepted,duplicates,rejected) VALUES(?,?,?,?)", ("test", 0, 0, 0))
            raise RuntimeError("rollback")
    assert len(store.quality()["recent_batches"]) == 1


def test_postgres_login_source_and_audit(pg_store, monkeypatch):
    password = "correct integration password"
    hashed = password_hash(password)
    monkeypatch.setenv("SIGNALDESK_USERS_JSON", json.dumps({"reader": {"password_hash": hashed, "role": "viewer"}, "admin": {"password_hash": hashed, "role": "admin"}}))
    monkeypatch.setenv("SIGNALDESK_ALERTMANAGER_TOKEN", "source-only-test")
    with TestClient(create_app(pg_store.path, token="", demo=False)) as client:
        def login(name):
            response = client.post("/api/auth/login", json={"username": name, "password": password})
            assert response.status_code == 200
            return {"Authorization": "Bearer " + response.json()["access_token"]}
        reader = login("reader")
        assert client.get("/api/incidents", headers=reader).status_code == 200
        assert client.post("/api/alerts", headers=reader, json=ALERT).status_code == 403
        admin = login("admin")
        item = client.post("/api/alerts", headers=admin, json=ALERT).json()
        assert pg_store.events(item["id"])[0]["actor"] == "admin"
        assert client.get("/api/admin/queue", headers=admin).status_code == 200
        payload = {"version": "4", "alerts": [{"status": "firing", "fingerprint": "0123456789abcdef", "startsAt": "2026-10-24T00:00:00Z", "labels": {"service": "Spark", "alertname": "Failed task"}}]}
        headers = {"Authorization": "Bearer source-only-test"}
        for _ in range(2):
            response = client.post("/api/integrations/alertmanager", headers=headers, json=payload)
            assert response.status_code == 200
        ident = response.json()["alerts"][0]["incident_id"]
        payload["alerts"][0]["status"] = "resolved"
        assert client.post("/api/integrations/alertmanager", headers=headers, json=payload).status_code == 200
        assert pg_store.incident(ident)["status"] == "Monitoring"
        assert client.post("/api/auth/logout", headers=reader).status_code == 200
        assert client.get("/api/incidents", headers=reader).status_code == 401


def test_sqlite_migration_is_guarded_and_preserves_data(pg_store, tmp_path):
    source = Store(tmp_path / "source.db")
    source.initialize()
    initialize(source)
    item, _ = source.create(ALERT, "migration-key")
    source.ingest(demo_rows())
    source.claim_investigation_job()
    AccessControl(source, "test").initialize()
    with source.transaction(True) as conn:
        conn.execute("INSERT INTO auth_sessions VALUES('hash','user',9999999999,'config')")
    result = migrate_sqlite(source.path, pg_store.path)
    assert result["source_unchanged"] and not result["sessions_copied"]
    assert pg_store.incident(item["id"])["signal"] == ALERT["signal"]
    assert pg_store.quality()["stored_records"] == 360
    assert pg_store.investigation_job(item["id"])["status"] == "queued"
    assert source.investigation_job(item["id"])["status"] == "running"
    with pytest.raises(ValueError, match="no application records"):
        migrate_sqlite(source.path, pg_store.path)
    # Imported sequence values must not collide on subsequent writes.
    batch = pg_store.ingest([{**demo_rows()[0], "event_id": "after-migration"}])
    assert batch["batch_id"] == 2
    job = pg_store.claim_investigation_job()
    save_investigation(pg_store, job["incident_id"], lease_token=job["lease_token"])
    assert pg_store.investigation_job(item["id"])["status"] == "completed"
