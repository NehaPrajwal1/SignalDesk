"""Behavioral evidence for authentication, source delivery, leases and recovery."""
from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
import time

from fastapi.testclient import TestClient
import pytest

from access import AccessControl, password_hash, verify_password
from api import create_app
from core import Conflict, Store
from manage import evaluate, snapshot
from platform_ops import initialize, save_investigation

PASSWORD = "a long test password"
ALERT = {"service": "Spark", "severity": "P2", "signal": "Memory pressure", "impact": "Test only", "source": "test"}


@pytest.fixture(scope="module")
def users():
    hashed = password_hash(PASSWORD)
    return {role: {"password_hash": hashed, "role": role} for role in ("viewer", "operator", "admin")}


@pytest.fixture
def client(tmp_path, monkeypatch, users):
    monkeypatch.setenv("SIGNALDESK_USERS_JSON", json.dumps(users))
    monkeypatch.setenv("SIGNALDESK_ALERTMANAGER_TOKEN", "test-source-only")
    with TestClient(create_app(tmp_path / "auth.db", token="legacy-service", demo=False)) as client:
        yield client


def sign_in(client, name):
    response = client.post("/api/auth/login", json={"username": name, "password": PASSWORD})
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    return {"Authorization": "Bearer " + response.json()["access_token"]}


@pytest.mark.parametrize("role", ["viewer", "operator", "admin"])
def test_login_roles_and_logout(client, role):
    headers = sign_in(client, role)
    assert client.get("/api/auth/me", headers=headers).json() == {"username": role, "role": role}
    assert client.get("/api/incidents", headers=headers).status_code == 200
    write = client.post("/api/alerts", headers=headers, json=ALERT)
    assert write.status_code == (403 if role == "viewer" else 201)
    assert client.get("/api/admin/queue", headers=headers).status_code == (200 if role == "admin" else 403)
    if role != "viewer":
        events = client.get("/api/incidents/" + write.json()["id"] + "/events", headers=headers).json()
        assert events[0]["actor"] == role
    assert client.post("/api/auth/logout", headers=headers).status_code == 200
    assert client.get("/api/incidents", headers=headers).status_code == 401


def test_expiration_relogin_and_configuration_change(client, monkeypatch, users):
    first = sign_in(client, "operator")
    second = sign_in(client, "operator")
    assert client.get("/api/auth/me", headers=first).status_code == 401
    with client.app.state.store.transaction(True) as conn:
        conn.execute("UPDATE auth_sessions SET expires_at=0")
    assert client.get("/api/auth/me", headers=second).status_code == 401
    third = sign_in(client, "operator")
    changed = {**users, "operator": {**users["operator"], "role": "viewer"}}
    monkeypatch.setenv("SIGNALDESK_USERS_JSON", json.dumps(changed))
    access = AccessControl(client.app.state.store, "legacy-service")
    from starlette.requests import Request
    request = Request({"type": "http", "method": "GET", "path": "/api/incidents", "headers": [(b"authorization", third["Authorization"].encode())]})
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as error:
        access.authorize(request)
    assert error.value.status_code == 401


def test_login_throttle_and_invalid_credentials(client):
    for _ in range(10):
        assert client.post("/api/auth/login", json={"username": "unknown", "password": PASSWORD}).status_code == 401
    assert client.post("/api/auth/login", json={"username": "viewer", "password": PASSWORD}).status_code == 429
    assert client.get("/api/incidents").status_code == 401
    assert client.get("/api/admin/queue", headers={"Authorization": "Bearer legacy-service"}).status_code == 403


def test_hash_validation_and_required_auth(tmp_path, monkeypatch):
    hashed = password_hash(PASSWORD)
    assert verify_password(PASSWORD, hashed)
    assert not verify_password("wrong", hashed)
    assert not verify_password(PASSWORD, "malformed")
    monkeypatch.setenv("SIGNALDESK_USERS_JSON", "{}")
    monkeypatch.setenv("SIGNALDESK_REQUIRE_AUTH", "1")
    with pytest.raises(ValueError):
        create_app(tmp_path / "required.db", token="", demo=False)
    monkeypatch.setenv("SIGNALDESK_USERS_JSON", '{"bad":{"role":"admin","password_hash":"plain"}}')
    with pytest.raises(ValueError):
        create_app(tmp_path / "bad.db", token="", demo=False)


def test_request_body_limit_and_source_auth(client, monkeypatch):
    headers = {"Authorization": "Bearer test-source-only"}
    assert client.post("/api/integrations/alertmanager", json={}, headers={}).status_code == 401
    assert client.get("/api/incidents", headers=headers).status_code == 401
    assert client.post("/api/telemetry", content=(b"x" * 100000 for _ in range(21))).status_code == 413
    monkeypatch.delenv("SIGNALDESK_ALERTMANAGER_TOKEN")
    assert client.post("/api/integrations/alertmanager", json={}).status_code == 503


def notification(status="firing", start="2026-10-24T00:00:00Z"):
    return {"version": "4", "receiver": "signaldesk", "alerts": [{"status": status, "fingerprint": "0123456789abcdef", "startsAt": start, "labels": {"service": "Spark", "alertname": "Executor failures", "severity": "critical"}, "annotations": {"summary": "untrusted source text"}, "generatorURL": "http://never-fetch.invalid"}]}


def test_alertmanager_replay_resolution_and_new_cycle(client):
    headers = {"Authorization": "Bearer test-source-only"}
    path = "/api/integrations/alertmanager"
    first = client.post(path, headers=headers, json=notification()).json()["alerts"][0]
    again = client.post(path, headers=headers, json=notification()).json()["alerts"][0]
    assert first["incident_id"] == again["incident_id"] and again["duplicate"]
    client.post(path, headers=headers, json=notification("resolved"))
    store = client.app.state.store
    assert store.incident(first["incident_id"])["status"] == "Monitoring"
    before = len(store.events(first["incident_id"]))
    client.post(path, headers=headers, json=notification("resolved"))
    assert len(store.events(first["incident_id"])) == before
    client.post(path, headers=headers, json=notification(start="2026-10-24T01:00:00Z"))
    assert len(store.incidents()) == 2
    ignored = client.post(path, headers=headers, json=notification("resolved", "2026-10-24T02:00:00Z")).json()["alerts"][0]
    assert ignored["status"] == "ignored" and len(store.incidents()) == 2
    bad = notification()
    bad["alerts"][0]["labels"]["service"] = "unknown"
    assert client.post(path, headers=headers, json=bad).status_code == 422
    bad = {**notification(), "truncatedAlerts": 1}
    assert client.post(path, headers=headers, json=bad).status_code == 422


@pytest.fixture
def queue(tmp_path):
    store = Store(tmp_path / "queue.db")
    store.initialize()
    initialize(store)
    item, _ = store.create(ALERT, "one-job")
    return store, item["id"]


def test_atomic_enqueue_and_exclusive_claim(queue):
    store, ident = queue
    replay, duplicate = store.create(ALERT, "one-job")
    assert duplicate and replay["id"] == ident
    with ThreadPoolExecutor(max_workers=4) as workers:
        claimed = list(workers.map(lambda _: store.claim_investigation_job(), range(4)))
    assert sum(job is not None for job in claimed) == 1
    assert store.investigation_job(ident)["attempts"] == 1
    with store.transaction(True) as conn:
        conn.execute("CREATE TRIGGER reject_job BEFORE INSERT ON investigation_jobs BEGIN SELECT RAISE(ABORT, 'test failure'); END")
    with pytest.raises(sqlite3.IntegrityError):
        store.create(ALERT, "must-rollback")
    assert len(store.incidents()) == 1


def test_recovery_fences_old_worker_and_atomic_completion(queue):
    store, ident = queue
    old = store.claim_investigation_job()
    initialize(store)
    assert store.investigation_job(ident)["status"] == "running"
    assert store.claim_investigation_job() is None
    with store.transaction(True) as conn:
        conn.execute("UPDATE investigation_jobs SET lease_until=0")
    restarted = Store(store.path)
    restarted.initialize()
    initialize(restarted)
    new = restarted.claim_investigation_job()
    assert new["lease_token"] != old["lease_token"]
    with pytest.raises(Conflict):
        save_investigation(store, ident, lease_token=old["lease_token"])
    assert not store.fail_investigation_job(ident, RuntimeError(), old["lease_token"])
    save_investigation(restarted, ident, lease_token=new["lease_token"])
    assert restarted.investigation_job(ident)["status"] == "completed"
    with store.transaction() as conn:
        assert conn.execute("SELECT COUNT(*) FROM investigations").fetchone()[0] == 1


def test_backoff_exhaustion_and_expired_fifth_attempt(queue):
    store, ident = queue
    for attempt in range(1, 6):
        job = store.claim_investigation_job()
        assert job
        before = time.time()
        assert store.fail_investigation_job(ident, RuntimeError("do not store secrets"), job["lease_token"])
        state = store.investigation_job(ident)
        assert state["attempts"] == attempt and state["last_error"] == "RuntimeError"
        assert state["available_at"] >= before + 2 ** (attempt - 1)
        with store.transaction(True) as conn:
            conn.execute("UPDATE investigation_jobs SET available_at=0")
    assert store.investigation_job(ident)["status"] == "failed"
    assert store.claim_investigation_job() is None
    store.retry_investigation_job(ident, "admin")
    assert store.events(ident)[0]["actor"] == "admin"
    with store.transaction(True) as conn:
        conn.execute("UPDATE investigation_jobs SET status='running',attempts=5,lease_until=0")
    store.recover_investigation_jobs()
    assert store.investigation_job(ident)["status"] == "failed"


def test_verified_backup_restore_and_evaluation(queue, tmp_path):
    store, ident = queue
    access = AccessControl(store, "test")
    access.initialize()
    with store.transaction(True) as conn:
        conn.execute("INSERT INTO auth_sessions VALUES('hash','viewer',9999999999,'config')")
    backup = tmp_path / "snapshot.db"
    assert snapshot(store.path, backup)["integrity"] == "ok"
    with pytest.raises(FileExistsError):
        snapshot(store.path, backup)
    restored = tmp_path / "restored.db"
    assert snapshot(backup, restored, restore=True)["sessions_revoked"]
    assert Store(restored).incident(ident)["id"] == ident
    with sqlite3.connect(restored) as conn:
        assert conn.execute("SELECT COUNT(*) FROM auth_sessions").fetchone()[0] == 0
    result = evaluate()
    assert result["mode"] == "deterministic_baseline" and len(result["cases"]) == 6
    assert result["requires_human_review"]
